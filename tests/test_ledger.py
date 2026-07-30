"""The resume primitives: window selection, the skip decision, the store.

The decision core is tested without touching disk — `StepCheck` is the
whole interface between "what the shell found" and "what may be
skipped", so every interesting case is expressible as data.
"""

import json

import pytest

from src.ledger import (
    LedgerEntry,
    LedgerNotFoundError,
    NoStepMatchedError,
    ResumeOptions,
    RunLedger,
    StepCheck,
    decide_resume,
    fingerprint,
    format_resume_banner,
    latest_ledger_run,
    list_ledger_runs,
    load_ledger,
    load_resume_request,
    select_window,
    source_fingerprint,
)

NAMES = [
    "load",
    "qx_bkt00_year2019",
    "qx_bkt01_year2019",
    "qx_bkt02_year2019",
    "publish",
]


def _entry(step, sha="sha", run_id="R1", produces=None):
    return LedgerEntry(
        run_id=run_id,
        step=step,
        source_sha=sha,
        produces=produces,
        seconds=1.0,
        finished_at="2026-07-30T10:00:00",
    )


def _check(name, sha="sha", produces=None, present=None):
    return StepCheck(
        name=name, source_sha=sha, produces=produces, output_present=present
    )


class TestSelectWindow:
    def test_no_bounds_is_the_whole_plan(self):
        assert select_window(NAMES) == (0, 5)

    def test_from_matches_an_expanded_name(self):
        first, stop = select_window(NAMES, start="qx_bkt01_year2019")
        assert NAMES[first:stop] == [
            "qx_bkt01_year2019",
            "qx_bkt02_year2019",
            "publish",
        ]

    def test_from_takes_the_first_substring_match(self):
        first, stop = select_window(NAMES, start="qx_")
        assert NAMES[first] == "qx_bkt00_year2019"

    def test_until_takes_the_last_match_inclusive(self):
        # "run everything up to and including the qx group".
        first, stop = select_window(NAMES, until="qx_")
        assert NAMES[first:stop] == NAMES[:4]

    def test_both_bounds_cut_a_middle_slice(self):
        first, stop = select_window(NAMES, start="bkt01", until="bkt02")
        assert NAMES[first:stop] == ["qx_bkt01_year2019", "qx_bkt02_year2019"]

    def test_unmatched_from_raises_instead_of_running_everything(self):
        with pytest.raises(NoStepMatchedError) as excinfo:
            select_window(NAMES, start="qx_bkt99")
        assert "matched none of the 5 steps" in str(excinfo.value)

    def test_unmatched_until_raises(self):
        with pytest.raises(NoStepMatchedError):
            select_window(NAMES, until="nope")

    def test_inverted_window_raises(self):
        with pytest.raises(NoStepMatchedError) as excinfo:
            select_window(NAMES, start="publish", until="load")
        assert "empty window" in str(excinfo.value)


class TestDecideResume:
    def test_everything_complete_leaves_nothing_to_run(self):
        checks = [_check(n) for n in NAMES]
        decision = decide_resume(checks, {n: _entry(n) for n in NAMES})
        assert decision.skip == tuple(NAMES)
        assert decision.start_at is None

    def test_stops_at_the_first_step_missing_from_the_ledger(self):
        checks = [_check(n) for n in NAMES]
        entries = {n: _entry(n) for n in NAMES[:2]}
        decision = decide_resume(checks, entries)
        assert decision.skip == ("load", "qx_bkt00_year2019")
        assert decision.start_at == "qx_bkt01_year2019"
        assert "not recorded as completed" in decision.stop_reason

    def test_a_changed_source_is_not_skipped(self):
        checks = [
            _check(n, sha="new" if n == "qx_bkt01_year2019" else "sha") for n in NAMES
        ]
        decision = decide_resume(checks, {n: _entry(n) for n in NAMES})
        assert decision.start_at == "qx_bkt01_year2019"
        assert "SQL changed since run R1" in decision.stop_reason

    def test_a_vanished_declared_output_is_not_skipped(self):
        checks = [
            _check(n, produces=f"/lake/{n}.parquet", present=(n != "qx_bkt01_year2019"))
            for n in NAMES
        ]
        decision = decide_resume(checks, {n: _entry(n) for n in NAMES})
        assert decision.start_at == "qx_bkt01_year2019"
        assert decision.stop_reason.endswith("/lake/qx_bkt01_year2019.parquet")
        # Everything after the vanished output re-runs too, even though the
        # ledger says those steps finished.
        assert "qx_bkt02_year2019" not in decision.skip

    def test_steps_without_a_declared_output_are_skipped_but_counted(self):
        checks = [_check("a", produces="/lake/a", present=True), _check("b")]
        decision = decide_resume(checks, {"a": _entry("a"), "b": _entry("b")})
        assert decision.skip == ("a", "b")
        assert decision.unverified == ("b",)

    def test_an_empty_ledger_skips_nothing(self):
        decision = decide_resume([_check(n) for n in NAMES], {})
        assert decision.skip == ()
        assert decision.start_at == "load"


class TestBanner:
    def test_names_what_was_checked_and_what_was_only_trusted(self):
        checks = [_check("a", produces="/lake/a", present=True), _check("b")]
        decision = decide_resume(checks, {"a": _entry("a"), "b": _entry("b")})
        text = format_resume_banner(decision, total=4, run_id="R1")
        assert "2 of 4 planned steps proven complete" in text
        assert "verified on disk : 1" in text
        assert "NOT verified     : 1" in text
        assert "TRUSTING" in text

    def test_says_so_when_the_plan_is_already_finished(self):
        decision = decide_resume([_check("a")], {"a": _entry("a")})
        assert "Nothing left to run" in format_resume_banner(
            decision, total=1, run_id="R1"
        )


class TestFingerprint:
    def test_params_are_part_of_the_identity(self):
        assert fingerprint("SELECT 1", {"bkt": 1}) != fingerprint(
            "SELECT 1", {"bkt": 2}
        )

    def test_same_input_same_hash(self):
        assert fingerprint("x", {"a": 1}) == fingerprint("x", {"a": 1})

    def test_missing_file_fingerprints_as_empty(self, tmp_path):
        assert source_fingerprint(str(tmp_path / "nope.sql"), {}) == ""
        assert source_fingerprint(None, {}) == ""

    def test_file_contents_change_the_fingerprint(self, tmp_path):
        path = tmp_path / "s.sql"
        path.write_text("SELECT 1", encoding="utf-8")
        before = source_fingerprint(str(path), {})
        path.write_text("SELECT 2", encoding="utf-8")
        assert source_fingerprint(str(path), {}) != before


class TestRunLedger:
    def test_records_round_trip(self, tmp_path):
        ledger = RunLedger(tmp_path, "RUN1")
        ledger.record(step="a", source_sha="s1", produces="/lake/a", seconds=2.5)
        ledger.record(step="b", source_sha="s2", produces=None, seconds=0.5)

        entries = load_ledger(tmp_path, "RUN1")
        assert [e.step for e in entries] == ["a", "b"]
        assert entries[0].produces == "/lake/a"
        assert entries[0].seconds == 2.5
        assert entries[1].produces is None

    def test_each_line_is_flushed_so_a_crash_keeps_it(self, tmp_path):
        ledger = RunLedger(tmp_path, "RUN1")
        ledger.record(step="a", source_sha="s", produces=None, seconds=1.0)
        # Read the raw file without closing anything.
        raw = (tmp_path / "RUN1" / "ledger.jsonl").read_text(encoding="utf-8")
        assert json.loads(raw.strip())["step"] == "a"

    def test_inherit_keeps_the_original_run_id(self, tmp_path):
        RunLedger(tmp_path, "RUN1").record(
            step="a", source_sha="s", produces=None, seconds=1.0
        )
        second = RunLedger(tmp_path, "RUN2")
        second.inherit(load_ledger(tmp_path, "RUN1"))
        second.record(step="b", source_sha="s", produces=None, seconds=1.0)

        entries = {e.step: e for e in load_ledger(tmp_path, "RUN2")}
        assert entries["a"].run_id == "RUN1"
        assert entries["b"].run_id == "RUN2"

    def test_a_later_line_wins_for_the_same_step(self, tmp_path):
        ledger = RunLedger(tmp_path, "RUN1")
        ledger.record(step="a", source_sha="old", produces=None, seconds=1.0)
        ledger.record(step="a", source_sha="new", produces=None, seconds=2.0)
        entries = load_ledger(tmp_path, "RUN1")
        assert len(entries) == 1
        assert entries[0].source_sha == "new"

    def test_missing_ledger_raises_loudly(self, tmp_path):
        with pytest.raises(LedgerNotFoundError) as excinfo:
            load_ledger(tmp_path, "NOPE")
        assert "no ledger at" in str(excinfo.value)

    def test_listing_and_latest(self, tmp_path):
        assert list_ledger_runs(tmp_path) == []
        for run_id in ("20260730T090000", "20260730T100000"):
            RunLedger(tmp_path, run_id).record(
                step="a", source_sha="s", produces=None, seconds=1.0
            )
        assert list_ledger_runs(tmp_path) == ["20260730T090000", "20260730T100000"]
        assert latest_ledger_run(tmp_path) == "20260730T100000"

    def test_latest_on_an_empty_root_raises(self, tmp_path):
        with pytest.raises(LedgerNotFoundError):
            latest_ledger_run(tmp_path)


class TestLoadResumeRequest:
    def test_no_run_id_loads_no_prior_work(self, tmp_path):
        request = load_resume_request(
            ResumeOptions(root=str(tmp_path), start="qx", until="publish")
        )
        assert request.prior == ()
        assert (request.start, request.until) == ("qx", "publish")

    def test_last_resolves_to_the_newest_run(self, tmp_path):
        for run_id in ("20260730T090000", "20260730T100000"):
            RunLedger(tmp_path, run_id).record(
                step=run_id, source_sha="s", produces=None, seconds=1.0
            )
        request = load_resume_request(ResumeOptions(run_id="last", root=str(tmp_path)))
        assert request.source_run_id == "20260730T100000"
        assert [e.step for e in request.prior] == ["20260730T100000"]

    def test_an_unknown_run_id_raises_rather_than_running_everything(self, tmp_path):
        with pytest.raises(LedgerNotFoundError):
            load_resume_request(ResumeOptions(run_id="GHOST", root=str(tmp_path)))
