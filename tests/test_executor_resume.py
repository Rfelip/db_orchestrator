"""End-to-end resume: a `foreach` manifest that fails, then picks up.

The assembly under test is the one that matters on the 345-step tábua
run — expanded step names, a real DuckDB session, a ledger written as
the run goes, and a second run that skips what the first finished.
Nothing here is mocked above the ssh hop.
"""

import sys

import pytest

from src.duckdb_session import write_helper
from src.executor import Executor
from src.ledger import (
    NoStepMatchedError,
    ResumeRequest,
    RunLedger,
    load_ledger,
)
from src.plans import PlanStore
from src.transport import DuckDbSettings

pytest.importorskip("duckdb")


class LocalDuckDbTransport:
    """Runs the session helper on this machine — same contract as
    `DuckDbSshTransport`, minus the ssh hop, which carries none of the
    semantics under test."""

    name = "ssh+duckdb"

    def __init__(self, helper, settings):
        self.helper = helper
        self.settings = settings

    def session_command(self):
        return [sys.executable, str(self.helper), "--serve"]


@pytest.fixture
def transport(tmp_path):
    helper = tmp_path / "helper.py"
    write_helper(helper)
    return LocalDuckDbTransport(
        helper,
        DuckDbSettings(
            memory_limit="1GB",
            threads=2,
            temp_directory=str(tmp_path / "spill"),
            max_temp_directory_size="1GB",
            profile=True,
        ),
    )


def _write_sql(tmp_path, name, body):
    path = tmp_path / name
    path.write_text(body, encoding="utf-8")
    return path


def _manifest(tmp_path, out_dir, *, third_step_sql):
    """Four steps: two singles, then a two-way `foreach`. Every step
    declares `produces:`, so resume can actually check them."""
    good = _write_sql(
        tmp_path,
        "good.sql",
        "COPY (SELECT 1 AS v) TO '{{ out }}/{{ tag }}.parquet' (FORMAT 'parquet')",
    )
    fan = _write_sql(
        tmp_path,
        "fan.sql",
        "COPY (SELECT {{ bkt }} AS bkt) TO '{{ out }}/fan{{ bkt }}.parquet' "
        "(FORMAT 'parquet')",
    )
    manifest = tmp_path / "manifest.yaml"
    manifest.write_text(
        f"""
steps:
  - name: alpha
    type: sql
    file: "{good}"
    params: {{ out: "{out_dir}", tag: alpha }}
    produces: "{out_dir}/alpha.parquet"
  - name: beta
    type: sql
    file: "{third_step_sql}"
    params: {{ out: "{out_dir}", tag: beta }}
    produces: "{out_dir}/beta.parquet"
  - name: fan
    type: sql
    file: "{fan}"
    params: {{ out: "{out_dir}" }}
    produces: "{out_dir}/fan{{{{ bkt }}}}.parquet"
    foreach:
      bkt: [0, 1]
""",
        encoding="utf-8",
    )
    return manifest


BROKEN = "SELECT * FROM a_table_that_is_not_there"
WORKING = "COPY (SELECT 2 AS v) TO '{{ out }}/{{ tag }}.parquet' (FORMAT 'parquet')"


def _run(
    manifest, transport, tmp_path, run_id, *, resume=None, record=True, enable_all=False
):
    """One orchestrator run. Returns (executed step names, ledger root).

    `enable_all=True` on the resumed runs is deliberate: a first run
    auto-disables the hand-written steps it finished, so without it those
    steps would never reach the resume logic and the ledger's verdict
    about them would be untestable. It is also the pairing the README
    recommends — one authority on what is done, not two.
    """
    root = tmp_path / "runs"
    plans = PlanStore(root, run_id=run_id)
    ledger = RunLedger(root, run_id) if record else None
    if ledger is not None and resume is not None:
        ledger.inherit(resume.prior)
    executor = Executor(
        manifest_path=manifest,
        db_config={},
        notifier_config={},
        force=True,
        enable_all=enable_all,
        duckdb_transport=transport,
        plan_store=plans,
        ledger=ledger,
        resume=resume,
    )
    executor.run()
    if not (root / run_id / "index.jsonl").exists():
        return [], root
    return [s.step for s in plans.summary().steps], root


class TestResumeAfterFailure:
    @pytest.fixture
    def broken_first_run(self, tmp_path, transport, monkeypatch):
        """Run 1: `beta` fails, so `alpha` is the only completed step."""
        monkeypatch.chdir(tmp_path)
        out_dir = tmp_path / "out"
        out_dir.mkdir()
        broken = _write_sql(tmp_path, "beta.sql", BROKEN)
        manifest = _manifest(tmp_path, out_dir, third_step_sql=broken)
        with pytest.raises(SystemExit):
            _run(manifest, transport, tmp_path, "R1")
        return manifest, out_dir, broken, tmp_path / "runs"

    def test_the_ledger_holds_exactly_the_completed_steps(self, broken_first_run):
        _, _, _, root = broken_first_run
        assert [e.step for e in load_ledger(root, "R1")] == ["alpha"]

    def test_resume_restarts_at_the_failed_step(
        self, broken_first_run, transport, tmp_path
    ):
        manifest, out_dir, broken, root = broken_first_run
        broken.write_text(WORKING, encoding="utf-8")

        request = ResumeRequest(prior=load_ledger(root, "R1"), source_run_id="R1")
        ran, _ = _run(
            manifest, transport, tmp_path, "R2", resume=request, enable_all=True
        )

        assert ran == ["beta", "fan_bkt0", "fan_bkt1"]
        assert "alpha" not in ran
        assert sorted(p.name for p in out_dir.glob("*.parquet")) == [
            "alpha.parquet",
            "beta.parquet",
            "fan0.parquet",
            "fan1.parquet",
        ]

    def test_a_completed_step_is_not_re_executed(
        self, broken_first_run, transport, tmp_path
    ):
        manifest, out_dir, broken, root = broken_first_run
        broken.write_text(WORKING, encoding="utf-8")
        alpha = out_dir / "alpha.parquet"
        before = alpha.stat().st_mtime_ns

        request = ResumeRequest(prior=load_ledger(root, "R1"), source_run_id="R1")
        _run(manifest, transport, tmp_path, "R2", resume=request, enable_all=True)

        assert alpha.stat().st_mtime_ns == before

    def test_the_resumed_run_inherits_the_first_runs_completions(
        self, broken_first_run, transport, tmp_path
    ):
        # Chaining matters: if R2 also failed, resuming R2 must still know
        # alpha is done, or the second failure costs the whole pipeline.
        manifest, _, broken, root = broken_first_run
        broken.write_text(WORKING, encoding="utf-8")
        request = ResumeRequest(prior=load_ledger(root, "R1"), source_run_id="R1")
        _run(manifest, transport, tmp_path, "R2", resume=request, enable_all=True)

        entries = {e.step: e.run_id for e in load_ledger(root, "R2")}
        assert entries["alpha"] == "R1"
        assert entries["beta"] == "R2"


class TestVanishedOutputs:
    @pytest.fixture
    def complete_run(self, tmp_path, transport, monkeypatch):
        monkeypatch.chdir(tmp_path)
        out_dir = tmp_path / "out"
        out_dir.mkdir()
        working = _write_sql(tmp_path, "beta.sql", WORKING)
        manifest = _manifest(tmp_path, out_dir, third_step_sql=working)
        _run(manifest, transport, tmp_path, "R1")
        return manifest, out_dir, tmp_path / "runs"

    def test_resuming_a_finished_run_does_nothing(
        self, complete_run, transport, tmp_path
    ):
        manifest, _, root = complete_run
        request = ResumeRequest(prior=load_ledger(root, "R1"), source_run_id="R1")
        ran, _ = _run(
            manifest, transport, tmp_path, "R2", resume=request, enable_all=True
        )
        assert ran == []

    def test_a_deleted_output_forces_that_step_and_everything_after_it(
        self, complete_run, transport, tmp_path, capsys
    ):
        manifest, out_dir, root = complete_run
        (out_dir / "beta.parquet").unlink()

        request = ResumeRequest(prior=load_ledger(root, "R1"), source_run_id="R1")
        ran, _ = _run(
            manifest, transport, tmp_path, "R2", resume=request, enable_all=True
        )

        assert ran == ["beta", "fan_bkt0", "fan_bkt1"]
        assert "its declared output is gone" in capsys.readouterr().out

    def test_a_deleted_output_of_a_foreach_expansion_is_caught(
        self, complete_run, transport, tmp_path
    ):
        # `produces:` is rendered per expansion, so fan0 and fan1 are
        # distinguishable — the thing `disable_step` could never do.
        manifest, out_dir, root = complete_run
        (out_dir / "fan1.parquet").unlink()

        request = ResumeRequest(prior=load_ledger(root, "R1"), source_run_id="R1")
        ran, _ = _run(
            manifest, transport, tmp_path, "R2", resume=request, enable_all=True
        )
        assert ran == ["fan_bkt1"]

    def test_changed_sql_is_re_run_even_though_the_ledger_says_done(
        self, complete_run, transport, tmp_path, capsys
    ):
        manifest, _, root = complete_run
        (tmp_path / "beta.sql").write_text(
            "COPY (SELECT 99 AS v) TO '{{ out }}/{{ tag }}.parquet' (FORMAT 'parquet')",
            encoding="utf-8",
        )
        request = ResumeRequest(prior=load_ledger(root, "R1"), source_run_id="R1")
        ran, _ = _run(
            manifest, transport, tmp_path, "R2", resume=request, enable_all=True
        )

        assert ran[0] == "beta"
        assert "its SQL changed since run R1" in capsys.readouterr().out

    def test_the_banner_admits_what_it_could_not_check(
        self, tmp_path, transport, monkeypatch, capsys
    ):
        # Same manifest with `produces:` stripped — nothing is verifiable.
        monkeypatch.chdir(tmp_path)
        out_dir = tmp_path / "out"
        out_dir.mkdir()
        working = _write_sql(tmp_path, "beta.sql", WORKING)
        manifest = _manifest(tmp_path, out_dir, third_step_sql=working)
        manifest.write_text(
            "\n".join(
                line
                for line in manifest.read_text(encoding="utf-8").splitlines()
                if "produces:" not in line
            ),
            encoding="utf-8",
        )
        _run(manifest, transport, tmp_path, "R1")
        for parquet in out_dir.glob("*.parquet"):
            parquet.unlink()

        request = ResumeRequest(
            prior=load_ledger(tmp_path / "runs", "R1"), source_run_id="R1"
        )
        ran, _ = _run(
            manifest, transport, tmp_path, "R2", resume=request, enable_all=True
        )

        out = capsys.readouterr().out
        # Every output is gone and resume skipped everything anyway — which
        # is exactly what it warns about instead of pretending otherwise.
        assert ran == []
        assert "NOT verified     : 4" in out
        assert "TRUSTING" in out


class TestWindowSelector:
    @pytest.fixture
    def manifest(self, tmp_path, transport, monkeypatch):
        monkeypatch.chdir(tmp_path)
        out_dir = tmp_path / "out"
        out_dir.mkdir()
        working = _write_sql(tmp_path, "beta.sql", WORKING)
        return _manifest(tmp_path, out_dir, third_step_sql=working)

    def test_from_starts_at_an_expanded_name(self, manifest, transport, tmp_path):
        ran, _ = _run(
            manifest,
            transport,
            tmp_path,
            "R1",
            resume=ResumeRequest(start="fan_bkt1"),
        )
        assert ran == ["fan_bkt1"]

    def test_until_stops_after_the_last_match(self, manifest, transport, tmp_path):
        ran, _ = _run(
            manifest, transport, tmp_path, "R1", resume=ResumeRequest(until="beta")
        )
        assert ran == ["alpha", "beta"]

    def test_an_unmatched_selector_raises_instead_of_running_everything(
        self, manifest, transport, tmp_path
    ):
        with pytest.raises(NoStepMatchedError):
            _run(
                manifest,
                transport,
                tmp_path,
                "R1",
                resume=ResumeRequest(start="fan_bkt9"),
            )


class TestBackwardCompatibility:
    def test_no_resume_argument_runs_the_whole_plan(
        self, tmp_path, transport, monkeypatch
    ):
        monkeypatch.chdir(tmp_path)
        out_dir = tmp_path / "out"
        out_dir.mkdir()
        working = _write_sql(tmp_path, "beta.sql", WORKING)
        manifest = _manifest(tmp_path, out_dir, third_step_sql=working)
        ran, _ = _run(manifest, transport, tmp_path, "R1", record=False)
        assert ran == ["alpha", "beta", "fan_bkt0", "fan_bkt1"]
        assert not (tmp_path / "runs" / "R1" / "ledger.jsonl").exists()
