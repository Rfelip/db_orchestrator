"""Tests for plan capture and the summary that makes it usable."""

import json

import pytest

from src.plans import (
    OperatorCost,
    PlanStore,
    StepPlan,
    format_summary,
    list_runs,
    load_run,
    merge_operators,
    new_run_id,
    parse_profile,
    profile_rows,
    profile_seconds,
    summarize,
)

PROFILE = {
    "query_name": "SELECT 1",
    "latency": 2.5,
    "rows_returned": 17,
    "children": [
        {
            "operator_name": "HASH_JOIN",
            "operator_timing": 1.5,
            "operator_cardinality": 1000,
            "children": [
                {
                    "operator_name": "PARQUET_SCAN",
                    "operator_timing": 0.75,
                    "operator_cardinality": 500,
                    "children": [],
                },
                {
                    "operator_name": "SEQ_SCAN",
                    "operator_timing": 0.25,
                    "operator_cardinality": 200,
                    "children": [],
                },
            ],
        }
    ],
}


class TestParseProfile:
    def test_flattens_the_tree_slowest_first(self):
        ops = parse_profile(PROFILE)
        assert [op.name for op in ops] == ["HASH_JOIN", "PARQUET_SCAN", "SEQ_SCAN"]
        assert ops[0].seconds == 1.5
        assert ops[0].cardinality == 1000

    def test_root_is_not_an_operator(self):
        # The root carries query totals, not a node — counting it would
        # double every run's time.
        assert all(op.name != "SELECT 1" for op in parse_profile(PROFILE))

    def test_missing_fields_default_to_zero(self):
        ops = parse_profile({"children": [{"operator_name": "PROJECTION"}]})
        assert ops == (OperatorCost(name="PROJECTION", seconds=0.0, cardinality=0),)

    def test_empty_profile(self):
        assert parse_profile({}) == ()

    def test_latency_and_rows(self):
        assert profile_seconds(PROFILE) == 2.5
        assert profile_rows(PROFILE) == 17
        assert profile_seconds({}) == 0.0


class TestMergeOperators:
    def test_totals_across_steps(self):
        steps = [
            StepPlan(1, "a", 1.0, 0, (OperatorCost("HASH_JOIN", 1.0, 10),), "1.json"),
            StepPlan(2, "b", 3.0, 0, (OperatorCost("HASH_JOIN", 3.0, 30),), "2.json"),
            StepPlan(3, "c", 2.0, 0, (OperatorCost("SEQ_SCAN", 2.0, 5),), "3.json"),
        ]
        totals = merge_operators(steps)
        assert totals[0] == OperatorCost("HASH_JOIN", 4.0, 40)
        assert totals[1] == OperatorCost("SEQ_SCAN", 2.0, 5)

    def test_no_steps(self):
        assert merge_operators([]) == ()


class TestSummarize:
    def _steps(self):
        return [
            StepPlan(1, "fast", 0.5, 1, (OperatorCost("SEQ_SCAN", 0.5, 1),), "1.json"),
            StepPlan(2, "slow", 9.0, 2, (OperatorCost("HASH_JOIN", 9.0, 2),), "2.json"),
        ]

    def test_slowest_first_and_total(self):
        summary = summarize("R", self._steps())
        assert summary.total_seconds == 9.5
        assert [s.step for s in summary.slowest] == ["slow", "fast"]
        # Declaration order is preserved in `steps`.
        assert [s.step for s in summary.steps] == ["fast", "slow"]

    def test_top_caps_the_slowest_list(self):
        assert len(summarize("R", self._steps(), top=1).slowest) == 1

    def test_format_names_steps_and_operators(self):
        text = format_summary(summarize("R", self._steps()))
        assert "run R" in text
        assert "slow" in text
        assert "HASH_JOIN" in text
        assert "dominant operators" in text

    def test_format_survives_a_zero_second_run(self):
        empty = StepPlan(1, "noop", 0.0, 0, (), "1.json")
        assert "noop" in format_summary(summarize("R", [empty]))


class TestPlanStore:
    def test_records_per_step_files_and_an_index(self, tmp_path):
        store = PlanStore(tmp_path, run_id="RUN")
        store.record(step="load bkt/00", seconds=9.0, profile=PROFILE)
        store.record(step="agg", seconds=1.0, profile=PROFILE)

        run_dir = tmp_path / "RUN"
        assert sorted(p.name for p in run_dir.glob("*.json")) == [
            "001_load_bkt_00.json",
            "002_agg.json",
        ]
        lines = (run_dir / "index.jsonl").read_text().strip().splitlines()
        assert len(lines) == 2
        assert json.loads(lines[0])["step"] == "load bkt/00"

    def test_duckdb_latency_wins_over_wall_clock(self, tmp_path):
        # The wall clock includes the transport round trip; DuckDB's own
        # latency is the number worth comparing across runs.
        plan = PlanStore(tmp_path, run_id="RUN").record(
            step="s", seconds=99.0, profile=PROFILE
        )
        assert plan.seconds == 2.5

    def test_wall_clock_used_when_the_profile_has_no_latency(self, tmp_path):
        plan = PlanStore(tmp_path, run_id="RUN").record(
            step="s", seconds=4.0, profile={"children": []}
        )
        assert plan.seconds == 4.0

    def test_round_trips_through_load_run(self, tmp_path):
        store = PlanStore(tmp_path, run_id="RUN")
        store.record(step="one", seconds=1.0, profile=PROFILE)
        store.record(step="two", seconds=1.0, profile=PROFILE)

        summary = load_run(tmp_path, "RUN")
        assert summary.run_id == "RUN"
        assert [s.step for s in summary.steps] == ["one", "two"]
        assert summary.operator_totals[0] == OperatorCost("HASH_JOIN", 3.0, 2000)

    def test_run_ids_are_listed_chronologically(self, tmp_path):
        for run_id in ("20260101T000000", "20250101T000000"):
            PlanStore(tmp_path, run_id=run_id).record(
                step="s", seconds=1.0, profile=PROFILE
            )
        assert list_runs(tmp_path) == ["20250101T000000", "20260101T000000"]

    def test_listing_a_missing_root_is_empty_not_an_error(self, tmp_path):
        assert list_runs(tmp_path / "nope") == []

    def test_loading_a_missing_run_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_run(tmp_path, "nope")

    def test_generated_run_id_is_a_sortable_timestamp(self):
        run_id = new_run_id()
        assert len(run_id) == 15 and run_id[8] == "T"
