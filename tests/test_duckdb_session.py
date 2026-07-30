"""Tests for the persistent DuckDB session.

The session's whole reason to exist is that state survives between
statements, and mocks cannot prove that. So the tests here drive the
*real* remote helper (`src.transport.DUCKDB_HELPER`) against a *real*
local DuckDB, over the same JSON-line protocol ssh would carry. Only the
transport hop is missing, and that hop is not what can break the
semantics.

Skipped when `duckdb` is not importable.
"""

import json
import subprocess
import sys

import pytest

from src.duckdb_session import (
    Completed,
    DuckDbSession,
    SessionError,
    classify,
    leading_keyword,
    open_duckdb_session,
    write_helper,
)
from src.plans import PlanStore
from src.transport import DuckDbSettings, RawResult

duckdb = pytest.importorskip("duckdb")


@pytest.fixture
def helper(tmp_path):
    path = tmp_path / "_orch_duckdb.py"
    write_helper(path)
    return path


@pytest.fixture
def settings(tmp_path):
    return DuckDbSettings(
        memory_limit="1GB",
        threads=2,
        temp_directory=str(tmp_path / "spill"),
        max_temp_directory_size="1GB",
    )


def _command(helper, *extra):
    return [sys.executable, str(helper), "--serve", *extra]


class TestSessionState:
    """The load-bearing property: one connection across many statements."""

    def test_temp_table_survives_across_execute_calls(self, helper, settings):
        with open_duckdb_session(_command(helper), settings) as session:
            created = session.run("CREATE TEMP TABLE _p1b_07 AS SELECT 42 AS v")
            assert isinstance(created, Completed)

            read_back = session.run("SELECT v FROM _p1b_07")
            assert isinstance(read_back, RawResult)
            assert read_back.rows == [(42,)]

            session.run("INSERT INTO _p1b_07 VALUES (43)")
            again = session.run("SELECT count(*) AS n FROM _p1b_07")
            assert again.rows == [(2,)]

    def test_macro_and_settings_survive_too(self, helper, settings):
        with open_duckdb_session(_command(helper), settings) as session:
            session.run("CREATE OR REPLACE MACRO plus_one(x) AS x + 1")
            assert session.run("SELECT plus_one(1) AS v").rows == [(2,)]
            threads = session.run("SELECT current_setting('threads') AS t")
            assert threads.rows == [(2,)]

    def test_separate_sessions_do_not_share_state(self, helper, settings):
        with open_duckdb_session(_command(helper), settings) as first:
            first.run("CREATE TEMP TABLE gone AS SELECT 1")
        with open_duckdb_session(_command(helper), settings) as second:
            with pytest.raises(SessionError):
                second.run("SELECT * FROM gone")

    def test_copy_to_parquet_reports_rows_written(self, helper, settings, tmp_path):
        target = tmp_path / "out.parquet"
        with open_duckdb_session(_command(helper), settings) as session:
            session.run("CREATE TEMP TABLE t AS SELECT * FROM range(5) tbl(i)")
            result = session.run(
                f"COPY (SELECT * FROM t) TO '{target}' (FORMAT 'parquet')"
            )
        assert isinstance(result, Completed)
        assert result.statement == "COPY"
        assert result.row_count == 5
        assert target.exists()


class TestTeardown:
    def test_process_dies_when_the_block_raises(self, helper, settings):
        captured = {}

        class Boom(RuntimeError):
            pass

        with pytest.raises(Boom):
            with open_duckdb_session(_command(helper), settings) as session:
                session.run("SELECT 1")
                captured["session"] = session
                assert session._proc.poll() is None
                raise Boom("step blew up")

        session = captured["session"]
        assert session.closed
        assert session._proc.poll() is not None

    def test_process_dies_on_clean_exit(self, helper, settings):
        with open_duckdb_session(_command(helper), settings) as session:
            session.run("SELECT 1")
        assert session.closed
        assert session._proc.poll() is not None

    def test_close_is_idempotent(self, helper, settings):
        with open_duckdb_session(_command(helper), settings) as session:
            pass
        session.close()
        assert session.closed

    def test_closed_session_refuses_work(self, helper, settings):
        with open_duckdb_session(_command(helper), settings) as session:
            pass
        with pytest.raises(SessionError, match="closed"):
            session.run("SELECT 1")

    def test_failed_statement_keeps_the_session_usable(self, helper, settings):
        with open_duckdb_session(_command(helper), settings) as session:
            session.run("CREATE TEMP TABLE keep AS SELECT 1 AS v")
            with pytest.raises(SessionError, match="no_such_table|Table with name"):
                session.run("SELECT * FROM no_such_table")
            # The failure was the statement's, not the session's.
            assert session.run("SELECT v FROM keep").rows == [(1,)]

    def test_dead_process_surfaces_stderr(self, settings, tmp_path):
        broken = tmp_path / "broken.py"
        broken.write_text("import sys\nsys.stderr.write('kaboom\\n')\nsys.exit(3)\n")
        with pytest.raises(SessionError, match="kaboom"):
            with open_duckdb_session([sys.executable, str(broken)], settings):
                pass


class TestProfileCapture:
    def test_plans_are_recorded_per_step(self, helper, settings, tmp_path):
        store = PlanStore(tmp_path / "plans", run_id="RUN1")
        profiling = DuckDbSettings(
            memory_limit=settings.memory_limit,
            threads=settings.threads,
            temp_directory=settings.temp_directory,
            max_temp_directory_size=settings.max_temp_directory_size,
            profile=True,
        )
        with open_duckdb_session(_command(helper), profiling, plans=store) as session:
            session.run("SELECT count(*) AS n FROM range(50000)", step="count_rows")
            session.run("CREATE TEMP TABLE t AS SELECT * FROM range(10)", step="make_t")

        summary = store.summary()
        assert summary.run_id == "RUN1"
        assert [s.step for s in summary.steps] == ["count_rows", "make_t"]
        assert all(s.seconds >= 0 for s in summary.steps)
        assert summary.operator_totals  # at least one operator was named
        stored = sorted((tmp_path / "plans" / "RUN1").glob("*.json"))
        assert len(stored) == 2
        assert json.loads(stored[0].read_text())["latency"] >= 0

    def test_nothing_recorded_when_profiling_is_off(self, helper, settings, tmp_path):
        store = PlanStore(tmp_path / "plans", run_id="RUN2")
        with open_duckdb_session(_command(helper), settings, plans=store) as session:
            session.run("SELECT 1", step="x")
        assert not (tmp_path / "plans" / "RUN2").exists()


class TestLeadingKeyword:
    def test_plain(self):
        assert leading_keyword("select 1") == "SELECT"

    def test_skips_line_comments(self):
        assert leading_keyword("-- explain\n-- more\nCOPY (SELECT 1) TO 'x'") == "COPY"

    def test_skips_block_comments(self):
        assert leading_keyword("/* {{ bkt }} */\n  create table t") == "CREATE"

    def test_leading_paren(self):
        assert leading_keyword("(SELECT 1)") == "SELECT"

    def test_empty(self):
        assert leading_keyword("   ") == ""


class TestClassify:
    def test_select_is_rows_even_when_named_count(self):
        result = classify("SELECT 1 AS Count", ["count"], [(1,)], 5)
        assert isinstance(result, RawResult)

    def test_copy_is_completed(self):
        result = classify("COPY (SELECT 1) TO 'x'", ["count"], [(7,)], 5)
        assert result == Completed(statement="COPY", row_count=7, elapsed_ms=5)

    def test_set_is_completed_without_a_count(self):
        result = classify("SET threads=4", ["success"], [], 1)
        assert result == Completed(statement="SET", row_count=None, elapsed_ms=1)

    def test_with_cte_is_rows(self):
        result = classify("WITH a AS (SELECT 1) SELECT * FROM a", ["count"], [(1,)], 2)
        assert isinstance(result, RawResult)


class TestProtocolErrors:
    """Failures that do not need a live DuckDB — driven by a stub process."""

    def test_start_rejects_a_helper_that_is_not_ready(self, monkeypatch):
        session = DuckDbSession(subprocess.Popen([sys.executable, "-c", "pass"]))
        monkeypatch.setattr(session, "_send", lambda payload: None)
        monkeypatch.setattr(session, "_receive", lambda: {"ready": False})
        with pytest.raises(SessionError, match="did not report ready"):
            session.start(DuckDbSettings())
