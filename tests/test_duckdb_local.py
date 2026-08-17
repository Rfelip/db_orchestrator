"""Tests for `DuckDbLocalTransport` — DuckDB in a child process, no ssh.

The transport exists so `main.py` can run *where the data is*. Two things
therefore have to be true and are checked here: it behaves like the ssh
transport wherever the two should agree (same helper, same protocol, same
session semantics), and the settings a target declares actually land in
the DuckDB that serves the run. The second is the expensive one to get
wrong — a transport that silently ignores `threads` or `memory_limit`
costs a whole pipeline rebuild before anyone notices.

Skipped when `duckdb` is not importable.
"""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.api import _manifest_target
from src.duckdb_session import (
    Completed,
    SessionError,
    open_transport_session,
)
from src.plans import PlanStore
from src.transport import (
    DUCKDB_HELPER,
    DuckDbLocalTransport,
    DuckDbSettings,
    DuckDbSshTransport,
    RawResult,
    build_transport,
)

pytest.importorskip("duckdb")


@pytest.fixture
def settings(tmp_path):
    return DuckDbSettings(
        memory_limit="2GB",
        threads=3,
        temp_directory=str(tmp_path / "spill"),
        max_temp_directory_size="4GB",
    )


@pytest.fixture
def transport(tmp_path, settings):
    return DuckDbLocalTransport(
        helper_path=str(tmp_path / "helper" / "_orch_duckdb.py"),
        settings=settings,
    )


def _declares(reported: str, declared_bytes: float) -> bool:
    """Whether DuckDB's echo of a byte ceiling is the value we declared.

    It never echoes the decimal form the setting was written in: '2GB'
    (2e9 bytes) comes back as '1.8 GiB', binary units truncated to one
    decimal. So the readback is the declared value minus at most one tick
    of whichever unit DuckDB picked."""
    value, unit = reported.split()
    scale = {"KiB": 1024.0, "MiB": 1024.0**2, "GiB": 1024.0**3}[unit]
    echoed = float(value) * scale
    return declared_bytes - scale * 0.1 < echoed <= declared_bytes


class TestConstruction:
    def test_build_transport_selects_it_by_name(self, tmp_path):
        t = build_transport(transport="duckdb", helper_path=str(tmp_path / "h.py"))
        assert isinstance(t, DuckDbLocalTransport)
        assert t.name == "duckdb"

    def test_build_transport_threads_shorthand(self):
        t = build_transport(transport="duckdb", threads=5)
        assert t.settings.threads == 5
        assert t.threads == 5

    def test_build_transport_settings_wins_over_threads(self):
        t = build_transport(
            transport="duckdb",
            threads=4,
            settings=DuckDbSettings(threads=16, memory_limit="32GB"),
        )
        assert (t.settings.threads, t.settings.memory_limit) == (16, "32GB")

    def test_defaults_to_the_interpreter_running_the_orchestrator(self):
        # The env that resolved `duckdb` is this one; "python3" on PATH
        # is a different environment and may not have it.
        assert build_transport(transport="duckdb").python == sys.executable

    def test_named_python_is_honoured(self):
        t = build_transport(transport="duckdb", python="/venv/bin/python")
        assert t.python == "/venv/bin/python"

    def test_helper_path_tilde_is_expanded_for_the_argv(self):
        t = DuckDbLocalTransport(helper_path="~/duck_helper.py")
        assert not t.helper_path.startswith("~")


class TestSessionCommand:
    def test_writes_the_helper_and_serves_it(self, transport, tmp_path):
        assert transport.session_command() == [
            sys.executable,
            str(tmp_path / "helper" / "_orch_duckdb.py"),
            "--serve",
        ]
        written = (tmp_path / "helper" / "_orch_duckdb.py").read_text()
        assert written == DUCKDB_HELPER

    def test_the_helper_is_written_once_per_process(self, transport, tmp_path):
        transport.session_command()
        path = tmp_path / "helper" / "_orch_duckdb.py"
        path.write_text("# clobbered", encoding="utf-8")
        transport.session_command()
        assert path.read_text() == "# clobbered"


class TestSessionState:
    """Same load-bearing property the ssh session has: one connection."""

    def test_temp_table_survives_across_run_calls(self, transport):
        with open_transport_session(transport) as session:
            assert isinstance(
                session.run("CREATE TEMP TABLE _p1b_07 AS SELECT 42 AS v"), Completed
            )
            read_back = session.run("SELECT v FROM _p1b_07")
            assert isinstance(read_back, RawResult)
            assert read_back.rows == [(42,)]
            session.run("INSERT INTO _p1b_07 VALUES (43)")
            assert session.run("SELECT count(*) AS n FROM _p1b_07").rows == [(2,)]

    def test_separate_sessions_do_not_share_state(self, transport):
        with open_transport_session(transport) as first:
            first.run("CREATE TEMP TABLE gone AS SELECT 1")
        with open_transport_session(transport) as second:
            with pytest.raises(SessionError):
                second.run("SELECT * FROM gone")

    def test_copy_to_parquet_reports_rows_written(self, transport, tmp_path):
        target = tmp_path / "out.parquet"
        with open_transport_session(transport) as session:
            session.run("CREATE TEMP TABLE t AS SELECT * FROM range(5) tbl(i)")
            result = session.run(
                f"COPY (SELECT * FROM t) TO '{target}' (FORMAT 'parquet')"
            )
        assert isinstance(result, Completed)
        assert (result.statement, result.row_count) == ("COPY", 5)
        assert target.exists()

    def test_plans_are_recorded_when_profiling_is_on(self, tmp_path, settings):
        profiling = DuckDbLocalTransport(
            helper_path=str(tmp_path / "h.py"),
            settings=DuckDbSettings(
                memory_limit=settings.memory_limit,
                threads=settings.threads,
                temp_directory=settings.temp_directory,
                max_temp_directory_size=settings.max_temp_directory_size,
                profile=True,
            ),
        )
        store = PlanStore(tmp_path / "plans", run_id="RUN1")
        with open_transport_session(profiling, plans=store) as session:
            session.run("SELECT count(*) AS n FROM range(50000)", step="count_rows")
        assert [s.step for s in store.summary().steps] == ["count_rows"]


class TestSettingsReachDuckDb:
    """The proof that the knobs are not decorative: read them back from
    inside the session that a declared target opened."""

    def test_every_declared_knob_is_live_in_the_session(self, transport, settings):
        with open_transport_session(transport) as session:
            live = {
                knob: session.run(f"SELECT current_setting('{knob}') AS v").rows[0][0]
                for knob in (
                    "threads",
                    "memory_limit",
                    "temp_directory",
                    "max_temp_directory_size",
                    "preserve_insertion_order",
                )
            }
        assert live["threads"] == settings.threads
        assert live["temp_directory"] == settings.temp_directory
        assert live["preserve_insertion_order"] is True
        assert _declares(live["memory_limit"], 2e9), live["memory_limit"]
        assert _declares(live["max_temp_directory_size"], 4e9), live[
            "max_temp_directory_size"
        ]

    def test_a_different_target_gets_different_knobs(self, tmp_path):
        other = DuckDbLocalTransport(
            helper_path=str(tmp_path / "h.py"),
            settings=DuckDbSettings(
                memory_limit="6GB",
                threads=1,
                temp_directory=str(tmp_path / "elsewhere"),
                max_temp_directory_size="9GB",
                preserve_insertion_order=True,
            ),
        )
        with open_transport_session(other) as session:
            assert session.run("SELECT current_setting('threads') AS v").rows == [(1,)]
            assert session.run(
                "SELECT current_setting('preserve_insertion_order') AS v"
            ).rows == [(True,)]
            reported = session.run("SELECT current_setting('memory_limit') AS v").rows[
                0
            ][0]
        assert _declares(reported, 6e9), reported

    def test_the_spill_directory_is_created_before_duckdb_needs_it(self, transport):
        assert not Path(transport.settings.temp_directory).exists()
        with open_transport_session(transport) as session:
            session.run("SELECT 1")
        assert Path(transport.settings.temp_directory).is_dir()


class TestTeardown:
    def test_process_dies_on_clean_exit(self, transport):
        with open_transport_session(transport) as session:
            session.run("SELECT 1")
        assert session.closed
        assert session._proc.poll() is not None

    def test_process_dies_when_the_block_raises(self, transport):
        captured = {}

        class Boom(RuntimeError):
            pass

        with pytest.raises(Boom):
            with open_transport_session(transport) as session:
                session.run("SELECT 1")
                captured["session"] = session
                raise Boom("step blew up")

        assert captured["session"].closed
        assert captured["session"]._proc.poll() is not None

    def test_a_failed_statement_keeps_the_session_usable(self, transport):
        with open_transport_session(transport) as session:
            session.run("CREATE TEMP TABLE keep AS SELECT 1 AS v")
            with pytest.raises(SessionError, match="no_such_table|Table with name"):
                session.run("SELECT * FROM no_such_table")
            assert session.run("SELECT v FROM keep").rows == [(1,)]

    def test_closed_session_refuses_work(self, transport):
        with open_transport_session(transport) as session:
            pass
        with pytest.raises(SessionError, match="closed"):
            session.run("SELECT 1")


class TestOneShotExecute:
    def test_execute_returns_rows(self, transport):
        result = transport.execute("SELECT 1 AS a, 'x' AS b")
        assert result.columns == ["a", "b"]
        assert result.rows == [("1", "x")]

    def test_execute_refuses_bind_params(self, transport):
        with pytest.raises(NotImplementedError, match="bind params"):
            transport.execute("SELECT :x", params={"x": 1})

    def test_a_failing_statement_raises_with_the_helper_stderr(self, transport):
        with pytest.raises(RuntimeError, match="duckdb failed"):
            transport.execute("SELECT * FROM no_such_table")


class TestParityWithSsh:
    """Where the two transports should agree, they must — the local one
    exists to remove a second execution path, not to add one."""

    def test_same_helper_program(self, transport, tmp_path):
        transport.session_command()
        local_helper = (tmp_path / "helper" / "_orch_duckdb.py").read_text()

        ssh = DuckDbSshTransport(ssh="mr3", wsl=False, helper_path="/tmp/h.py")
        with patch("src.transport.subprocess.run") as run:
            run.return_value = MagicMock(returncode=0, stdout=b"", stderr=b"")
            ssh.session_command()
            uploaded = run.call_args.kwargs["input"].decode("utf-8")
        assert local_helper == uploaded

    def test_same_serve_argv_below_the_ssh_prefix(self, tmp_path, settings):
        local = DuckDbLocalTransport(
            helper_path="/tmp/h.py", settings=settings, python="/venv/bin/python"
        )
        ssh = DuckDbSshTransport(
            ssh="mr3", wsl=False, helper_path="/tmp/h.py", python="/venv/bin/python"
        )
        with patch("src.transport.subprocess.run") as run:
            run.return_value = MagicMock(returncode=0, stdout=b"", stderr=b"")
            remote = ssh.session_command()
        assert remote[:2] == ["ssh", "mr3"]
        assert remote[2:] == local.session_command()

    def test_same_settings_payload_on_the_first_stdin_line(self, transport, settings):
        with patch("src.transport.subprocess.run") as run:
            run.return_value = MagicMock(returncode=0, stdout=b"a\n1\n", stderr=b"")
            transport.execute("SELECT 1")
            local_stdin = run.call_args.kwargs["input"].decode()

        ssh = DuckDbSshTransport(ssh="mr3", wsl=False, settings=settings)
        ssh._helper_synced = True
        with patch("src.transport.subprocess.run") as run:
            run.return_value = MagicMock(returncode=0, stdout=b"a\n1\n", stderr=b"")
            ssh.execute("SELECT 1")
            remote_stdin = run.call_args.kwargs["input"].decode()

        assert local_stdin == remote_stdin
        assert json.loads(local_stdin.split("\n", 1)[0]) == settings.as_payload()


class TestTargetConfiguration:
    """`DB_TARGET_<NAME>_TRANSPORT=duckdb` plus the existing knob keys."""

    def test_a_declared_local_target_builds_the_local_transport(
        self, monkeypatch, tmp_path
    ):
        for key, value in {
            "TRANSPORT": "duckdb",
            "HELPER_PATH": str(tmp_path / "h.py"),
            "MEMORY_LIMIT": "12GB",
            "THREADS": "6",
            "TEMP_DIRECTORY": str(tmp_path / "spill"),
            "MAX_TEMP_DIRECTORY_SIZE": "128GB",
            "PRESERVE_INSERTION_ORDER": "false",
            "PROFILE": "true",
        }.items():
            monkeypatch.setenv(f"DB_TARGET_LOCALDUCK_{key}", value)

        built, cfg = _manifest_target("LOCALDUCK")
        assert isinstance(built, DuckDbLocalTransport)
        assert built.settings == DuckDbSettings(
            memory_limit="12GB",
            threads=6,
            temp_directory=str(tmp_path / "spill"),
            max_temp_directory_size="128GB",
            preserve_insertion_order=False,
            profile=True,
        )
        assert built.helper_path == str(tmp_path / "h.py")
        assert cfg["transport"] == "duckdb"

    def test_a_local_target_needs_no_ssh_key(self, monkeypatch, tmp_path):
        # The whole point: nothing in the argv reaches for ssh.
        monkeypatch.setenv("DB_TARGET_LOCALDUCK2_TRANSPORT", "duckdb")
        monkeypatch.setenv("DB_TARGET_LOCALDUCK2_HELPER_PATH", str(tmp_path / "h.py"))
        built, _ = _manifest_target("LOCALDUCK2")
        assert "ssh" not in built.session_command()
