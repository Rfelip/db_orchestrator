"""Tests for the library entry point — DQL guard, run_sql plumbing, transport dispatch."""
import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.api import (
    DqlOnlyError, QueryResult, build_db_url, run_sql, run_manifest, _check_dql,
)
from src.transport import RawResult


def _qr(columns=None, rows=None, elapsed_ms=10, sql_hash="abcd", transport="direct"):
    return QueryResult(
        columns=columns or [],
        rows=rows or [],
        elapsed_ms=elapsed_ms,
        sql_hash=sql_hash,
        transport=transport,
    )


class TestBuildDbUrl:
    def test_postgres(self):
        url = build_db_url({
            'dialect': 'postgresql+psycopg2',
            'user': 'u', 'password': 'p',
            'host': 'h', 'port': '5432',
            'database': 'db',
        })
        assert url == 'postgresql+psycopg2://u:p@h:5432/db'

    def test_oracle_uses_service(self):
        url = build_db_url({
            'dialect': 'oracle+oracledb',
            'user': 'u', 'password': 'p',
            'host': 'h', 'port': '1521',
            'service': 'svc',
        })
        assert url == 'oracle+oracledb://u:p@h:1521/svc'


class TestDqlGuard:
    def test_select_passes(self):
        _check_dql("SELECT 1")
        _check_dql("WITH x AS (SELECT 1) SELECT * FROM x")
        _check_dql("  -- comment\n  SELECT * FROM t")

    def test_drop_blocked(self):
        with pytest.raises(DqlOnlyError, match="DROP"):
            _check_dql("DROP TABLE foo")

    def test_insert_blocked(self):
        with pytest.raises(DqlOnlyError, match="INSERT"):
            _check_dql("INSERT INTO t VALUES (1)")

    def test_update_blocked(self):
        with pytest.raises(DqlOnlyError, match="UPDATE"):
            _check_dql("UPDATE t SET x=1")


class TestQueryResult:
    def test_to_csv_string(self):
        qr = _qr(columns=['a', 'b'], rows=[(1, 'x'), (2, 'y')])
        csv = qr.to_csv_string()
        assert csv.splitlines()[0] == "a,b"
        assert "1,x" in csv
        assert "2,y" in csv

    def test_row_count(self):
        qr = _qr(columns=['a'], rows=[(1,), (2,), (3,)])
        assert qr.row_count == 3

    def test_carries_sql_hash_and_transport(self):
        qr = _qr(sql_hash="deadbeef", transport="ssh+wsl")
        assert qr.sql_hash == "deadbeef"
        assert qr.transport == "ssh+wsl"


class TestRunSqlDirect:
    """`run_sql` with the default direct transport. We mock the
    transport's execute() to confirm the api wraps RawResult into
    QueryResult and writes a provenance line."""

    @patch("src.transport.DirectTransport.execute")
    def test_returns_query_result(self, mock_execute):
        mock_execute.return_value = RawResult(
            columns=['col1', 'col2'],
            rows=[('a', 1), ('b', 2)],
            elapsed_ms=42,
        )
        with tempfile.TemporaryDirectory() as tmp:
            out = run_sql(
                "SELECT * FROM t",
                db_config={
                    'dialect': 'postgresql+psycopg2',
                    'user': 'u', 'password': 'p',
                    'host': 'h', 'port': '5432', 'database': 'db',
                },
                repo_root=Path(tmp),
            )
        assert isinstance(out, QueryResult)
        assert out.columns == ['col1', 'col2']
        assert out.rows == [('a', 1), ('b', 2)]
        assert out.row_count == 2
        assert out.elapsed_ms == 42
        assert out.transport == "direct"
        assert len(out.sql_hash) == 16

    @patch("src.transport.DirectTransport.execute")
    def test_dql_only_blocks_drop(self, mock_execute):
        with pytest.raises(DqlOnlyError):
            run_sql("DROP TABLE t", db_config={
                'dialect': 'postgresql+psycopg2',
                'user': 'u', 'password': 'p',
                'host': 'h', 'port': '5432', 'database': 'db',
            }, dql_only=True, log_provenance=False)
        mock_execute.assert_not_called()

    @patch("src.transport.DirectTransport.execute")
    def test_limit_wraps_postgres(self, mock_execute):
        mock_execute.return_value = RawResult(columns=[], rows=[], elapsed_ms=1)
        with tempfile.TemporaryDirectory() as tmp:
            run_sql("SELECT * FROM t", db_config={
                'dialect': 'postgresql+psycopg2',
                'user': 'u', 'password': 'p',
                'host': 'h', 'port': '5432', 'database': 'db',
            }, limit=10, repo_root=Path(tmp))
        executed_sql = mock_execute.call_args.args[0]
        assert "LIMIT 10" in executed_sql

    @patch("src.transport.DirectTransport.execute")
    def test_limit_wraps_oracle_with_rownum(self, mock_execute):
        mock_execute.return_value = RawResult(columns=[], rows=[], elapsed_ms=1)
        with tempfile.TemporaryDirectory() as tmp:
            run_sql("SELECT * FROM t", db_config={
                'dialect': 'oracle+oracledb',
                'user': 'u', 'password': 'p',
                'host': 'h', 'port': '1521', 'service': 's',
            }, limit=5, repo_root=Path(tmp))
        executed_sql = mock_execute.call_args.args[0]
        assert "ROWNUM <= 5" in executed_sql


class TestRunSqlSshWsl:
    """`run_sql` with the ssh+wsl transport. We mock subprocess.run."""

    @patch("src.transport.subprocess.run")
    def test_routes_to_ssh_wsl(self, mock_run):
        # Mock psql --csv output: header + one row.
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=b"col1,col2\nfoo,42\n",
            stderr=b"",
        )
        with tempfile.TemporaryDirectory() as tmp:
            out = run_sql(
                "SELECT 1",
                transport="ssh+wsl",
                ssh="adm@host",
                container="pgduckdb",
                pg_user="postgres",
                pg_database="labma",
                repo_root=Path(tmp),
            )
        assert out.transport == "ssh+wsl"
        assert out.columns == ["col1", "col2"]
        assert out.rows == [("foo", "42")]
        # Confirm the command was shaped like ssh ... wsl docker exec ... psql --csv
        cmd = mock_run.call_args.args[0]
        assert "ssh" in cmd[0]
        assert "wsl" in cmd
        assert "docker" in cmd
        assert "exec" in cmd
        assert "pgduckdb" in cmd
        assert "--csv" in cmd

    @patch("src.transport.subprocess.run")
    def test_ssh_wsl_failure_raises_runtime_error(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=1,
            stdout=b"",
            stderr=b"ERROR: relation does not exist",
        )
        with tempfile.TemporaryDirectory() as tmp:
            with pytest.raises(RuntimeError, match="ssh\\+wsl psql failed"):
                run_sql(
                    "SELECT 1",
                    transport="ssh+wsl",
                    ssh="adm@host",
                    container="pgduckdb",
                    repo_root=Path(tmp),
                )


class TestProvenance:
    @patch("src.transport.DirectTransport.execute")
    def test_writes_ok_line(self, mock_execute):
        mock_execute.return_value = RawResult(
            columns=['a'], rows=[(1,)], elapsed_ms=5,
        )
        with tempfile.TemporaryDirectory() as tmp:
            run_sql(
                "SELECT 1",
                db_config={'dialect': 'postgresql+psycopg2', 'user': 'u',
                           'password': 'p', 'host': 'h', 'port': '5432',
                           'database': 'db'},
                fetch_name="my_query",
                repo_root=Path(tmp),
            )
            log_path = Path(tmp) / "output" / "_ad_hoc" / "_provenance.jsonl"
            assert log_path.exists()
            line = json.loads(log_path.read_text().strip())
            assert line["status"] == "ok"
            assert line["fetch"] == "my_query"
            assert line["transport"] == "direct"
            assert line["rows"] == 1

    @patch("src.transport.DirectTransport.execute")
    def test_writes_error_line(self, mock_execute):
        mock_execute.side_effect = RuntimeError("connection refused")
        with tempfile.TemporaryDirectory() as tmp:
            with pytest.raises(RuntimeError):
                run_sql(
                    "SELECT 1",
                    db_config={'dialect': 'postgresql+psycopg2', 'user': 'u',
                               'password': 'p', 'host': 'h', 'port': '5432',
                               'database': 'db'},
                    repo_root=Path(tmp),
                )
            log_path = Path(tmp) / "output" / "_ad_hoc" / "_provenance.jsonl"
            line = json.loads(log_path.read_text().strip())
            assert line["status"] == "error"
            assert "connection refused" in line["error"]

    @patch("src.transport.DirectTransport.execute")
    def test_log_provenance_false_skips_write(self, mock_execute):
        mock_execute.return_value = RawResult(columns=[], rows=[], elapsed_ms=1)
        with tempfile.TemporaryDirectory() as tmp:
            run_sql(
                "SELECT 1",
                db_config={'dialect': 'postgresql+psycopg2', 'user': 'u',
                           'password': 'p', 'host': 'h', 'port': '5432',
                           'database': 'db'},
                log_provenance=False,
                repo_root=Path(tmp),
            )
            log_path = Path(tmp) / "output" / "_ad_hoc" / "_provenance.jsonl"
            assert not log_path.exists()


class TestRunSqlByTarget:
    """`target=` is the no-secrets path — caller names a target,
    everything else is read from env via load_targets()."""

    @patch("src.transport.subprocess.run")
    @patch("src.api._resolve_target")
    def test_target_resolves_to_ssh_wsl(self, mock_resolve, mock_subprocess):
        mock_resolve.return_value = {
            "transport": "ssh+wsl",
            "ssh": "adm@host",
            "container": "pgduckdb",
            "pg_user": "postgres",
            "pg_database": "labma",
            "wsl": "true",
            "sudo": "true",
        }
        mock_subprocess.return_value = MagicMock(
            returncode=0, stdout=b"a\n1\n", stderr=b"",
        )
        with tempfile.TemporaryDirectory() as tmp:
            out = run_sql("SELECT 1", target="MR3", repo_root=Path(tmp))
        assert out.transport == "ssh+wsl"
        mock_resolve.assert_called_once_with("MR3")

    @patch("src.transport.DirectTransport.execute")
    @patch("src.api._resolve_target")
    def test_target_resolves_to_direct(self, mock_resolve, mock_execute):
        mock_resolve.return_value = {
            "transport": "direct",
            "dialect": "postgresql+psycopg2",
            "user": "u",
            "password": "p",
            "host": "h",
            "port": "5432",
            "database": "db",
        }
        mock_execute.return_value = RawResult(
            columns=["x"], rows=[(1,)], elapsed_ms=1,
        )
        with tempfile.TemporaryDirectory() as tmp:
            out = run_sql("SELECT 1", target="LOCAL", repo_root=Path(tmp))
        assert out.transport == "direct"

    def test_target_with_db_config_rejects(self):
        # The point of `target=` is "no extra info" — passing both is
        # a misuse and should fail loudly.
        with pytest.raises(ValueError, match="do not also pass"):
            run_sql("SELECT 1", target="MR3", db_config={"x": 1})


class TestRunManifest:
    @patch("src.api.Executor")
    def test_run_manifest_constructs_executor(self, MockExecutor):
        run_manifest(
            "manifest.yaml",
            db_config={'dialect': 'postgresql+psycopg2', 'user': 'u',
                       'password': 'p', 'host': 'h', 'port': '5432',
                       'database': 'db'},
            notifier_config={'discord_webhook_url': 'https://x.test/y'},
            dry_run=True,
        )
        MockExecutor.assert_called_once()
        kwargs = MockExecutor.call_args.kwargs
        assert kwargs['manifest_path'] == "manifest.yaml"
        assert kwargs['dry_run'] is True
        MockExecutor.return_value.run.assert_called_once()

    @patch("src.api.Executor")
    def test_run_manifest_default_notifier_config_is_empty_dict(self, MockExecutor):
        run_manifest(
            "manifest.yaml",
            db_config={'dialect': 'postgresql+psycopg2', 'user': 'u',
                       'password': 'p', 'host': 'h', 'port': '5432',
                       'database': 'db'},
        )
        kwargs = MockExecutor.call_args.kwargs
        assert kwargs['notifier_config'] == {}
