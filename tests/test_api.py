"""Tests for the library entry point — DQL guard, URL builder, run_sql plumbing."""
from unittest.mock import MagicMock, patch

import pytest

from src.api import (
    DqlOnlyError, QueryResult, build_db_url, run_sql, run_manifest, _check_dql,
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
        qr = QueryResult(
            columns=['a', 'b'],
            rows=[(1, 'x'), (2, 'y')],
            elapsed_ms=10,
        )
        csv = qr.to_csv_string()
        assert csv.splitlines()[0] == "a,b"
        assert "1,x" in csv
        assert "2,y" in csv

    def test_row_count(self):
        qr = QueryResult(columns=['a'], rows=[(1,), (2,), (3,)], elapsed_ms=5)
        assert qr.row_count == 3


class TestRunSql:
    """`run_sql` plumbs into DatabaseManager. We mock the manager to
    confirm the contract: build URL, get session, execute, package result."""

    @patch("src.api.DatabaseManager")
    def test_returns_query_result(self, MockDB):
        mock_result = MagicMock()
        mock_result.keys.return_value = ['col1', 'col2']
        mock_result.fetchall.return_value = [('a', 1), ('b', 2)]
        MockDB.return_value.execute_query.return_value = mock_result

        out = run_sql("SELECT * FROM t", db_config={
            'dialect': 'postgresql+psycopg2',
            'user': 'u', 'password': 'p',
            'host': 'h', 'port': '5432', 'database': 'db',
        })
        assert isinstance(out, QueryResult)
        assert out.columns == ['col1', 'col2']
        assert out.rows == [('a', 1), ('b', 2)]
        assert out.row_count == 2

    @patch("src.api.DatabaseManager")
    def test_dql_only_blocks_drop(self, MockDB):
        with pytest.raises(DqlOnlyError):
            run_sql("DROP TABLE t", db_config={
                'dialect': 'postgresql+psycopg2',
                'user': 'u', 'password': 'p',
                'host': 'h', 'port': '5432', 'database': 'db',
            }, dql_only=True)
        MockDB.assert_not_called()

    @patch("src.api.DatabaseManager")
    def test_limit_wraps_postgres(self, MockDB):
        mock_result = MagicMock()
        mock_result.keys.return_value = []
        mock_result.fetchall.return_value = []
        MockDB.return_value.execute_query.return_value = mock_result

        run_sql("SELECT * FROM t", db_config={
            'dialect': 'postgresql+psycopg2',
            'user': 'u', 'password': 'p',
            'host': 'h', 'port': '5432', 'database': 'db',
        }, limit=10)
        # Confirm the executed SQL was wrapped, not the raw input.
        call_args = MockDB.return_value.execute_query.call_args
        executed_sql = call_args.args[0] if call_args.args else call_args.kwargs.get("sql")
        assert "LIMIT 10" in executed_sql

    @patch("src.api.DatabaseManager")
    def test_limit_wraps_oracle_with_rownum(self, MockDB):
        mock_result = MagicMock()
        mock_result.keys.return_value = []
        mock_result.fetchall.return_value = []
        MockDB.return_value.execute_query.return_value = mock_result

        run_sql("SELECT * FROM t", db_config={
            'dialect': 'oracle+oracledb',
            'user': 'u', 'password': 'p',
            'host': 'h', 'port': '1521', 'service': 's',
        }, limit=5)
        call_args = MockDB.return_value.execute_query.call_args
        executed_sql = call_args.args[0] if call_args.args else call_args.kwargs.get("sql")
        assert "ROWNUM <= 5" in executed_sql


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
