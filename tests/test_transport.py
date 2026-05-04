"""Tests for the transport layer — dispatch, ssh+wsl command shape, CSV parsing."""
from unittest.mock import MagicMock, patch

import pytest

from src.transport import (
    DirectTransport, SshWslTransport, build_transport, _parse_psql_csv,
)


class TestBuildTransport:
    def test_default_is_direct(self):
        t = build_transport(db_config={
            'dialect': 'postgresql+psycopg2', 'user': 'u', 'password': 'p',
            'host': 'h', 'port': '5432', 'database': 'db',
        })
        assert isinstance(t, DirectTransport)
        assert t.name == "direct"

    def test_explicit_direct(self):
        t = build_transport(db_config={'dialect': 'sqlite', 'user': 'u',
                                         'password': 'p', 'host': 'h',
                                         'port': '5', 'database': 'd'},
                             transport="direct")
        assert isinstance(t, DirectTransport)

    def test_ssh_wsl(self):
        t = build_transport(transport="ssh+wsl",
                             ssh="adm@host", container="pgduckdb")
        assert isinstance(t, SshWslTransport)
        assert t.name == "ssh+wsl"
        assert t.ssh == "adm@host"
        assert t.container == "pgduckdb"

    def test_direct_requires_db_config(self):
        with pytest.raises(ValueError, match="db_config"):
            build_transport(transport="direct")

    def test_ssh_wsl_requires_ssh_and_container(self):
        with pytest.raises(ValueError, match="ssh.*container"):
            build_transport(transport="ssh+wsl")
        with pytest.raises(ValueError, match="ssh.*container"):
            build_transport(transport="ssh+wsl", ssh="adm@host")

    def test_unknown_transport_raises(self):
        with pytest.raises(ValueError, match="Unknown transport"):
            build_transport(transport="rsh")


class TestSshWslCommandShape:
    def _mk(self, **overrides) -> SshWslTransport:
        return SshWslTransport(
            ssh="adm@host",
            container="pgduckdb",
            pg_user="postgres",
            pg_database="labma",
            **overrides,
        )

    def test_default_command(self):
        t = self._mk()
        cmd = t._build_command()
        assert cmd[0] == "ssh"
        assert "adm@host" in cmd
        assert "wsl" in cmd
        assert "sudo" in cmd  # default sudo=True
        assert "docker" in cmd
        assert "exec" in cmd
        assert "-i" in cmd
        assert "pgduckdb" in cmd
        assert "psql" in cmd
        assert "-U" in cmd and "postgres" in cmd
        assert "-d" in cmd and "labma" in cmd
        assert "--csv" in cmd
        assert "-f" in cmd and "-" in cmd

    def test_no_sudo(self):
        t = self._mk(sudo=False)
        cmd = t._build_command()
        assert "sudo" not in cmd

    def test_no_wsl(self):
        t = self._mk(wsl=False)
        cmd = t._build_command()
        assert "wsl" not in cmd

    def test_ssh_options_inserted(self):
        t = self._mk(ssh_options=["-o", "ConnectTimeout=5"])
        cmd = t._build_command()
        # Options must come before the ssh target.
        ssh_idx = cmd.index("ssh")
        target_idx = cmd.index("adm@host")
        assert ssh_idx < cmd.index("-o") < target_idx


class TestSshWslExecute:
    @patch("src.transport.subprocess.run")
    def test_parses_csv_response(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout=b"id,name\n1,Alice\n2,Bob\n",
            stderr=b"",
        )
        t = SshWslTransport(ssh="x", container="y")
        result = t.execute("SELECT id, name FROM users")
        assert result.columns == ["id", "name"]
        assert result.rows == [("1", "Alice"), ("2", "Bob")]
        assert result.elapsed_ms >= 0

    @patch("src.transport.subprocess.run")
    def test_failure_raises(self, mock_run):
        mock_run.return_value = MagicMock(
            returncode=1, stdout=b"", stderr=b"FATAL: connection refused",
        )
        t = SshWslTransport(ssh="x", container="y")
        with pytest.raises(RuntimeError, match="connection refused"):
            t.execute("SELECT 1")

    @patch("src.transport.subprocess.run")
    def test_empty_result(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout=b"", stderr=b"")
        t = SshWslTransport(ssh="x", container="y")
        result = t.execute("SELECT 1 WHERE FALSE")
        assert result.columns == []
        assert result.rows == []

    def test_params_not_supported(self):
        t = SshWslTransport(ssh="x", container="y")
        with pytest.raises(NotImplementedError):
            t.execute("SELECT :x", params={"x": 1})


class TestParseCsv:
    def test_simple_header_and_rows(self):
        cols, rows = _parse_psql_csv("a,b\n1,2\n3,4\n")
        assert cols == ["a", "b"]
        assert rows == [("1", "2"), ("3", "4")]

    def test_empty_input(self):
        assert _parse_psql_csv("") == ([], [])
        assert _parse_psql_csv("\n\n") == ([], [])

    def test_quoted_commas(self):
        cols, rows = _parse_psql_csv('name,note\n"alice","says, hi"\n')
        assert cols == ["name", "note"]
        assert rows == [("alice", "says, hi")]
