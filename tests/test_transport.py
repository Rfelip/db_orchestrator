"""Tests for the transport layer — dispatch, ssh+wsl command shape, CSV parsing."""

import json
from unittest.mock import MagicMock, patch

import pytest

from src.transport import (
    DirectTransport,
    DuckDbSettings,
    DuckDbSshTransport,
    SshWslTransport,
    build_transport,
    coerce_bool,
    _parse_psql_csv,
)


class TestBuildTransport:
    def test_default_is_direct(self):
        t = build_transport(
            db_config={
                "dialect": "postgresql+psycopg2",
                "user": "u",
                "password": "p",
                "host": "h",
                "port": "5432",
                "database": "db",
            }
        )
        assert isinstance(t, DirectTransport)
        assert t.name == "direct"

    def test_explicit_direct(self):
        t = build_transport(
            db_config={
                "dialect": "sqlite",
                "user": "u",
                "password": "p",
                "host": "h",
                "port": "5",
                "database": "d",
            },
            transport="direct",
        )
        assert isinstance(t, DirectTransport)

    def test_ssh_wsl(self):
        t = build_transport(transport="ssh+wsl", ssh="adm@host", container="pgduckdb")
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
            returncode=1,
            stdout=b"",
            stderr=b"FATAL: connection refused",
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


class TestDuckDbSettings:
    def test_defaults_are_the_measured_mr3_values(self):
        s = DuckDbSettings()
        assert s.memory_limit == "16GB"
        assert s.threads == 8
        assert s.preserve_insertion_order is True

    def test_temp_directory_defaults_off_the_raid(self):
        # ~ is the remote user's home, which on MR3 is NVMe. /mnt/BANCOS
        # is a rotational RAID1 and spill is write-heavy.
        assert DuckDbSettings().temp_directory.startswith("~")

    def test_rejects_a_nonsense_memory_limit(self):
        with pytest.raises(ValueError, match="memory_limit"):
            DuckDbSettings(memory_limit="lots")

    def test_rejects_zero_threads(self):
        with pytest.raises(ValueError, match="threads"):
            DuckDbSettings(threads=0)

    def test_rejects_an_empty_temp_directory(self):
        with pytest.raises(ValueError, match="temp_directory"):
            DuckDbSettings(temp_directory="  ")

    def test_rejects_an_unbounded_spill(self):
        # The host is shared; an unbounded spill takes down someone else.
        with pytest.raises(ValueError, match="max_temp_directory_size"):
            DuckDbSettings(max_temp_directory_size="")
        with pytest.raises(ValueError, match="max_temp_directory_size"):
            DuckDbSettings(max_temp_directory_size="unlimited")

    def test_from_mapping_reads_env_style_strings(self):
        s = DuckDbSettings.from_mapping(
            {
                "memory_limit": "24GB",
                "threads": "12",
                "temp_directory": "/nvme/spill",
                "max_temp_directory_size": "256GB",
                "preserve_insertion_order": "true",
                "profile": "yes",
            }
        )
        assert (s.memory_limit, s.threads) == ("24GB", 12)
        assert s.temp_directory == "/nvme/spill"
        assert s.preserve_insertion_order is True
        assert s.profile is True

    def test_from_mapping_absent_keys_keep_defaults(self):
        assert DuckDbSettings.from_mapping({}) == DuckDbSettings()

    def test_partitioned_write_ceiling_clears_the_widest_write(self):
        # `eventos_final` grava 280 particoes. Abaixo disso o DuckDB despeja e
        # reabre particao, e cada despejo vira um arquivo pequeno.
        assert DuckDbSettings().partitioned_write_max_open_files >= 280

    def test_partitioned_write_ceiling_travels_para_o_helper(self):
        # O helper remoto so ve o payload; um campo que nao viaja nao vira SET.
        assert (
            DuckDbSettings(partitioned_write_max_open_files=333).as_payload()[
                "partitioned_write_max_open_files"
            ]
            == 333
        )

    def test_partitioned_write_ceiling_reads_env_style_strings(self):
        s = DuckDbSettings.from_mapping({"partitioned_write_max_open_files": "1024"})
        assert s.partitioned_write_max_open_files == 1024

    def test_payload_ships_temp_directory_unexpanded(self):
        # Only the remote knows where ~ is.
        payload = DuckDbSettings().as_payload()
        assert payload["temp_directory"] == "~/duckdb_spill"
        assert payload["threads"] == 8

    def test_coerce_bool(self):
        assert coerce_bool("YES") is True
        assert coerce_bool("0") is False
        assert coerce_bool(True) is True


class TestDuckDbSshTransport:
    def test_build_transport_threads_shorthand(self):
        t = build_transport(transport="ssh+duckdb", ssh="mr3", threads=4)
        assert isinstance(t, DuckDbSshTransport)
        assert t.settings.threads == 4
        assert t.threads == 4

    def test_build_transport_settings_wins_over_threads(self):
        t = build_transport(
            transport="ssh+duckdb",
            ssh="mr3",
            threads=4,
            settings=DuckDbSettings(threads=16, memory_limit="32GB"),
        )
        assert t.settings.threads == 16
        assert t.settings.memory_limit == "32GB"

    def test_session_command_uploads_the_helper_then_serves(self):
        t = DuckDbSshTransport(ssh="mr3", wsl=False, python="/venv/bin/python")
        with patch("src.transport.subprocess.run") as run:
            run.return_value = MagicMock(returncode=0, stdout=b"", stderr=b"")
            cmd = t.session_command()
            uploaded = run.call_args.args[0]
        assert uploaded[:2] == ["ssh", "mr3"] and "tee" in uploaded
        assert cmd == [
            "ssh",
            "mr3",
            "/venv/bin/python",
            "/tmp/_orch_duckdb.py",
            "--serve",
        ]

    def test_execute_sends_settings_on_the_first_stdin_line(self):
        t = DuckDbSshTransport(
            ssh="mr3",
            wsl=False,
            settings=DuckDbSettings(memory_limit="4GB"),
        )
        with patch("src.transport.subprocess.run") as run:
            run.return_value = MagicMock(returncode=0, stdout=b"a\n1\n", stderr=b"")
            t.execute("SELECT 1")
            stdin = run.call_args.kwargs["input"].decode()
        header, sql = stdin.split("\n", 1)
        assert json.loads(header)["memory_limit"] == "4GB"
        assert sql == "SELECT 1"
