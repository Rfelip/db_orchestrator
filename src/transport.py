"""Transports — how a SQL statement actually reaches a database.

Two implementations live here:

  - `DirectTransport` connects to a database via SQLAlchemy. Use when
    the caller has a network-reachable host:port (local Postgres,
    Oracle, or pgduckdb container with an exposed port).

  - `SshWslTransport` runs SQL on a remote machine via `ssh + wsl
    docker exec psql`. Use when the database lives inside a container
    on a remote host and only SSH is available — for example, MR3's
    pgduckdb (the container binds to 5434 inside WSL; Tailscale
    terminates at the Windows host, so direct connections are
    refused).

Both transports return a `RawResult` with columns + rows + elapsed_ms.
The `run_sql` entry point in `src.api` picks one and types the output.
"""
from __future__ import annotations

import csv
import io
import logging
import shlex
import subprocess
import time
from dataclasses import dataclass
from typing import Any, Mapping, Protocol

from src.database import DatabaseManager

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RawResult:
    """The shape every transport returns. `run_sql` wraps this into the
    public `QueryResult` after computing a SQL hash and writing
    provenance."""
    columns: list[str]
    rows: list[tuple]
    elapsed_ms: int


class Transport(Protocol):
    """Common shape for any way of getting SQL onto a database."""

    name: str
    """Short identifier used in provenance lines (e.g. 'direct',
    'ssh+wsl+pgduckdb')."""

    def execute(self, sql: str, params: Mapping[str, Any] | None = None) -> RawResult: ...


class DirectTransport:
    """SQLAlchemy connection to a network-reachable host:port.

    The DB URL is constructed from `db_config` exactly the way
    `Executor` does — same dialect/user/password/host/port/database
    fields. Use this for local DBs, exposed-port containers, or
    SSH-tunnelled connections (pre-tunneled to localhost)."""

    name = "direct"

    def __init__(self, db_config: Mapping[str, Any]) -> None:
        self._db_config = dict(db_config)

    def execute(self, sql: str,
                 params: Mapping[str, Any] | None = None) -> RawResult:
        url = _build_db_url(self._db_config)
        db = DatabaseManager(url)
        session = db.get_session()
        try:
            log.info("DirectTransport executing (%d chars)...", len(sql))
            start = time.monotonic()
            result = db.execute_query(sql, params=dict(params) if params else None,
                                        session=session)
            columns = list(result.keys())
            rows = [tuple(r) for r in result.fetchall()]
            elapsed_ms = int((time.monotonic() - start) * 1000)
            log.info("DirectTransport returned %d rows (%dms).",
                      len(rows), elapsed_ms)
            return RawResult(columns=columns, rows=rows, elapsed_ms=elapsed_ms)
        finally:
            session.close()
            db.close()


class SshWslTransport:
    """Run SQL on a remote machine via `ssh + wsl docker exec psql`.

    Targets the LabMA pattern where pgduckdb (or similar) runs inside a
    Docker container on an MR3-like Windows-with-WSL host. The
    transport ships SQL over stdin to `psql --csv` running inside the
    container; CSV output comes back over stdout.

    Args:
        ssh: ssh target, e.g. ``adm@100.95.184.17``.
        container: docker container name, e.g. ``pgduckdb``.
        pg_user: postgres user inside the container.
        pg_database: postgres database inside the container.
        wsl: prepend ``wsl`` (i.e. host is Windows running WSL). Default
            True since that's the only deployment we have so far.
        sudo: prepend ``sudo`` to docker (rootful docker installs).
        ssh_options: extra ssh options as a list of `-o KEY=VALUE`
            strings. Empty list by default.
    """

    name = "ssh+wsl"

    def __init__(self, *, ssh: str, container: str,
                 pg_user: str = "postgres",
                 pg_database: str = "postgres",
                 wsl: bool = True,
                 sudo: bool = True,
                 ssh_options: list[str] | None = None) -> None:
        self.ssh = ssh
        self.container = container
        self.pg_user = pg_user
        self.pg_database = pg_database
        self.wsl = wsl
        self.sudo = sudo
        self.ssh_options = list(ssh_options or [])

    def execute(self, sql: str,
                 params: Mapping[str, Any] | None = None) -> RawResult:
        if params:
            # SQLAlchemy-style :name binding doesn't survive a raw psql
            # call. Callers that need parameter substitution should
            # render the SQL before invoking the transport (the
            # orchestrator's existing `render_template` is one option).
            raise NotImplementedError(
                "SshWslTransport does not support :name bind params. "
                "Render the SQL before calling execute()."
            )
        cmd = self._build_command()
        log.info("SshWslTransport executing on %s/%s (%d chars)...",
                  self.ssh, self.container, len(sql))
        start = time.monotonic()
        proc = subprocess.run(
            cmd,
            input=sql.encode("utf-8"),
            capture_output=True,
            check=False,
        )
        elapsed_ms = int((time.monotonic() - start) * 1000)
        if proc.returncode != 0:
            stderr = proc.stderr.decode("utf-8", errors="replace")
            raise RuntimeError(
                f"ssh+wsl psql failed (rc={proc.returncode}): "
                f"{stderr[:500]}"
            )
        body = proc.stdout.decode("utf-8", errors="replace")
        columns, rows = _parse_psql_csv(body)
        log.info("SshWslTransport returned %d rows (%dms).", len(rows), elapsed_ms)
        return RawResult(columns=columns, rows=rows, elapsed_ms=elapsed_ms)

    def _build_command(self) -> list[str]:
        ssh_part = ["ssh"] + self.ssh_options + [self.ssh]
        wrapper = ["wsl"] if self.wsl else []
        docker_part = (["sudo"] if self.sudo else []) + [
            "docker", "exec", "-i", self.container,
            "psql", "-U", self.pg_user, "-d", self.pg_database,
            "-v", "ON_ERROR_STOP=1", "--csv", "-f", "-",
        ]
        return ssh_part + wrapper + docker_part


def build_transport(
    db_config: Mapping[str, Any] | None = None,
    *,
    transport: str | None = None,
    ssh: str | None = None,
    container: str | None = None,
    pg_user: str = "postgres",
    pg_database: str = "postgres",
    wsl: bool = True,
    sudo: bool = True,
) -> Transport:
    """Return a transport based on the supplied arguments.

    `transport` is a string selector. If omitted, `direct` is the
    default. The dispatch lives here (rather than in run_sql) so other
    callers can construct transports for testing or for non-run_sql
    workflows."""
    kind = (transport or "direct").lower()
    if kind == "direct":
        if db_config is None:
            raise ValueError("DirectTransport requires db_config")
        return DirectTransport(db_config)
    if kind in ("ssh+wsl", "ssh_wsl"):
        if not ssh or not container:
            raise ValueError(
                "SshWslTransport requires `ssh` (host target) and "
                "`container` (docker container name)."
            )
        return SshWslTransport(
            ssh=ssh, container=container,
            pg_user=pg_user, pg_database=pg_database,
            wsl=wsl, sudo=sudo,
        )
    raise ValueError(f"Unknown transport: {kind!r}")


def _build_db_url(db_config: Mapping[str, Any]) -> str:
    """Build a SQLAlchemy URL. Lives here so transport doesn't import
    api (api imports transport). The api re-exports this for the CLI."""
    dialect = db_config['dialect']
    user = db_config['user']
    password = db_config['password']
    host = db_config['host']
    port = db_config['port']
    if 'oracle' in dialect:
        return f"{dialect}://{user}:{password}@{host}:{port}/{db_config['service']}"
    return f"{dialect}://{user}:{password}@{host}:{port}/{db_config['database']}"


def _parse_psql_csv(text: str) -> tuple[list[str], list[tuple]]:
    """Parse `psql --csv` output. First non-empty line is the header.

    Returns ([], []) for empty stdout (e.g. DDL with no result set,
    though the ssh transport is mostly used for SELECTs)."""
    body = text.strip()
    if not body:
        return [], []
    reader = csv.reader(io.StringIO(body))
    rows = list(reader)
    if not rows:
        return [], []
    columns = rows[0]
    data = [tuple(r) for r in rows[1:]]
    return columns, data
