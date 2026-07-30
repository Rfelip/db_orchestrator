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
import json
import logging
import re
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

    def execute(
        self, sql: str, params: Mapping[str, Any] | None = None
    ) -> RawResult: ...


class DirectTransport:
    """SQLAlchemy connection to a network-reachable host:port.

    The DB URL is constructed from `db_config` exactly the way
    `Executor` does — same dialect/user/password/host/port/database
    fields. Use this for local DBs, exposed-port containers, or
    SSH-tunnelled connections (pre-tunneled to localhost)."""

    name = "direct"

    def __init__(self, db_config: Mapping[str, Any]) -> None:
        self._db_config = dict(db_config)

    def execute(self, sql: str, params: Mapping[str, Any] | None = None) -> RawResult:
        url = _build_db_url(self._db_config)
        db = DatabaseManager(url)
        session = db.get_session()
        try:
            log.info("DirectTransport executing (%d chars)...", len(sql))
            start = time.monotonic()
            result = db.execute_query(
                sql, params=dict(params) if params else None, session=session
            )
            columns = list(result.keys())
            rows = [tuple(r) for r in result.fetchall()]
            elapsed_ms = int((time.monotonic() - start) * 1000)
            log.info("DirectTransport returned %d rows (%dms).", len(rows), elapsed_ms)
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
        ssh: ssh target, e.g. ``user@host``.
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

    def __init__(
        self,
        *,
        ssh: str,
        container: str,
        pg_user: str = "postgres",
        pg_database: str = "postgres",
        wsl: bool = True,
        sudo: bool = True,
        ssh_options: list[str] | None = None,
    ) -> None:
        self.ssh = ssh
        self.container = container
        self.pg_user = pg_user
        self.pg_database = pg_database
        self.wsl = wsl
        self.sudo = sudo
        self.ssh_options = list(ssh_options or [])

    def execute(self, sql: str, params: Mapping[str, Any] | None = None) -> RawResult:
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
        log.info(
            "SshWslTransport executing on %s/%s (%d chars)...",
            self.ssh,
            self.container,
            len(sql),
        )
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
                f"ssh+wsl psql failed (rc={proc.returncode}): {stderr[:500]}"
            )
        body = proc.stdout.decode("utf-8", errors="replace")
        columns, rows = _parse_psql_csv(body)
        log.info("SshWslTransport returned %d rows (%dms).", len(rows), elapsed_ms)
        return RawResult(columns=columns, rows=rows, elapsed_ms=elapsed_ms)

    def _build_command(self) -> list[str]:
        ssh_part = ["ssh"] + self.ssh_options + [self.ssh]
        wrapper = ["wsl"] if self.wsl else []
        docker_part = (["sudo"] if self.sudo else []) + [
            "docker",
            "exec",
            "-i",
            self.container,
            "psql",
            "-U",
            self.pg_user,
            "-d",
            self.pg_database,
            "-v",
            "ON_ERROR_STOP=1",
            "--csv",
            "-f",
            "-",
        ]
        return ssh_part + wrapper + docker_part


_SIZE = re.compile(r"^\d+(\.\d+)?\s*(B|K|M|G|T|KB|MB|GB|TB|KIB|MIB|GIB|TIB)$", re.I)


def coerce_bool(value: Any) -> bool:
    """`.env` values arrive as strings; this is the one place that
    decides what counts as true."""
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True, slots=True)
class DuckDbSettings:
    """DuckDB knobs that belong to the run, not to the SQL.

    These used to be set by whichever script happened to open the
    connection, which meant every consumer re-derived them and none of
    them were reviewable. Defaults are the values measured on MR3 —
    8 threads at 16GB is ~2GB/thread — and changing them without
    measuring is how a shared machine starts thrashing.

    `temp_directory` defaults under `$HOME`, which on MR3 is NVMe.
    Pointing it at `/mnt/BANCOS` would put spill on a rotational RAID1,
    and spill is write-heavy — the worst case for that device.

    `max_temp_directory_size` is mandatory and bounded: the box is
    shared, and an unbounded spill fills the root filesystem and takes
    down somebody else's session.
    """

    memory_limit: str = "16GB"
    threads: int = 8
    temp_directory: str = "~/duckdb_spill"
    max_temp_directory_size: str = "512GB"
    preserve_insertion_order: bool = False
    profile: bool = False

    def __post_init__(self) -> None:
        if not _SIZE.match(self.memory_limit.strip()):
            raise ValueError(
                f"memory_limit must be a DuckDB size like '16GB', got "
                f"{self.memory_limit!r}"
            )
        if self.threads < 1:
            raise ValueError(f"threads must be >= 1, got {self.threads}")
        if not self.temp_directory.strip():
            raise ValueError(
                "temp_directory must name a directory — spill has to land "
                "somewhere chosen, not wherever DuckDB defaults to."
            )
        if not _SIZE.match(self.max_temp_directory_size.strip()):
            raise ValueError(
                f"max_temp_directory_size must be a bounded DuckDB size like "
                f"'512GB', got {self.max_temp_directory_size!r} — the host is "
                f"shared, so an unbounded spill is not an option."
            )

    @classmethod
    def from_mapping(cls, cfg: Mapping[str, Any]) -> "DuckDbSettings":
        """Build from a `DB_TARGET_<NAME>_*` entry. Absent keys keep the
        documented default; they are not sentinels."""
        defaults = cls()
        return cls(
            memory_limit=str(cfg.get("memory_limit") or defaults.memory_limit),
            threads=int(cfg.get("threads") or defaults.threads),
            temp_directory=str(cfg.get("temp_directory") or defaults.temp_directory),
            max_temp_directory_size=str(
                cfg.get("max_temp_directory_size") or defaults.max_temp_directory_size
            ),
            preserve_insertion_order=coerce_bool(
                cfg.get("preserve_insertion_order", defaults.preserve_insertion_order)
            ),
            profile=coerce_bool(cfg.get("profile", defaults.profile)),
        )

    def as_payload(self) -> dict[str, Any]:
        """The JSON the remote helper reads off its first stdin line.

        `temp_directory` ships unexpanded on purpose: `~` means the
        remote user's home, and only the remote side knows where that
        is."""
        return {
            "memory_limit": self.memory_limit,
            "threads": self.threads,
            "temp_directory": self.temp_directory,
            "max_temp_directory_size": self.max_temp_directory_size,
            "preserve_insertion_order": self.preserve_insertion_order,
            "profile": self.profile,
        }


DUCKDB_HELPER = r'''"""Remote program for the orchestrator's DuckDB transports.

Reads a settings JSON object off the first stdin line, opens ONE DuckDB
connection, and then either (a) runs the rest of stdin as a single
statement and writes CSV, or (b) with `--serve`, loops reading
line-delimited JSON requests and answering them on that same connection
— which is what keeps TEMP TABLEs and macros alive across steps.

Uploaded by `DuckDbSshTransport._ensure_helper`; never imported locally.
"""
import csv
import json
import os
import sys
import time

import duckdb

PROFILE_FILE = "_orch_profile.json"


def _lit(value):
    return str(value).replace("'", "''")


def apply_settings(con, cfg):
    """Apply the run's knobs. Returns the profile path, or None when
    profiling is off."""
    tmp = os.path.expanduser(cfg["temp_directory"])
    os.makedirs(tmp, exist_ok=True)
    con.execute("SET memory_limit='%s'" % _lit(cfg["memory_limit"]))
    con.execute("SET threads=%d" % int(cfg["threads"]))
    con.execute("SET temp_directory='%s'" % _lit(tmp))
    con.execute(
        "SET max_temp_directory_size='%s'" % _lit(cfg["max_temp_directory_size"])
    )
    con.execute(
        "SET preserve_insertion_order=%s"
        % ("true" if cfg["preserve_insertion_order"] else "false")
    )
    if not cfg.get("profile"):
        return None
    path = os.path.join(tmp, PROFILE_FILE)
    con.execute("SET enable_profiling='json'")
    con.execute("SET profiling_output='%s'" % _lit(path))
    return path


def take_profile(path):
    """Read and DELETE the profile DuckDB wrote for the statement that
    just ran. Deleting is what makes a missing file mean 'no profile for
    this statement' instead of 'the profile of some earlier one'."""
    if not path:
        return None
    try:
        with open(path) as handle:
            profile = json.load(handle)
    except (OSError, ValueError):
        return None
    try:
        os.remove(path)
    except OSError:
        pass
    return profile


def run(con, sql, profile_path):
    start = time.perf_counter()
    cur = con.execute(sql)
    description = cur.description
    columns = [d[0].lower() for d in description] if description else []
    rows = cur.fetchall() if description else []
    return {
        "columns": columns,
        "rows": rows,
        "elapsed_ms": int((time.perf_counter() - start) * 1000),
        "profile": take_profile(profile_path),
    }


def serve(con, profile_path, stdin, stdout):
    stdout.write(json.dumps({"ready": True, "duckdb": duckdb.__version__}) + "\n")
    stdout.flush()
    while True:
        line = stdin.readline()
        if not line:
            return
        line = line.strip()
        if not line:
            continue
        request = json.loads(line)
        if request.get("cmd") == "shutdown":
            return
        try:
            response = run(con, request["sql"], profile_path)
            response.update({"id": request.get("id"), "ok": True, "error": None})
        except Exception as exc:
            # A failing statement must not kill the session: the caller
            # decides whether to abandon the run, and it needs the error
            # text to decide. Catch-all because DuckDB raises a dozen
            # unrelated types and every one of them means the same thing
            # here.
            response = {
                "id": request.get("id"),
                "ok": False,
                "error": "%s: %s" % (type(exc).__name__, exc),
            }
        stdout.write(json.dumps(response, default=str) + "\n")
        stdout.flush()


def main(argv):
    cfg = json.loads(sys.stdin.readline())
    con = duckdb.connect()
    profile_path = apply_settings(con, cfg)
    if "--serve" in argv:
        serve(con, profile_path, sys.stdin, sys.stdout)
        return 0
    out = run(con, sys.stdin.read().strip().rstrip(";"), profile_path)
    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow(out["columns"])
    for row in out["rows"]:
        writer.writerow(row)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
'''
"""Source of the remote helper. Kept as a string because it has to run on
the *other* machine, where this package does not exist."""


class DuckDbSshTransport:
    """Run DuckDB SQL on a remote host via `ssh [wsl] python3 <helper>`.

    The companion of `SshWslTransport`: where that one reaches a
    containerised Postgres, this one reaches **DuckDB running directly on
    the host**, so queries that read on-disk parquet trees
    (`read_parquet([...])`, `glob()`, `filename=true`, `strftime`,
    `hash`) work — the full DuckDB dialect, which pgduckdb-in-Postgres
    does not expose.

    The helper script is shipped to `helper_path` on first use (idempotent,
    once per process) and invoked with the settings on the first stdin
    line and the SQL after it. No bind params: callers render the SQL
    before calling.

    `execute()` opens a fresh remote DuckDB per call, which is right for
    ad-hoc queries and wrong for a pipeline. For a run whose steps share
    TEMP TABLEs, use `src.duckdb_session.open_ssh_session(transport)`.

    Args:
        ssh: ssh target, e.g. ``user@host``.
        helper_path: where the helper lands on the host.
        wsl: prepend ``wsl`` (host is Windows running WSL). Default True.
        settings: DuckDB knobs for the connection.
    """

    name = "ssh+duckdb"

    def __init__(
        self,
        *,
        ssh: str,
        helper_path: str = "/tmp/_orch_duckdb.py",
        wsl: bool = True,
        settings: DuckDbSettings | None = None,
        python: str = "python3",
        ssh_options: list[str] | None = None,
    ) -> None:
        self.ssh = ssh
        self.helper_path = helper_path
        self.wsl = wsl
        self.settings = settings or DuckDbSettings()
        # The helper needs a `duckdb` import, which a host's *system* python
        # often lacks while a project venv on the same host has it. Naming the
        # interpreter is the difference between "this host can't run DuckDB"
        # and "point at the venv that already can".
        self.python = python
        self.ssh_options = list(ssh_options or [])
        self._helper_synced = False

    @property
    def threads(self) -> int:
        return self.settings.threads

    def _ssh_prefix(self) -> list[str]:
        return ["ssh"] + self.ssh_options + [self.ssh] + (["wsl"] if self.wsl else [])

    def session_command(self) -> list[str]:
        """argv that starts a persistent remote DuckDB. Uploads the
        helper first — the process cannot start without it."""
        self._ensure_helper()
        return self._ssh_prefix() + [self.python, self.helper_path, "--serve"]

    def _ensure_helper(self) -> None:
        if self._helper_synced:
            return
        body = DUCKDB_HELPER.encode("utf-8")
        proc = subprocess.run(
            self._ssh_prefix() + ["tee", self.helper_path],
            input=body,
            capture_output=True,
            check=False,
        )
        if proc.returncode != 0:
            raise RuntimeError(
                f"failed to upload duckdb helper to {self.ssh}:{self.helper_path} "
                f"(rc={proc.returncode}): {proc.stderr.decode('utf-8', 'replace')[:300]}"
            )
        self._helper_synced = True

    def execute(self, sql: str, params: Mapping[str, Any] | None = None) -> RawResult:
        if params:
            raise NotImplementedError(
                "DuckDbSshTransport does not support :name bind params. "
                "Render the SQL before calling execute()."
            )
        self._ensure_helper()
        cmd = self._ssh_prefix() + [self.python, self.helper_path]
        log.info("DuckDbSshTransport executing on %s (%d chars)...", self.ssh, len(sql))
        stdin = json.dumps(self.settings.as_payload()) + "\n" + sql
        start = time.monotonic()
        proc = subprocess.run(
            cmd,
            input=stdin.encode("utf-8"),
            capture_output=True,
            check=False,
        )
        elapsed_ms = int((time.monotonic() - start) * 1000)
        if proc.returncode != 0:
            stderr = proc.stderr.decode("utf-8", errors="replace")
            raise RuntimeError(
                f"ssh+duckdb failed (rc={proc.returncode}): {stderr[:500]}"
            )
        body = proc.stdout.decode("utf-8", errors="replace")
        columns, rows = _parse_psql_csv(body)
        log.info("DuckDbSshTransport returned %d rows (%dms).", len(rows), elapsed_ms)
        return RawResult(columns=columns, rows=rows, elapsed_ms=elapsed_ms)


class ClickHouseSshTransport:
    """Run ClickHouse SQL on a remote machine via `ssh + wsl docker exec
    clickhouse-client`.

    The ClickHouse sibling of `SshWslTransport`: same `ssh + [wsl]
    [sudo] docker exec -i <container>` envelope, but the in-container
    program is `clickhouse-client` instead of `psql`. SQL ships over
    stdin; results come back as CSV with a header row
    (`--format CSVWithNames`), so the existing `_parse_psql_csv` helper
    parses them unchanged.

    Performance settings that matter for the LabMA biometric tables
    (large GROUP BY / JOIN over hundreds of millions of CNIS rows) are
    exposed as constructor params and passed as `--<setting>=<value>`
    flags. Defaults are sized for the MR3 rig (~27 GB host RAM).

    Args:
        ssh: ssh target, e.g. ``user@host``.
        container: docker container name, e.g. ``clickhouse-tabua``.
        ch_database: ClickHouse database to connect to (``-d <db>``).
            Optional — leave None to qualify tables in the SQL instead
            (``SELECT … FROM mydb.mytable``) or rely on ``USE``.
        wsl: prepend ``wsl`` (i.e. host is Windows running WSL). Default
            True since that's the only deployment we have so far.
        sudo: prepend ``sudo`` to docker (rootful docker installs).
        max_threads: ClickHouse ``max_threads`` setting.
        max_bytes_before_external_group_by: spill threshold for GROUP BY.
        join_algorithm: ClickHouse ``join_algorithm`` (e.g.
            ``grace_hash`` to spill large joins to disk).
        max_memory_usage: per-query memory ceiling in bytes.
        ssh_options: extra ssh options as a list of `-o KEY=VALUE`
            strings. Empty list by default.
    """

    name = "ssh+clickhouse"

    def __init__(
        self,
        *,
        ssh: str,
        container: str,
        ch_database: str | None = None,
        wsl: bool = True,
        sudo: bool = True,
        max_threads: int = 4,
        max_bytes_before_external_group_by: int = 2_000_000_000,
        join_algorithm: str = "grace_hash",
        max_memory_usage: int = 22_000_000_000,
        ssh_options: list[str] | None = None,
    ) -> None:
        self.ssh = ssh
        self.container = container
        self.ch_database = ch_database
        self.wsl = wsl
        self.sudo = sudo
        self.max_threads = max_threads
        self.max_bytes_before_external_group_by = max_bytes_before_external_group_by
        self.join_algorithm = join_algorithm
        self.max_memory_usage = max_memory_usage
        self.ssh_options = list(ssh_options or [])

    def execute(self, sql: str, params: Mapping[str, Any] | None = None) -> RawResult:
        if params:
            raise NotImplementedError(
                "ClickHouseSshTransport does not support :name bind params. "
                "Render the SQL before calling execute()."
            )
        cmd = self._build_command()
        log.info(
            "ClickHouseSshTransport executing on %s/%s (%d chars)...",
            self.ssh,
            self.container,
            len(sql),
        )
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
                f"ssh+clickhouse clickhouse-client failed "
                f"(rc={proc.returncode}): {stderr[:500]}"
            )
        body = proc.stdout.decode("utf-8", errors="replace")
        columns, rows = _parse_psql_csv(body)
        log.info(
            "ClickHouseSshTransport returned %d rows (%dms).", len(rows), elapsed_ms
        )
        return RawResult(columns=columns, rows=rows, elapsed_ms=elapsed_ms)

    def _build_command(self) -> list[str]:
        ssh_part = ["ssh"] + self.ssh_options + [self.ssh]
        wrapper = ["wsl"] if self.wsl else []
        client = [
            "clickhouse-client",
            "--format",
            "CSVWithNames",
            "--multiquery",
            f"--max_threads={self.max_threads}",
            "--max_bytes_before_external_group_by="
            f"{self.max_bytes_before_external_group_by}",
            f"--join_algorithm={self.join_algorithm}",
            f"--max_memory_usage={self.max_memory_usage}",
        ]
        if self.ch_database:
            client += ["-d", self.ch_database]
        docker_part = (
            (["sudo"] if self.sudo else [])
            + [
                "docker",
                "exec",
                "-i",
                self.container,
            ]
            + client
        )
        return ssh_part + wrapper + docker_part


def build_transport(
    db_config: Mapping[str, Any] | None = None,
    *,
    transport: str | None = None,
    ssh: str | None = None,
    container: str | None = None,
    pg_user: str = "postgres",
    pg_database: str = "postgres",
    ch_database: str | None = None,
    wsl: bool = True,
    sudo: bool = True,
    helper_path: str = "/tmp/_orch_duckdb.py",
    threads: int = 8,
    settings: DuckDbSettings | None = None,
    python: str = "python3",
) -> Transport:
    """Return a transport based on the supplied arguments.

    `transport` is a string selector. If omitted, `direct` is the
    default. The dispatch lives here (rather than in run_sql) so other
    callers can construct transports for testing or for non-run_sql
    workflows.

    `threads` is the one-knob shorthand kept for existing callers;
    `settings` carries the full `DuckDbSettings` and wins when both are
    given."""
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
            ssh=ssh,
            container=container,
            pg_user=pg_user,
            pg_database=pg_database,
            wsl=wsl,
            sudo=sudo,
        )
    if kind in ("ssh+duckdb", "ssh_duckdb", "duckdb+ssh"):
        if not ssh:
            raise ValueError("DuckDbSshTransport requires `ssh` (host target).")
        return DuckDbSshTransport(
            ssh=ssh,
            helper_path=helper_path,
            wsl=wsl,
            settings=settings or DuckDbSettings(threads=threads),
            python=python,
        )
    if kind in ("ssh+clickhouse", "ssh_clickhouse", "clickhouse+ssh"):
        if not ssh or not container:
            raise ValueError(
                "ClickHouseSshTransport requires `ssh` (host target) and "
                "`container` (docker container name)."
            )
        return ClickHouseSshTransport(
            ssh=ssh,
            container=container,
            ch_database=ch_database,
            wsl=wsl,
            sudo=sudo,
        )
    raise ValueError(f"Unknown transport: {kind!r}")


def _build_db_url(db_config: Mapping[str, Any]) -> str:
    """Build a SQLAlchemy URL. Lives here so transport doesn't import
    api (api imports transport). The api re-exports this for the CLI."""
    dialect = db_config["dialect"]
    user = db_config["user"]
    password = db_config["password"]
    host = db_config["host"]
    port = db_config["port"]
    if "oracle" in dialect:
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
