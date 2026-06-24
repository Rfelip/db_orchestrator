"""Long-lived batch server for the orchestrator — connection REUSE.

The default CLI path (`main.py --sql-file X --target T --output O`) spawns
a fresh process per fetch, and that process builds a brand-new transport
(a fresh SQLAlchemy engine + DB connection for `direct`, or a fresh ssh
invocation for the ssh transports) every single time. A report runs ~100
fetches, so ~100 fresh connections get opened and torn down. Through the
comp20 LAN portproxy that intermittently exhausts ephemeral ports / stalls
connection setup, costing many wasted minutes per run.

This module is the fix: a server loop (`serve()`, wired to `main.py
--serve`) that reads one request per line from stdin, runs each on a
**persistent per-target transport**, writes the CSV, and prints one status
line to stdout. The client spawns ONE such server per report run and sends
each fetch as a request, so the whole run reuses a small set of connections
(one engine per distinct `--target`) instead of one per fetch.

Wire protocol (line-delimited JSON, utf-8, one request/response per line):

    request   {"id": <int>, "sql_file": "<path>", "target": "<NAME>",
               "output": "<path>", "limit": <int|null>}
    response  {"id": <int>, "ok": <bool>, "rows": <int>, "error": "<str|null>"}

A ``{"id": ..., "cmd": "shutdown"}`` request makes the loop return cleanly.

The response carries the orchestrator's raw error string verbatim in
`error` (truncated), so the client's existing transient/missing-source
classification (`_TRANSIENT_FETCH_SIGNS` / `_MISSING_SOURCE_SIGNS`) keeps
working unchanged: the server surfaces the SAME error signatures the
subprocess path printed to stderr.  Classification is done entirely on the
client by substring matching — the server does not duplicate that logic.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any, Mapping, TextIO

from src.api import (
    DqlOnlyError, QueryResult, _apply_limit, _resolve_target, _coerce_bool,
)
from src.transport import (
    Transport, build_transport, DirectTransport,
)
from src.database import DatabaseManager


class PersistentDirectTransport:
    """A `direct` transport that keeps ONE engine/connection-pool alive
    across `execute()` calls.

    The stock `DirectTransport.execute()` builds a fresh `DatabaseManager`
    (new SQLAlchemy engine, new connection) on every call — exactly the
    churn this whole module exists to kill. This subclass builds the engine
    once and reuses it. `pool_pre_ping=True` (set inside `DatabaseManager`)
    means a connection dropped between fetches is transparently re-opened on
    the next checkout, and a genuinely dead connection surfaces as the same
    OperationalError string the per-subprocess path raised — so the client's
    transient-retry classification still fires.
    """

    name = "direct"

    def __init__(self, db_config: Mapping[str, Any]) -> None:
        from src.transport import _build_db_url
        self._url = _build_db_url(db_config)
        self._db: DatabaseManager | None = None

    def _manager(self) -> DatabaseManager:
        if self._db is None:
            self._db = DatabaseManager(self._url)
        return self._db

    def execute(self, sql: str,
                params: Mapping[str, Any] | None = None):
        from src.transport import RawResult
        db = self._manager()
        session = db.get_session()
        try:
            start = time.monotonic()
            result = db.execute_query(
                sql, params=dict(params) if params else None, session=session
            )
            columns = list(result.keys())
            rows = [tuple(r) for r in result.fetchall()]
            elapsed_ms = int((time.monotonic() - start) * 1000)
            return RawResult(columns=columns, rows=rows, elapsed_ms=elapsed_ms)
        finally:
            session.close()

    def close(self) -> None:
        if self._db is not None:
            self._db.close()
            self._db = None


class TransportPool:
    """One persistent transport per named target, built lazily and reused.

    `direct` targets get a `PersistentDirectTransport` (engine kept alive —
    the whole point). The ssh transports (`ssh+wsl`, `ssh+duckdb`,
    `ssh+clickhouse`) are already long-lived objects whose only per-call
    cost is one ssh invocation; caching the instance still saves repeated
    target resolution + (for duckdb) re-uploading the helper, and keeps the
    one-server-per-run shape uniform across both data planes.
    """

    def __init__(self, *, target_resolver=_resolve_target) -> None:
        self._resolver = target_resolver
        self._pool: dict[str, Transport] = {}

    def get(self, target: str) -> Transport:
        tp = self._pool.get(target)
        if tp is not None:
            return tp
        cfg = dict(self._resolver(target))
        kind = cfg.get("transport", "direct")
        if kind == "direct":
            db_config = {
                "dialect": cfg["dialect"], "user": cfg["user"],
                "password": cfg["password"], "host": cfg["host"],
                "port": cfg["port"], "database": cfg.get("database"),
                "service": cfg.get("service"),
            }
            tp = PersistentDirectTransport(db_config)
        elif kind in ("ssh+duckdb", "ssh_duckdb", "duckdb+ssh"):
            tp = build_transport(
                transport=kind, ssh=cfg["ssh"],
                wsl=_coerce_bool(cfg.get("wsl", True)),
                helper_path=cfg.get("helper_path", "/tmp/_orch_duckdb.py"),
                threads=int(cfg.get("threads", 8)),
            )
        elif kind in ("ssh+clickhouse", "ssh_clickhouse", "clickhouse+ssh"):
            tp = build_transport(
                transport=kind, ssh=cfg["ssh"], container=cfg["container"],
                ch_database=cfg.get("ch_database"),
                wsl=_coerce_bool(cfg.get("wsl", True)),
                sudo=_coerce_bool(cfg.get("sudo", True)),
            )
        else:  # ssh+wsl (psql) and aliases
            tp = build_transport(
                transport=kind, ssh=cfg["ssh"], container=cfg["container"],
                pg_user=cfg.get("pg_user", "postgres"),
                pg_database=cfg.get("pg_database", "postgres"),
                wsl=_coerce_bool(cfg.get("wsl", True)),
                sudo=_coerce_bool(cfg.get("sudo", True)),
            )
        self._pool[target] = tp
        return tp

    def _dialect_for(self, target: str) -> str:
        """Dialect string for the target (for `--limit` wrapping), or ''."""
        try:
            cfg = self._resolver(target)
        except Exception:
            return ""
        return str(cfg.get("dialect", "") or "")

    def close(self) -> None:
        for tp in self._pool.values():
            closer = getattr(tp, "close", None)
            if callable(closer):
                try:
                    closer()
                except Exception:
                    pass
        self._pool.clear()


def _handle_request(req: dict, pool: TransportPool) -> dict:
    """Run one fetch request against the pooled transport, write its CSV,
    and return the response dict. Never raises — every failure becomes an
    `error` string in the response so the loop keeps serving."""
    rid = req.get("id")
    sql_file = req.get("sql_file")
    target = req.get("target")
    output = req.get("output")
    limit = req.get("limit")
    try:
        if not sql_file or not target or not output:
            raise ValueError(
                "request needs 'sql_file', 'target', and 'output'"
            )
        sql_path = Path(sql_file)
        if not sql_path.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_path}")
        sql = sql_path.read_text(encoding="utf-8").strip()
        if sql.endswith(";"):
            sql = sql[:-1].strip()

        tp = pool.get(target)
        rendered = sql
        if limit:
            dialect = pool._dialect_for(target)
            if dialect:
                rendered = _apply_limit(rendered, int(limit), dialect)

        raw = tp.execute(rendered)
        result = QueryResult(
            columns=raw.columns, rows=raw.rows, elapsed_ms=raw.elapsed_ms,
            sql_hash="", transport=tp.name,
        )
        out_path = Path(output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", newline="", encoding="utf-8") as fh:
            result.to_csv(fh)
        return {"id": rid, "ok": True, "rows": result.row_count, "error": None}
    except Exception as exc:  # noqa: BLE001 — surface every failure as text
        # Surface the SAME string shape the subprocess path printed to
        # stderr ("ERROR: <msg>"), so the client's substring classification
        # (_TRANSIENT_FETCH_SIGNS / _MISSING_SOURCE_SIGNS) keeps matching.
        msg = f"ERROR: {exc}"
        return {"id": rid, "ok": False, "rows": 0, "error": msg}


def serve(stdin: TextIO | None = None, stdout: TextIO | None = None,
          *, pool: TransportPool | None = None) -> int:
    """Read line-delimited JSON requests from `stdin`, run each on a
    persistent per-target transport, write one JSON response line per
    request to `stdout`. Returns 0 on clean EOF / shutdown.

    Pipes are text-mode utf-8 with an explicit flush after every response,
    so framing works identically on Windows and Linux (no platform tricks).
    """
    stdin = stdin or sys.stdin
    stdout = stdout or sys.stdout
    own_pool = pool is None
    pool = pool or TransportPool()
    # Announce readiness so the client can confirm the server came up
    # before sending the first fetch.
    stdout.write(json.dumps({"ready": True}) + "\n")
    stdout.flush()
    try:
        for line in stdin:
            line = line.strip()
            if not line:
                continue
            try:
                req = json.loads(line)
            except json.JSONDecodeError as exc:
                stdout.write(json.dumps(
                    {"id": None, "ok": False, "rows": 0,
                     "error": f"ERROR: bad request JSON: {exc}"}) + "\n")
                stdout.flush()
                continue
            if req.get("cmd") == "shutdown":
                break
            resp = _handle_request(req, pool)
            stdout.write(json.dumps(resp) + "\n")
            stdout.flush()
        return 0
    finally:
        if own_pool:
            pool.close()
