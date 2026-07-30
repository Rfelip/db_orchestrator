"""A persistent DuckDB session — one process, many statements.

A DuckDB transport's `execute()` opens a fresh helper and a fresh DuckDB
per call. That is correct for an ad-hoc query and wrong for a pipeline:
the tábua plan holds a per-bucket `TEMP TABLE _p1b_{bkt}` and an
`exposure_lookup` across steps, and a connection-per-statement transport
loses both. Running those steps through `execute()` would break
semantics, not merely performance.

This module keeps the helper alive for the span of a run, over ssh or
locally — the protocol does not know which:

    with open_transport_session(transport, settings, plans=store) as session:
        session.run("CREATE TEMP TABLE t AS SELECT 1")
        session.run("COPY (SELECT * FROM t) TO 'x.parquet'")

The return contract is a sum type, not an empty SELECT result. DuckDB
reports DDL/DML/COPY as a one-column `Count`/`Success` frame; presenting
that as `RawResult(columns=['count'], rows=[(2,)])` would be pretending
a `COPY` returned data. `run()` returns `Completed` for those and
`RawResult` only when there was a genuine result set.
"""

from __future__ import annotations

import itertools
import json
import logging
import re
import subprocess
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from typing import IO, Any, Iterator

from src.plans import PlanStore
from src.transport import (
    DuckDbSettings,
    DuckDbTransport,
    RawResult,
    write_helper,
)

log = logging.getLogger(__name__)

_CLOSE_TIMEOUT_S = 15
"""How long a shutdown request has to drain before the process is killed.
Generous because the last statement may still be flushing parquet."""

_LEADING_COMMENTS = re.compile(r"^(?:\s|--[^\n]*\n|/\*.*?\*/)*", re.S)

_QUERY_KEYWORDS = frozenset(
    {
        "SELECT",
        "WITH",
        "FROM",
        "VALUES",
        "TABLE",
        "DESCRIBE",
        "SHOW",
        "EXPLAIN",
        "PIVOT",
    }
)
"""Statements that legitimately return a result set. Everything else that
comes back as a lone `Count`/`Success` column is DuckDB reporting
completion, not data."""


@dataclass(frozen=True, slots=True)
class Completed:
    """A statement that finished without producing a result set.

    `statement` is the leading keyword (`COPY`, `CREATE`, `INSERT`, …).
    `row_count` is what DuckDB reported in its `Count` frame — rows
    written for `COPY`, rows affected for DML — or None when it reported
    only `Success`."""

    statement: str
    row_count: int | None
    elapsed_ms: int


StatementResult = RawResult | Completed
"""What one statement on a session yields. Callers that only care about
success can ignore the branch; callers that read rows must handle it."""


class SessionError(RuntimeError):
    """The session failed. Either the statement raised inside DuckDB, or
    the process died and took the session's TEMP state with it — those
    are different recoveries, so the message says which."""


def leading_keyword(sql: str) -> str:
    """First SQL word, uppercased, ignoring leading comments.

    Used to tell a query from a statement without asking the database,
    which matters because DuckDB answers both with a frame."""
    body = _LEADING_COMMENTS.sub("", sql, count=1).lstrip("( \t\n")
    match = re.match(r"[A-Za-z_]+", body)
    return match.group(0).upper() if match else ""


def classify(
    sql: str, columns: list[str], rows: list[tuple], elapsed_ms: int
) -> StatementResult:
    """Turn the helper's raw answer into an honest result.

    A one-column `count`/`success` frame from a non-query statement is
    DuckDB's completion report. A `SELECT` is trusted to have returned
    data even when it happens to name its column `count`."""
    keyword = leading_keyword(sql)
    reported = [c.lower() for c in columns]
    if keyword in _QUERY_KEYWORDS or reported not in (["count"], ["success"]):
        return RawResult(columns=columns, rows=rows, elapsed_ms=elapsed_ms)
    count = rows[0][0] if rows and reported == ["count"] else None
    return Completed(
        statement=keyword,
        row_count=int(count) if isinstance(count, (int, float)) else None,
        elapsed_ms=elapsed_ms,
    )


class DuckDbSession:
    """One live DuckDB helper process, driven over a JSON-line protocol.

    Every `run()` lands on the same connection, so TEMP TABLEs, macros
    and settings from an earlier call are visible to a later one. Not
    thread-safe: one pipe, one conversation.
    """

    def __init__(
        self,
        proc: subprocess.Popen,
        *,
        errors: IO[bytes] | None = None,
        plans: PlanStore | None = None,
    ) -> None:
        self._proc = proc
        self._errors = errors
        self._plans = plans
        self._ids = itertools.count(1)
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def start(self, settings: DuckDbSettings) -> str:
        """Ship the settings and wait for the helper's ready line.
        Returns the helper's DuckDB version."""
        self._send(settings.as_payload())
        hello = self._receive()
        if not hello.get("ready"):
            raise SessionError(f"helper did not report ready: {hello!r}")
        version = str(hello.get("duckdb", "?"))
        log.info("DuckDB session up (duckdb %s)", version)
        return version

    def run(self, sql: str, *, step: str | None = None) -> StatementResult:
        """Execute one statement on the session's connection."""
        if self._closed:
            raise SessionError("session is closed")
        request_id = next(self._ids)
        self._send({"id": request_id, "sql": sql, "step": step})
        response = self._receive()
        if not response.get("ok"):
            raise SessionError(
                f"step {step or request_id}: {response.get('error', 'unknown error')}"
            )
        elapsed_ms = int(response.get("elapsed_ms", 0))
        columns = list(response.get("columns") or [])
        rows = [tuple(r) for r in response.get("rows") or []]
        self._record_plan(
            step or f"stmt_{request_id}", elapsed_ms, response.get("profile")
        )
        return classify(sql, columns, rows, elapsed_ms)

    def close(self) -> None:
        """Shut the helper process down. Idempotent, and safe to call
        from a `finally` after any failure — a session that is already
        dead just gets reaped."""
        if self._closed:
            return
        self._closed = True
        try:
            self._request_shutdown()
        finally:
            self._reap()

    # ---- pipe mechanics -------------------------------------------------

    def _send(self, payload: dict[str, Any]) -> None:
        stdin = self._proc.stdin
        if stdin is None:
            raise SessionError("session process has no stdin")
        try:
            stdin.write(json.dumps(payload) + "\n")
            stdin.flush()
        except (BrokenPipeError, ValueError) as exc:
            raise SessionError(
                f"session died before the request landed: {exc}\n{self._stderr()}"
            ) from exc

    def _receive(self) -> dict[str, Any]:
        stdout = self._proc.stdout
        if stdout is None:
            raise SessionError("session process has no stdout")
        line = stdout.readline()
        if not line:
            raise SessionError(
                f"session ended without answering (rc={self._proc.poll()})\n"
                f"{self._stderr()}"
            )
        return json.loads(line)

    def _record_plan(self, step: str, elapsed_ms: int, profile: Any) -> None:
        if self._plans is None or not isinstance(profile, dict):
            return
        self._plans.record(step=step, seconds=elapsed_ms / 1000.0, profile=profile)

    def _request_shutdown(self) -> None:
        stdin = self._proc.stdin
        if stdin is None or self._proc.poll() is not None:
            return
        try:
            stdin.write(json.dumps({"cmd": "shutdown"}) + "\n")
            stdin.flush()
            stdin.close()
        except (BrokenPipeError, OSError, ValueError):
            # The helper is already gone; `_reap` still has to collect it.
            pass

    def _reap(self) -> None:
        try:
            self._proc.wait(timeout=_CLOSE_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            log.warning(
                "DuckDB session did not exit in %ss — killing", _CLOSE_TIMEOUT_S
            )
            self._proc.kill()
            self._proc.wait()

    def _stderr(self) -> str:
        """Tail of the helper's stderr. Kept in a file rather than a pipe
        so a chatty helper can never deadlock the response read."""
        if self._errors is None:
            return ""
        try:
            self._errors.seek(0)
            return self._errors.read().decode("utf-8", errors="replace")[-2000:]
        except (OSError, ValueError):
            return ""


@contextmanager
def open_duckdb_session(
    command: list[str],
    settings: DuckDbSettings,
    *,
    plans: PlanStore | None = None,
) -> Iterator[DuckDbSession]:
    """Start `command` as a persistent DuckDB helper and yield a session.

    The process is torn down on the way out whatever happened inside the
    block — a raised step must not leave a DuckDB holding 16GB on a
    shared machine.
    """
    errors = tempfile.TemporaryFile()
    proc = subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=errors,
        text=True,
        encoding="utf-8",
    )
    session = DuckDbSession(proc, errors=errors, plans=plans)
    try:
        session.start(settings)
        yield session
    finally:
        session.close()
        errors.close()


@contextmanager
def open_transport_session(
    transport: DuckDbTransport,
    settings: DuckDbSettings | None = None,
    *,
    plans: PlanStore | None = None,
) -> Iterator[DuckDbSession]:
    """`open_duckdb_session` over a DuckDB transport: puts the helper
    where that transport runs it, then keeps one DuckDB alive for the
    caller's block.

    Takes the transport rather than an argv so ssh and local reach this
    the same way — the session semantics below this line are identical,
    and having two entry points would invite them to stop being."""
    chosen = settings or transport.settings
    with open_duckdb_session(
        transport.session_command(), chosen, plans=plans
    ) as session:
        yield session


__all__ = [
    "Completed",
    "DuckDbSession",
    "SessionError",
    "StatementResult",
    "classify",
    "leading_keyword",
    "open_duckdb_session",
    "open_transport_session",
    "write_helper",
]
