"""Library entry point for the orchestrator.

Two public functions for downstream callers:

  - `run_sql(sql, *, transport=..., ...) -> QueryResult` — execute one
    SQL statement and return the rows + column names. Picks a
    transport (direct SQLAlchemy, or ssh+wsl docker exec psql) and
    optionally writes a provenance line to
    `output/_ad_hoc/_provenance.jsonl`.
  - `run_manifest(manifest_path, ...) -> None` — load and execute a
    YAML manifest end-to-end. The CLI's default mode wraps this.

The transport layer (`src/transport.py`) is the part that varies. The
api owns the contract every caller sees: types, validation, logging.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from src.executor import Executor
from src.transport import (
    Transport, build_transport, _build_db_url as build_db_url,
)

log = logging.getLogger(__name__)


_FORBIDDEN_DQL_PATTERNS = re.compile(
    r'^\s*(CREATE|DROP|ALTER|TRUNCATE|INSERT|UPDATE|DELETE|MERGE|GRANT|REVOKE|EXEC|EXECUTE|CALL)\b',
    re.IGNORECASE | re.MULTILINE,
)


class DqlOnlyError(ValueError):
    """Raised by `run_sql(dql_only=True)` when the SQL contains a
    non-SELECT statement. Carries the matched keyword so callers can
    present a targeted error."""


@dataclass(frozen=True, slots=True)
class QueryResult:
    """Rows + columns from a single `run_sql` call.

    `rows` is a list of tuples in declaration order. `columns` matches
    `rows[i]` positionally. `elapsed_ms` measures the SQL call only,
    not engine setup or session creation. `sql_hash` is a short SHA-256
    prefix of the rendered SQL — matches the prefix written to the
    ad-hoc provenance log so callers can grep back."""
    columns: list[str]
    rows: list[tuple]
    elapsed_ms: int
    sql_hash: str
    transport: str

    @property
    def row_count(self) -> int:
        return len(self.rows)

    def to_csv(self, fh) -> None:
        """Write columns + rows to `fh` (anything with a .write taking str)."""
        writer = csv.writer(fh)
        writer.writerow(self.columns)
        for row in self.rows:
            writer.writerow(row)

    def to_csv_string(self) -> str:
        buf = io.StringIO()
        self.to_csv(buf)
        return buf.getvalue()


def _check_dql(sql: str) -> None:
    """Raise `DqlOnlyError` if `sql` contains a non-SELECT statement."""
    match = _FORBIDDEN_DQL_PATTERNS.search(sql)
    if match:
        keyword = match.group().strip()
        raise DqlOnlyError(
            f"Blocked: '{keyword}' statements are not allowed in DQL-only "
            f"mode. SELECT only."
        )


def _apply_limit(sql: str, limit: int, dialect: str) -> str:
    """Wrap `sql` in a row-limiter compatible with the dialect."""
    if 'oracle' in dialect:
        return f"SELECT * FROM ({sql}) WHERE ROWNUM <= {int(limit)}"
    return f"SELECT * FROM ({sql}) sub LIMIT {int(limit)}"


def _hash_sql(sql: str) -> str:
    return hashlib.sha256(sql.encode("utf-8")).hexdigest()[:16]


def _write_ad_hoc_provenance(record: dict, repo_root: Path | None = None) -> None:
    """Append one line to `output/_ad_hoc/_provenance.jsonl`.

    Mirrors the per-empresa provenance Reporter writes for manifest
    runs, but for ad-hoc `run_sql` calls outside any manifest. Failures
    here log + continue — observability must not break the query."""
    root = repo_root or Path.cwd()
    log_dir = root / "output" / "_ad_hoc"
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        with (log_dir / "_provenance.jsonl").open("a") as fh:
            fh.write(json.dumps(record) + "\n")
    except OSError as exc:
        log.warning("Failed to write ad-hoc provenance: %s", exc)


def _resolve_target(name: str) -> dict[str, Any]:
    """Look up a named DB target from `.env` via `load_targets`.

    Raises ValueError if the name isn't registered. Lazy import keeps
    the api module independent of config at import time."""
    from config.settings import load_targets
    targets = load_targets()
    if name not in targets:
        raise ValueError(
            f"Unknown DB target: {name!r}. Registered targets: "
            f"{sorted(targets.keys()) or 'none — define DB_TARGET_<NAME>_* in .env'}"
        )
    return dict(targets[name])


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def run_sql(
    sql: str,
    *,
    target: str | None = None,
    db_config: Mapping[str, Any] | None = None,
    transport: Transport | str | None = None,
    ssh: str | None = None,
    container: str | None = None,
    pg_user: str = "postgres",
    pg_database: str = "postgres",
    wsl: bool = True,
    sudo: bool = True,
    params: Mapping[str, Any] | None = None,
    limit: int | None = None,
    dql_only: bool = False,
    fetch_name: str = "ad_hoc",
    log_provenance: bool = True,
    repo_root: Path | None = None,
) -> QueryResult:
    """Execute one SQL statement and return its result.

    Two transport paths, picked by the `transport` argument or by what
    other kwargs are populated:

      - **Direct (default).** Pass `db_config` (dialect / user /
        password / host / port / database|service). The api connects
        via SQLAlchemy and runs the SQL.
      - **ssh+wsl.** Pass `transport='ssh+wsl'` plus `ssh` (e.g.
        ``adm@host``) and `container` (docker container name). The api
        runs `ssh ... wsl docker exec -i <container> psql --csv -f -`
        with the SQL on stdin and parses the CSV result.

    Args:
        sql: SQL string. For `direct` you can use `:name` bind params
            via `params`; the `ssh+wsl` transport does not support bind
            params (caller must render before calling).
        db_config: Required for the direct transport. Optional for
            ssh+wsl (some callers may want to pass it for context only;
            the transport itself uses its own dispatch).
        transport: ``'direct'`` (default), ``'ssh+wsl'``, or a
            pre-built `Transport` instance.
        ssh, container, pg_user, pg_database, wsl, sudo: SshWslTransport
            constructor arguments. Ignored when `transport` is a
            constructed `Transport` instance or `'direct'`.
        params: Optional bind parameters for the direct transport.
        limit: Optional row cap; wraps the SQL in a dialect-appropriate
            limiter before execution (direct only — ssh+wsl callers
            should add their own LIMIT clause).
        dql_only: If True, refuses any non-SELECT statement.
        fetch_name: Identifier written to the ad-hoc provenance log so
            the call is locatable by purpose, not just hash.
        log_provenance: If True (default), append a JSONL line to
            `output/_ad_hoc/_provenance.jsonl` for every call.
        repo_root: Where the `output/_ad_hoc/` directory lives. Default
            is `Path.cwd()`. Tests can override.

    Returns:
        `QueryResult` with columns + rows + elapsed_ms + sql_hash +
        transport name.

    Raises:
        DqlOnlyError: when `dql_only=True` and the SQL is not a SELECT.
        RuntimeError: when the underlying transport fails (typed
        transport-specific failures are translated here).
    """
    if dql_only:
        _check_dql(sql)

    # If `target` is supplied, every other transport-shaping kwarg is
    # filled from the named target's env entries. Callers say "run this
    # on MR3" — no secrets, no transport details, nothing to handle.
    if target is not None:
        if transport is not None or db_config is not None or ssh is not None:
            raise ValueError(
                "When `target` is given, do not also pass `transport`, "
                "`db_config`, or `ssh`/`container`/etc. — the target "
                "supplies them all."
            )
        cfg = _resolve_target(target)
        transport = cfg.get("transport", "direct")
        if transport == "direct":
            db_config = {
                "dialect": cfg["dialect"],
                "user": cfg["user"],
                "password": cfg["password"],
                "host": cfg["host"],
                "port": cfg["port"],
                "database": cfg.get("database"),
                "service": cfg.get("service"),
            }
        else:
            ssh = cfg["ssh"]
            container = cfg["container"]
            pg_user = cfg.get("pg_user", "postgres")
            pg_database = cfg.get("pg_database", "postgres")
            wsl = _coerce_bool(cfg.get("wsl", True))
            sudo = _coerce_bool(cfg.get("sudo", True))

    rendered = sql
    if limit:
        # We can only safely wrap when we know the dialect — that comes
        # from db_config. For ssh+wsl without db_config the caller must
        # add their own LIMIT.
        dialect = (db_config or {}).get('dialect', '')
        if dialect:
            rendered = _apply_limit(rendered, limit, dialect)

    sql_hash = _hash_sql(rendered)

    # Resolve the transport.
    if isinstance(transport, str) or transport is None:
        tp = build_transport(
            db_config=db_config,
            transport=transport,
            ssh=ssh, container=container,
            pg_user=pg_user, pg_database=pg_database,
            wsl=wsl, sudo=sudo,
        )
    else:
        tp = transport  # already a Transport instance

    start = time.monotonic()
    try:
        raw = tp.execute(rendered, params=params)
    except Exception as exc:
        elapsed_ms = int((time.monotonic() - start) * 1000)
        if log_provenance:
            _write_ad_hoc_provenance({
                "ts": datetime.now().isoformat(timespec="seconds"),
                "fetch": fetch_name,
                "transport": tp.name,
                "sql_hash": sql_hash,
                "elapsed_ms": elapsed_ms,
                "rows": 0,
                "status": "error",
                "error": str(exc)[:500],
            }, repo_root=repo_root)
        raise

    if log_provenance:
        _write_ad_hoc_provenance({
            "ts": datetime.now().isoformat(timespec="seconds"),
            "fetch": fetch_name,
            "transport": tp.name,
            "sql_hash": sql_hash,
            "elapsed_ms": raw.elapsed_ms,
            "rows": len(raw.rows),
            "status": "ok",
            "error": None,
        }, repo_root=repo_root)

    return QueryResult(
        columns=raw.columns,
        rows=raw.rows,
        elapsed_ms=raw.elapsed_ms,
        sql_hash=sql_hash,
        transport=tp.name,
    )


def run_manifest(
    manifest_path: Path | str,
    *,
    db_config: Mapping[str, Any],
    notifier_config: Mapping[str, Any] | None = None,
    dry_run: bool = False,
    force: bool = False,
    enable_all: bool = False,
) -> None:
    """Load and execute a YAML manifest end-to-end.

    Side effects:
        - Writes per-step plans + rendered SQL + summary.json + report.html
          under `reports/{timestamp}/`.
        - Disables completed steps in the manifest YAML in place
          (preserves comments).
        - Sends notifications to whatever channels are configured in
          `notifier_config`.
    """
    executor = Executor(
        manifest_path=manifest_path,
        db_config=db_config,
        notifier_config=notifier_config or {},
        dry_run=dry_run,
        force=force,
        enable_all=enable_all,
    )
    executor.run()


__all__ = [
    "DqlOnlyError",
    "QueryResult",
    "build_db_url",
    "run_sql",
    "run_manifest",
]
