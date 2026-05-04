"""Library entry point for the orchestrator.

Two public functions for downstream callers:

  - `run_sql(sql, *, db_config, ...) -> QueryResult` — execute one SQL
    statement and return the rows + column names. The CLI's `--query`
    mode is a thin wrapper around this; any other Python caller can
    import it directly.
  - `run_manifest(manifest_path, ...) -> None` — load and execute a
    YAML manifest end-to-end. The CLI's default mode wraps this.

Why a separate api module: keeps the orchestration vision honest. The
project now exposes a single SQL protocol that is callable as a
library, not just from the command line. CLI presentation
(CSV-writing, status output, daemonisation) lives in `main.py`; the
work itself lives here.
"""
from __future__ import annotations

import csv
import io
import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from src.database import DatabaseManager
from src.executor import Executor

log = logging.getLogger(__name__)

# Statements that mutate data or schema. Used by `run_sql(dql_only=True)`
# and the CLI's --query mode to refuse anything that isn't a SELECT.
_FORBIDDEN_DQL_PATTERNS = re.compile(
    r'^\s*(CREATE|DROP|ALTER|TRUNCATE|INSERT|UPDATE|DELETE|MERGE|GRANT|REVOKE|EXEC|EXECUTE|CALL)\b',
    re.IGNORECASE | re.MULTILINE,
)


class DqlOnlyError(ValueError):
    """Raised by run_sql(dql_only=True) when the SQL contains a non-SELECT
    statement. Carries the matched keyword so callers can present a
    targeted error."""


@dataclass(frozen=True, slots=True)
class QueryResult:
    """Rows + columns from a single `run_sql` call.

    `rows` is a list of tuples in declaration order. `columns` matches
    `rows[i]` positionally. `elapsed_ms` measures the SQL call only,
    not engine setup or session creation.
    """
    columns: list[str]
    rows: list[tuple]
    elapsed_ms: int

    @property
    def row_count(self) -> int:
        return len(self.rows)

    def to_csv(self, fh) -> None:
        """Write columns + rows to `fh` (any object with .write that
        takes str). Provided for the CLI path; downstream callers can
        write their own formatter."""
        writer = csv.writer(fh)
        writer.writerow(self.columns)
        for row in self.rows:
            writer.writerow(row)

    def to_csv_string(self) -> str:
        buf = io.StringIO()
        self.to_csv(buf)
        return buf.getvalue()


def build_db_url(db_config: Mapping[str, Any]) -> str:
    """Build a SQLAlchemy URL from the orchestrator's db_config dict.

    Public so the CLI's --query path doesn't have to reimplement
    URL construction. Same behaviour as `Executor`'s URL builder."""
    dialect = db_config['dialect']
    user = db_config['user']
    password = db_config['password']
    host = db_config['host']
    port = db_config['port']
    if 'oracle' in dialect:
        return f"{dialect}://{user}:{password}@{host}:{port}/{db_config['service']}"
    return f"{dialect}://{user}:{password}@{host}:{port}/{db_config['database']}"


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
    """Wrap `sql` in a row-limiter compatible with the dialect.
    Oracle pre-12c uses ROWNUM; everything else uses LIMIT."""
    if 'oracle' in dialect:
        return f"SELECT * FROM ({sql}) WHERE ROWNUM <= {int(limit)}"
    return f"SELECT * FROM ({sql}) sub LIMIT {int(limit)}"


def run_sql(
    sql: str,
    *,
    db_config: Mapping[str, Any],
    params: Mapping[str, Any] | None = None,
    limit: int | None = None,
    dql_only: bool = False,
) -> QueryResult:
    """Execute one SQL statement and return its result.

    Args:
        sql: The SQL string. Bind parameters with `:name` and pass them
            via `params`; the runtime substitution lives in SQLAlchemy.
        db_config: A dict shaped like `settings.load_settings()['db']`.
            Must include dialect / user / password / host / port plus
            either service (Oracle) or database (Postgres).
        params: Optional bind parameters for the statement.
        limit: Optional row cap; wraps `sql` in a dialect-appropriate
            limiter before execution.
        dql_only: If True, refuses any non-SELECT statement. Used by
            the CLI's --query mode to prevent ad-hoc DDL/DML.

    Returns:
        `QueryResult` with columns + rows + elapsed_ms.

    Raises:
        DqlOnlyError: when `dql_only=True` and the SQL is not a SELECT.
        sqlalchemy.exc.SQLAlchemyError: on database-level failures.
    """
    if dql_only:
        _check_dql(sql)

    rendered = sql
    if limit:
        rendered = _apply_limit(rendered, limit, db_config.get('dialect', ''))

    db = DatabaseManager(build_db_url(db_config))
    session = db.get_session()
    try:
        log.info("Executing query (%d chars)...", len(rendered))
        start = time.monotonic()
        result = db.execute_query(rendered, params=dict(params) if params else None,
                                    session=session)
        columns = list(result.keys())
        rows = [tuple(r) for r in result.fetchall()]
        elapsed_ms = int((time.monotonic() - start) * 1000)
        log.info("Query returned %d rows, %d columns (%dms).",
                  len(rows), len(columns), elapsed_ms)
        return QueryResult(columns=columns, rows=rows, elapsed_ms=elapsed_ms)
    finally:
        session.close()
        db.close()


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

    Args:
        manifest_path: Path to the manifest.yaml.
        db_config: Database connection dict (see `run_sql`).
        notifier_config: Notification channel config — Discord
            webhook, Telegram bot token + chat id, user_name. Empty
            dict / None means no notifications.
        dry_run: If True, print the plan and exit without executing.
        force: If True, skip the interactive confirmation prompt.
        enable_all: If True, run all steps regardless of the manifest's
            `enabled: false` flags.

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
