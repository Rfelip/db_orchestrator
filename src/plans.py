"""Execution plans: capture them per run, then make them readable.

DuckDB can report a plan two ways. `EXPLAIN ANALYZE <sql>` needs the
statement rewritten, which the pipeline cannot afford — its steps are
`COPY … TO '…parquet'`, and a wrapper changes both what runs and what
comes back. `SET enable_profiling='json'` + `SET profiling_output=<path>`
captures the same tree for **every** statement with no rewriting at all,
including DDL and COPY. That is what this module consumes; the capture
itself happens in the remote helper (`src.transport.DUCKDB_HELPER`).

Plans are stored per run and per step:

    <root>/<run_id>/index.jsonl        one line per statement
    <root>/<run_id>/001_<step>.json    the raw DuckDB profile

`index.jsonl` is the part meant to be read: keyed by run id and step
name, so the same step is comparable across runs. `summarize_run` folds
it into slowest-steps and dominant-operators, which is what
`main.py --plans` prints.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

log = logging.getLogger(__name__)

_UNSAFE = re.compile(r"[^A-Za-z0-9._-]+")
_TOP_OPERATORS = 6
"""How many operators a step keeps in the index. The raw profile keeps
every one; this is the part meant to be skimmed."""


@dataclass(frozen=True, slots=True)
class OperatorCost:
    """One node of a DuckDB profile tree."""

    name: str
    seconds: float
    cardinality: int


@dataclass(frozen=True, slots=True)
class StepPlan:
    """What one statement cost, plus where its raw profile landed."""

    seq: int
    step: str
    seconds: float
    rows: int
    operators: tuple[OperatorCost, ...]
    profile_file: str


@dataclass(frozen=True, slots=True)
class RunSummary:
    """A whole run, folded down to the two questions worth asking:
    which steps were slow, and which operators ate the time."""

    run_id: str
    steps: tuple[StepPlan, ...]
    slowest: tuple[StepPlan, ...]
    operator_totals: tuple[OperatorCost, ...]

    @property
    def total_seconds(self) -> float:
        return sum(s.seconds for s in self.steps)


# ---- functional core ----------------------------------------------------


def parse_profile(profile: Mapping[str, Any]) -> tuple[OperatorCost, ...]:
    """Flatten a DuckDB JSON profile into its operator costs.

    The root node carries the query's own totals rather than an operator,
    so only its descendants are collected."""
    found: list[OperatorCost] = []

    def walk(node: Mapping[str, Any]) -> None:
        name = node.get("operator_name")
        if name:
            found.append(
                OperatorCost(
                    name=str(name),
                    seconds=float(node.get("operator_timing") or 0.0),
                    cardinality=int(node.get("operator_cardinality") or 0),
                )
            )
        for child in node.get("children") or []:
            if isinstance(child, Mapping):
                walk(child)

    walk(profile)
    return tuple(sorted(found, key=lambda op: -op.seconds))


def profile_rows(profile: Mapping[str, Any]) -> int:
    return int(profile.get("rows_returned") or 0)


def profile_seconds(profile: Mapping[str, Any]) -> float:
    """DuckDB's own latency for the statement, in seconds."""
    return float(profile.get("latency") or 0.0)


def merge_operators(steps: Iterable[StepPlan]) -> tuple[OperatorCost, ...]:
    """Total time and cardinality per operator name across a run.

    This is the "where is CPU actually going" view: one HASH_JOIN taking
    4s is noise, ninety of them taking 4s each is the run."""
    totals: dict[str, list[float]] = {}
    for step in steps:
        for op in step.operators:
            bucket = totals.setdefault(op.name, [0.0, 0.0])
            bucket[0] += op.seconds
            bucket[1] += op.cardinality
    return tuple(
        sorted(
            (
                OperatorCost(name=name, seconds=secs, cardinality=int(card))
                for name, (secs, card) in totals.items()
            ),
            key=lambda op: -op.seconds,
        )
    )


def summarize(run_id: str, steps: Iterable[StepPlan], *, top: int = 10) -> RunSummary:
    ordered = tuple(steps)
    slowest = tuple(sorted(ordered, key=lambda s: -s.seconds)[:top])
    return RunSummary(
        run_id=run_id,
        steps=ordered,
        slowest=slowest,
        operator_totals=merge_operators(ordered),
    )


def format_summary(summary: RunSummary, *, top: int = 10) -> str:
    """Plain-text report. Terminal-friendly on purpose: a directory of
    JSON nobody opens is not a deliverable."""
    lines = [
        f"run {summary.run_id} — {len(summary.steps)} statements, "
        f"{summary.total_seconds:.1f}s total",
        "",
        "slowest steps",
    ]
    for step in summary.slowest[:top]:
        share = (
            100 * step.seconds / summary.total_seconds if summary.total_seconds else 0
        )
        lines.append(
            f"  {step.seconds:8.2f}s  {share:5.1f}%  {step.step}  ({step.rows:,} rows)"
        )
        for op in step.operators[:3]:
            lines.append(
                f"            {op.seconds:8.2f}s  {op.name} ({op.cardinality:,})"
            )
    lines += ["", "dominant operators (whole run)"]
    for op in summary.operator_totals[:top]:
        share = 100 * op.seconds / summary.total_seconds if summary.total_seconds else 0
        lines.append(
            f"  {op.seconds:8.2f}s  {share:5.1f}%  {op.name}  ({op.cardinality:,} rows)"
        )
    return "\n".join(lines)


def new_run_id(now: datetime | None = None) -> str:
    """Sortable, comparable across runs, and unique enough for a machine
    that runs the pipeline a handful of times a day."""
    return (now or datetime.now()).strftime("%Y%m%dT%H%M%S")


def _safe_name(step: str) -> str:
    return _UNSAFE.sub("_", step).strip("_")[:80] or "step"


# ---- imperative shell ---------------------------------------------------


class PlanStore:
    """Writes one run's plans to disk, one file per statement.

    The store owns the run id, so every plan it writes is keyed to the
    same run and steps can be diffed against the same step in another
    run.
    """

    def __init__(self, root: Path | str, run_id: str | None = None) -> None:
        self.run_id = run_id or new_run_id()
        self.dir = Path(root) / self.run_id
        self._seq = 0

    def record(
        self, *, step: str, seconds: float, profile: Mapping[str, Any]
    ) -> StepPlan:
        """Persist one statement's profile and index entry.

        `seconds` is the caller's wall clock; DuckDB's own latency is
        preferred when the profile carries it, because it excludes the
        transport round trip."""
        self._seq += 1
        self.dir.mkdir(parents=True, exist_ok=True)
        filename = f"{self._seq:03d}_{_safe_name(step)}.json"
        (self.dir / filename).write_text(json.dumps(profile), encoding="utf-8")
        plan = StepPlan(
            seq=self._seq,
            step=step,
            seconds=profile_seconds(profile) or seconds,
            rows=profile_rows(profile),
            operators=parse_profile(profile)[:_TOP_OPERATORS],
            profile_file=filename,
        )
        with (self.dir / "index.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(_plan_as_dict(plan)) + "\n")
        return plan

    def summary(self) -> RunSummary:
        return load_run(self.dir.parent, self.run_id)


def _plan_as_dict(plan: StepPlan) -> dict[str, Any]:
    return {
        "seq": plan.seq,
        "step": plan.step,
        "seconds": plan.seconds,
        "rows": plan.rows,
        "profile_file": plan.profile_file,
        "operators": [
            {"name": op.name, "seconds": op.seconds, "cardinality": op.cardinality}
            for op in plan.operators
        ],
    }


def _plan_from_dict(raw: Mapping[str, Any]) -> StepPlan:
    return StepPlan(
        seq=int(raw["seq"]),
        step=str(raw["step"]),
        seconds=float(raw["seconds"]),
        rows=int(raw.get("rows") or 0),
        operators=tuple(
            OperatorCost(
                name=str(op["name"]),
                seconds=float(op["seconds"]),
                cardinality=int(op["cardinality"]),
            )
            for op in raw.get("operators") or []
        ),
        profile_file=str(raw.get("profile_file") or ""),
    )


def load_run(root: Path | str, run_id: str) -> RunSummary:
    """Read a stored run's index back into a `RunSummary`."""
    index = Path(root) / run_id / "index.jsonl"
    if not index.exists():
        raise FileNotFoundError(f"no plan index at {index}")
    steps = [
        _plan_from_dict(json.loads(line))
        for line in index.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    return summarize(run_id, steps)


def list_runs(root: Path | str) -> list[str]:
    """Run ids under `root`, newest last. Ids are timestamps, so
    lexical order is chronological order."""
    base = Path(root)
    if not base.exists():
        return []
    return sorted(d.name for d in base.iterdir() if (d / "index.jsonl").exists())


__all__ = [
    "OperatorCost",
    "PlanStore",
    "RunSummary",
    "StepPlan",
    "format_summary",
    "list_runs",
    "load_run",
    "merge_operators",
    "new_run_id",
    "parse_profile",
    "summarize",
]
