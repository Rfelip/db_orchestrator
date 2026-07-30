"""Resume: run a manifest again without redoing what already finished.

Two mechanisms live here, and they answer different questions.

**The run ledger** answers *"resume where it broke"*. Every completed
step appends one line to `<root>/<run_id>/ledger.jsonl` — the same run
directory `plans.PlanStore` writes into, so one run id names one
directory holding both what ran and what it cost. `--resume <run_id>`
reads that ledger back and skips the steps it proves are done.

**The window selector** answers *"just run this part"*. `--from` /
`--until` cut a range out of the *expanded* plan by substring match on
step names, which is the ergonomics the old `scripts/run_pipeline.py`
had and the only one that ever worked on `foreach` output.

Both operate on expanded names (`qx_bkt03_year2019`), because they run
against `ManifestConfig.steps` — post-expansion — rather than against
the source YAML. That is the whole point: `YamlManager.disable_step`
matches names in the YAML file, and a `foreach` step's expansions are
not in the YAML file.

## What resume can and cannot prove

Skipping a step is only correct if that step's output is still on disk
and still valid. The ledger knows three things about a completed step:
it finished, what its SQL hashed to, and — *if the step declared
`produces:`* — where its output landed.

So resume proves what it can and is loud about the rest:

- **SQL changed** since the recorded run → the step is not skipped.
- **`produces:` declared and the file is gone** → not skipped.
- **No `produces:` declared** → nothing to check. The step is skipped
  and counted into `ResumeDecision.unverified`, which
  `format_resume_banner` prints as an explicit "resume is TRUSTING
  these" line. It is not silent.

The skip set is a *prefix*: the first step that fails any check is the
resume point, and everything from there runs. A later step being in the
ledger does not save it, because whatever invalidated its predecessor
probably invalidated it too.

## Relation to `carga_runs` (ADR-0004)

Not the same ledger and not a duplicate of it. `carga_runs` is a
*publication* record — one row per published dataset, in Postgres,
carrying git SHA and the Iceberg snapshot it points at. This one is a
*step execution* record — hundreds of lines per run, on local disk,
alive only until the run succeeds. They meet at the run id: the id this
module mints is the id a `carga_runs` row can reference.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

log = logging.getLogger(__name__)

LEDGER_FILENAME = "ledger.jsonl"


class LedgerNotFoundError(FileNotFoundError):
    """No ledger exists for the requested run id."""


class NoStepMatchedError(ValueError):
    """`--from` / `--until` matched no step in the expanded plan.

    Loud on purpose: silently running the whole 345-step pipeline
    because a substring had a typo is the expensive failure here.
    """


@dataclass(frozen=True, slots=True)
class LedgerEntry:
    """One step that finished, as recorded during the run."""

    run_id: str
    step: str
    source_sha: str
    """Fingerprint of the SQL that ran — file bytes plus params. Empty
    for steps with no `file` (a `python` step, say), which then cannot
    be invalidated by a source change."""
    produces: str | None
    """The step's rendered `produces:` path, or None when it declared
    none. None is the honest value: 'nothing to check', not 'checked
    and fine'."""
    seconds: float
    finished_at: str


@dataclass(frozen=True, slots=True)
class StepCheck:
    """Evidence the shell gathered about one planned step, for the core
    to judge against the ledger."""

    name: str
    source_sha: str
    produces: str | None
    output_present: bool | None
    """True / False when the step declared an output, None when it did
    not — a tri-state, because 'unknown' is not 'absent'."""


@dataclass(frozen=True, slots=True)
class ResumeDecision:
    """Which steps a resumed run may skip, and why it stopped there."""

    skip: tuple[str, ...]
    unverified: tuple[str, ...]
    """Skipped steps whose output nothing checked. Printed, never hidden."""
    start_at: str | None
    """First step that will run. None means the plan is already complete."""
    stop_reason: str | None


@dataclass(frozen=True, slots=True)
class ResumeOptions:
    """What the caller asked for, before any disk has been read."""

    run_id: str | None = None
    """Ledger run to resume from. `'last'` is resolved by the shell."""
    start: str | None = None
    until: str | None = None
    root: str = "reports/plans"
    record: bool = True
    """Write a ledger for this run. Off only for callers that want the
    old fire-and-forget behaviour."""


@dataclass(frozen=True, slots=True)
class ResumeRequest:
    """A `ResumeOptions` with its ledger already loaded — what the
    executor receives, so the executor never touches the ledger store."""

    prior: tuple[LedgerEntry, ...] = ()
    start: str | None = None
    until: str | None = None
    source_run_id: str | None = None


# ---- functional core ----------------------------------------------------


def select_window(
    names: Sequence[str], *, start: str | None = None, until: str | None = None
) -> tuple[int, int]:
    """Half-open index range of `names` selected by substring match.

    `start` picks the FIRST name containing it; `until` picks the LAST,
    inclusive — so `--until qx_bkt03` runs every `qx_bkt03_*` expansion
    rather than stopping inside the group. Either bound may be None.
    """
    first = 0 if start is None else _first_match(names, start, "--from")
    last = len(names) - 1 if until is None else _last_match(names, until, "--until")
    if first > last:
        raise NoStepMatchedError(
            f"--from {start!r} (step {names[first]!r}) comes after "
            f"--until {until!r} (step {names[last]!r}) — empty window"
        )
    return first, last + 1


def _first_match(names: Sequence[str], needle: str, flag: str) -> int:
    for index, name in enumerate(names):
        if needle in name:
            return index
    raise NoStepMatchedError(_no_match_message(names, needle, flag))


def _last_match(names: Sequence[str], needle: str, flag: str) -> int:
    for index in range(len(names) - 1, -1, -1):
        if needle in names[index]:
            return index
    raise NoStepMatchedError(_no_match_message(names, needle, flag))


def _no_match_message(names: Sequence[str], needle: str, flag: str) -> str:
    return (
        f"{flag} {needle!r} matched none of the {len(names)} steps in the "
        f"expanded plan. First few: {list(names[:5])}"
    )


def decide_resume(
    checks: Sequence[StepCheck], entries: Mapping[str, LedgerEntry]
) -> ResumeDecision:
    """The longest prefix of `checks` the ledger proves is still done.

    Stops at the first step that is missing from the ledger, whose SQL
    changed, or whose declared output vanished.
    """
    skip: list[str] = []
    unverified: list[str] = []
    for check in checks:
        reason = _blocks_skip(check, entries.get(check.name))
        if reason is not None:
            return ResumeDecision(tuple(skip), tuple(unverified), check.name, reason)
        skip.append(check.name)
        if check.output_present is None:
            unverified.append(check.name)
    return ResumeDecision(tuple(skip), tuple(unverified), None, None)


def _blocks_skip(check: StepCheck, entry: LedgerEntry | None) -> str | None:
    """Why this step cannot be skipped, or None if it can."""
    if entry is None:
        return "it is not recorded as completed in the ledger"
    if entry.source_sha != check.source_sha:
        return f"its SQL changed since run {entry.run_id}"
    if check.output_present is False:
        return f"its declared output is gone: {check.produces}"
    return None


def format_resume_banner(
    decision: ResumeDecision, *, total: int, run_id: str | None
) -> str:
    """The honesty notice, printed at the point of use.

    Resume trusts that skipped steps' outputs survived. Where that trust
    was checked it says so; where it could not be checked it says that
    too, with a count, rather than letting the run look verified.
    """
    verified = len(decision.skip) - len(decision.unverified)
    lines = [
        "--- Resume ---",
        f"ledger {run_id}: {len(decision.skip)} of {total} planned steps "
        f"proven complete.",
        f"  verified on disk : {verified} (declared `produces:` still present)",
        f"  NOT verified     : {len(decision.unverified)} (no `produces:` declared "
        f"— resume is TRUSTING",
        "                     their outputs are still on disk and still valid. "
        "Nothing checked that.)",
    ]
    if decision.start_at is None:
        lines.append("  Nothing left to run: every planned step is already complete.")
    else:
        lines.append(f"  Resuming at      : {decision.start_at}")
        lines.append(f"  Because          : {decision.stop_reason}")
    return "\n".join(lines)


def fingerprint(source: str, params: Mapping[str, Any]) -> str:
    """Stable short hash of a step's SQL text plus its params.

    Params are in it so two `foreach` expansions of the same file are
    distinguishable, and so a changed param invalidates the skip.
    """
    digest = hashlib.sha256()
    digest.update(source.encode("utf-8"))
    digest.update(json.dumps(dict(params), sort_keys=True, default=str).encode("utf-8"))
    return digest.hexdigest()[:16]


# ---- imperative shell ---------------------------------------------------


def source_fingerprint(file: str | None, params: Mapping[str, Any]) -> str:
    """`fingerprint` of a step's SQL file, or `''` when it has none.

    An unreadable file is not an error here: the fingerprint's job is to
    detect *change*, and a step whose file cannot be read will fail at
    execution anyway, with a better message than this function could
    give.
    """
    if not file:
        return ""
    path = Path(file)
    if not path.exists():
        return ""
    return fingerprint(path.read_text(encoding="utf-8", errors="replace"), params)


class RunLedger:
    """Appends one line per completed step, flushed as it goes.

    Flushed per line on purpose: the ledger is only useful if it
    survives the crash that made resume necessary.
    """

    def __init__(self, root: Path | str, run_id: str) -> None:
        self.run_id = run_id
        self.dir = Path(root) / run_id
        self.path = self.dir / LEDGER_FILENAME

    def inherit(self, entries: Iterable[LedgerEntry]) -> None:
        """Carry a previous run's completions into this run's ledger,
        keeping their original `run_id`.

        Without this, resuming a resumed run would forget the first
        run's work and redo it — chaining two failures would cost the
        whole pipeline again.
        """
        for entry in entries:
            self._append(entry)

    def record(
        self,
        *,
        step: str,
        source_sha: str,
        produces: str | None,
        seconds: float,
    ) -> LedgerEntry:
        entry = LedgerEntry(
            run_id=self.run_id,
            step=step,
            source_sha=source_sha,
            produces=produces,
            seconds=seconds,
            finished_at=datetime.now().isoformat(timespec="seconds"),
        )
        self._append(entry)
        return entry

    def _append(self, entry: LedgerEntry) -> None:
        self.dir.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(_entry_as_dict(entry)) + "\n")
            handle.flush()


def _entry_as_dict(entry: LedgerEntry) -> dict[str, Any]:
    return {
        "run_id": entry.run_id,
        "step": entry.step,
        "source_sha": entry.source_sha,
        "produces": entry.produces,
        "seconds": entry.seconds,
        "finished_at": entry.finished_at,
    }


def _entry_from_dict(raw: Mapping[str, Any]) -> LedgerEntry:
    return LedgerEntry(
        run_id=str(raw["run_id"]),
        step=str(raw["step"]),
        source_sha=str(raw.get("source_sha") or ""),
        produces=raw.get("produces"),
        seconds=float(raw.get("seconds") or 0.0),
        finished_at=str(raw.get("finished_at") or ""),
    )


def load_ledger(root: Path | str, run_id: str) -> tuple[LedgerEntry, ...]:
    """Read a run's ledger. Later lines win for a repeated step name, so
    a step re-run in a chained resume records its newest state."""
    path = Path(root) / run_id / LEDGER_FILENAME
    if not path.exists():
        raise LedgerNotFoundError(
            f"no ledger at {path}. Known runs under {root}: "
            f"{list_ledger_runs(root) or 'none'}"
        )
    by_name: dict[str, LedgerEntry] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            entry = _entry_from_dict(json.loads(line))
            by_name[entry.step] = entry
    return tuple(by_name.values())


def list_ledger_runs(root: Path | str) -> list[str]:
    """Run ids under `root` that have a ledger, oldest first. Ids are
    timestamps, so lexical order is chronological order."""
    base = Path(root)
    if not base.exists():
        return []
    return sorted(d.name for d in base.iterdir() if (d / LEDGER_FILENAME).exists())


def latest_ledger_run(root: Path | str) -> str:
    runs = list_ledger_runs(root)
    if not runs:
        raise LedgerNotFoundError(f"no run under {root} has a {LEDGER_FILENAME}")
    return runs[-1]


def load_resume_request(options: ResumeOptions) -> ResumeRequest:
    """Turn CLI-shaped options into the loaded request the executor takes.

    Resolves `'last'`, reads the ledger, and fails loudly when the named
    run has none — a resume that silently degrades into a full run is the
    thing this whole module exists to prevent.
    """
    if options.run_id is None:
        return ResumeRequest(start=options.start, until=options.until)
    run_id = (
        latest_ledger_run(options.root) if options.run_id == "last" else options.run_id
    )
    return ResumeRequest(
        prior=load_ledger(options.root, run_id),
        start=options.start,
        until=options.until,
        source_run_id=run_id,
    )


__all__ = [
    "LEDGER_FILENAME",
    "LedgerEntry",
    "LedgerNotFoundError",
    "NoStepMatchedError",
    "ResumeDecision",
    "ResumeOptions",
    "ResumeRequest",
    "RunLedger",
    "StepCheck",
    "decide_resume",
    "fingerprint",
    "format_resume_banner",
    "latest_ledger_run",
    "list_ledger_runs",
    "load_ledger",
    "load_resume_request",
    "select_window",
    "source_fingerprint",
]
