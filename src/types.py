"""Typed contracts for the orchestrator's data shapes.

Replaces the previous "untyped dict plumbing" — every YAML step is now
validated into a `Step` frozen dataclass at manifest load. Unknown keys
fail loudly at parse time instead of silently shadowing typos. Optional
fields keep their `None` default so existing manifests stay valid.

`Step.from_dict(...)` is the single ingress point. Internal code reads
fields via attribute access (`step.name`, `step.transaction_group`)
rather than `.get()`-style dict access.

Repetition is *declared* with `foreach:` and expanded by
`ManifestConfig.from_dict`, so everything downstream of manifest load
sees concrete steps only and needs no loop concept of its own.
"""

from __future__ import annotations

import itertools
import logging
from dataclasses import dataclass, field, replace
from typing import Any, Mapping, NewType

log = logging.getLogger(__name__)


StepName = NewType("StepName", str)
"""A step's stable identifier within a manifest. Used to disable the
step on completion and to label it in reports."""

GroupId = NewType("GroupId", str)
"""Label for a transaction or joined group. Consecutive steps sharing a
GroupId are coalesced at execution time."""


# The full set of step kinds the executor knows how to dispatch. Listed
# here so unknown values fail at construction rather than at the inline
# `if step_type == ...` chain in executor._run_steps.
_VALID_TYPES = frozenset({"sql", "plsql", "psql", "bulk_insert", "python", "manifest"})

# Recognised keys on a step. Anything outside this set is a typo and
# raises at parse time.
_RECOGNISED_KEYS = frozenset(
    {
        "name",
        "type",
        "enabled",
        "description",
        "file",
        "sql_id",
        "params",
        "transaction_group",
        "joined_group",
        "joined_glue",
        "cleanup_target",
        "cleanup_mode",
        "profile",
        "output_file",
        "notify",
        "ping_on_end",
        "ping_on_error",
        "foreach",
    }
)


@dataclass(frozen=True, slots=True)
class Step:
    """One unit of work in the manifest.

    Required: `name`, `type`. Everything else is optional and defaults
    to None or the field's documented default. The dataclass is frozen
    so step values cannot be mutated mid-run; callers that need a
    derived step build a new one via `dataclasses.replace`.
    """

    name: StepName
    type: str

    # Filter/control
    enabled: bool = True
    description: str | None = None

    # Source artifacts. `file` is the absolute or relative .sql path;
    # `sql_id` is a name registered in `sql-catalog.yaml` that the
    # manifest loader resolves to a file. Exactly one of the two may be
    # set per step.
    file: str | None = None
    sql_id: str | None = None
    params: Mapping[str, Any] = field(default_factory=dict)

    # Grouping
    transaction_group: GroupId | None = None
    joined_group: GroupId | None = None
    joined_glue: str | None = None  # 'statement' (default) or 'raw'

    # Cleanup pre-flight
    cleanup_target: str | None = None
    cleanup_mode: str = "drop"  # 'drop' or 'truncate'

    # psql-specific
    profile: bool = False

    # sql/plsql output
    output_file: str | None = None

    # Notification controls
    notify: bool = False
    ping_on_end: str | None = None
    ping_on_error: str | None = None

    # Declared repetition. Each key is a param name, each value the list
    # it ranges over; several keys mean their cross product. Empty on
    # every step the executor ever sees — `ManifestConfig.from_dict`
    # expands it away at load.
    foreach: Mapping[str, list[Any]] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "Step":
        """Build a Step from a YAML-loaded dict. Raises ValueError on
        unknown keys, missing required fields, or invalid type values."""
        if not isinstance(raw, Mapping):
            raise ValueError(f"step must be a mapping, got {type(raw).__name__}")
        unknown = set(raw.keys()) - _RECOGNISED_KEYS
        if unknown:
            raise ValueError(
                f"step '{raw.get('name', '<unnamed>')}': unknown keys "
                f"{sorted(unknown)}. Recognised keys: {sorted(_RECOGNISED_KEYS)}"
            )
        if "name" not in raw:
            raise ValueError(f"step is missing required 'name' field: {raw}")
        if "type" not in raw:
            raise ValueError(f"step '{raw['name']}' is missing required 'type' field")
        if raw["type"] not in _VALID_TYPES:
            raise ValueError(
                f"step '{raw['name']}': type '{raw['type']}' is not one of "
                f"{sorted(_VALID_TYPES)}"
            )
        if raw.get("file") and raw.get("sql_id"):
            raise ValueError(
                f"step '{raw['name']}': set either 'file' or 'sql_id', not both."
            )
        joined_glue = raw.get("joined_glue")
        if joined_glue is not None and joined_glue not in ("statement", "raw"):
            raise ValueError(
                f"step '{raw['name']}': joined_glue must be 'statement' or "
                f"'raw', got '{joined_glue}'"
            )
        cleanup_mode = raw.get("cleanup_mode", "drop")
        if cleanup_mode not in ("drop", "truncate"):
            raise ValueError(
                f"step '{raw['name']}': cleanup_mode must be 'drop' or "
                f"'truncate', got '{cleanup_mode}'"
            )
        params = dict(raw.get("params") or {})
        return cls(
            name=StepName(raw["name"]),
            type=raw["type"],
            enabled=bool(raw.get("enabled", True)),
            description=raw.get("description"),
            file=raw.get("file"),
            sql_id=raw.get("sql_id"),
            params=params,
            transaction_group=GroupId(raw["transaction_group"])
            if raw.get("transaction_group")
            else None,
            joined_group=GroupId(raw["joined_group"])
            if raw.get("joined_group")
            else None,
            joined_glue=joined_glue,
            cleanup_target=raw.get("cleanup_target"),
            cleanup_mode=cleanup_mode,
            profile=bool(raw.get("profile", False)),
            output_file=raw.get("output_file"),
            notify=bool(raw.get("notify", False)),
            ping_on_end=raw.get("ping_on_end"),
            ping_on_error=raw.get("ping_on_error"),
            foreach=_validated_foreach(raw.get("foreach"), raw["name"], params),
        )


def _validated_foreach(
    raw: Any, step_name: str, params: Mapping[str, Any]
) -> dict[str, list[Any]]:
    """Validate a step's `foreach:` block into `{axis: [values]}`.

    An axis whose name already appears in `params` is rejected rather
    than silently overridden: `params: {bkt: 3}` next to
    `foreach: {bkt: [0, 1]}` is a contradiction, and picking a winner
    would hide it."""
    if raw is None:
        return {}
    if not isinstance(raw, Mapping) or not raw:
        raise ValueError(
            f"step '{step_name}': foreach must be a non-empty mapping of "
            f"axis -> list of values, got {raw!r}"
        )
    axes: dict[str, list[Any]] = {}
    for axis, values in raw.items():
        if not isinstance(axis, str) or not axis.isidentifier():
            raise ValueError(
                f"step '{step_name}': foreach axis {axis!r} is not a valid param name"
            )
        if not isinstance(values, list) or not values:
            raise ValueError(
                f"step '{step_name}': foreach axis '{axis}' must be a "
                f"non-empty list, got {values!r}"
            )
        if axis in params:
            raise ValueError(
                f"step '{step_name}': foreach axis '{axis}' is also set in "
                f"params — remove one."
            )
        axes[axis] = list(values)
    return axes


def _suffix(axis: str, value: Any, width: int) -> str:
    """`_bkt00`, `_year2017`. Integers are zero-padded to the widest
    value on their axis so expanded names sort the way the values do."""
    if isinstance(value, int) and not isinstance(value, bool):
        return f"_{axis}{value:0{width}d}"
    return f"_{axis}{value}"


def _axis_width(values: list[Any]) -> int:
    ints = [v for v in values if isinstance(v, int) and not isinstance(v, bool)]
    return max((len(str(v)) for v in ints), default=1)


def expand_foreach(step: Step) -> list[Step]:
    """One declared step becomes the cross product of its `foreach` axes.

    Axes expand in declaration order with the LAST one varying fastest,
    i.e. the nesting a reader writing `for bkt: for year:` would expect.
    A step with no `foreach` passes through untouched, so this is safe
    to map over every manifest.
    """
    if not step.foreach:
        return [step]
    axes = list(step.foreach)
    widths = {a: _axis_width(step.foreach[a]) for a in axes}
    expanded: list[Step] = []
    for combo in itertools.product(*(step.foreach[a] for a in axes)):
        assignment = dict(zip(axes, combo))
        suffix = "".join(_suffix(a, assignment[a], widths[a]) for a in axes)
        expanded.append(
            replace(
                step,
                name=StepName(f"{step.name}{suffix}"),
                params={**step.params, **assignment},
                foreach={},
            )
        )
    return expanded


@dataclass(frozen=True, slots=True)
class ManifestConfig:
    """A loaded manifest — a list of validated steps in declaration order."""

    steps: list[Step]

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any], *, catalog=None) -> "ManifestConfig":
        """Build from a YAML-loaded mapping. If `catalog` is supplied
        (a `SqlCatalog`), every step that uses `sql_id:` instead of
        `file:` is resolved into a step with `file:` set, so downstream
        code never has to know whether the path came from a catalog or
        from inline manifest text.

        Steps declaring `foreach:` are expanded here, so `steps` is
        always the concrete execution list."""
        if not isinstance(raw, Mapping):
            raise ValueError(f"manifest must be a mapping, got {type(raw).__name__}")
        raw_steps = raw.get("steps") or []
        if not isinstance(raw_steps, list):
            raise ValueError("manifest 'steps' must be a list")
        declared = [Step.from_dict(s) for s in raw_steps]
        steps = [e for s in declared for e in expand_foreach(s)]
        if len(steps) != len(declared):
            # Expanded names exist nowhere in the source YAML, so
            # `YamlManager.disable_step` can never match them: a foreach
            # manifest is never rewritten, and never resumable that way.
            log.warning(
                "foreach expanded %d declared steps into %d — expanded steps "
                "are not auto-disabled in the manifest on completion.",
                len(declared),
                len(steps),
            )
        if catalog is not None:
            steps = [_resolve_sql_id(s, catalog) for s in steps]
        return cls(steps=steps)


def _resolve_sql_id(step: "Step", catalog) -> "Step":
    """If the step references a sql_id, resolve it against the catalog
    and return a new step with `file` filled in. Otherwise pass through."""
    if step.sql_id is None:
        return step
    entry = catalog.resolve(step.sql_id)
    return replace(step, file=entry.file, sql_id=None)
