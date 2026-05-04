"""Typed contracts for the orchestrator's data shapes.

Replaces the previous "untyped dict plumbing" — every YAML step is now
validated into a `Step` frozen dataclass at manifest load. Unknown keys
fail loudly at parse time instead of silently shadowing typos. Optional
fields keep their `None` default so existing manifests stay valid.

`Step.from_dict(...)` is the single ingress point. Internal code reads
fields via attribute access (`step.name`, `step.transaction_group`)
rather than `.get()`-style dict access.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, NewType


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
_RECOGNISED_KEYS = frozenset({
    "name", "type", "enabled", "description",
    "file", "params",
    "transaction_group", "joined_group", "joined_glue",
    "cleanup_target", "cleanup_mode",
    "profile", "output_file",
    "notify", "ping_on_end", "ping_on_error",
})


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

    # Source artifacts
    file: str | None = None
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
        return cls(
            name=StepName(raw["name"]),
            type=raw["type"],
            enabled=bool(raw.get("enabled", True)),
            description=raw.get("description"),
            file=raw.get("file"),
            params=dict(raw.get("params") or {}),
            transaction_group=GroupId(raw["transaction_group"]) if raw.get("transaction_group") else None,
            joined_group=GroupId(raw["joined_group"]) if raw.get("joined_group") else None,
            joined_glue=joined_glue,
            cleanup_target=raw.get("cleanup_target"),
            cleanup_mode=cleanup_mode,
            profile=bool(raw.get("profile", False)),
            output_file=raw.get("output_file"),
            notify=bool(raw.get("notify", False)),
            ping_on_end=raw.get("ping_on_end"),
            ping_on_error=raw.get("ping_on_error"),
        )


@dataclass(frozen=True, slots=True)
class ManifestConfig:
    """A loaded manifest — a list of validated steps in declaration order."""

    steps: list[Step]

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ManifestConfig":
        if not isinstance(raw, Mapping):
            raise ValueError(
                f"manifest must be a mapping, got {type(raw).__name__}"
            )
        raw_steps = raw.get("steps") or []
        if not isinstance(raw_steps, list):
            raise ValueError("manifest 'steps' must be a list")
        return cls(steps=[Step.from_dict(s) for s in raw_steps])
