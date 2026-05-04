"""SQL catalog — registry mapping `sql_id` → file path + metadata.

The catalog is the answer to "which `.sql` files exist, what does each
one do, and where does it live." Manifests can reference SQL by `sql_id`
instead of by file path, which gives the lineage chain a single point
of truth:

    profiles/<MANIFEST>.yaml → step.sql_id
       → sql-catalog.yaml#sql.<id>.file
          → scripts/sql/<file>.sql

Catalogs are optional. Manifests can keep using `file:` directly for
ad-hoc work; the catalog only matters when a SQL file has a stable
identity that callers want to refer to by name.

YAML format:

    sql:
      - id: load_staging
        file: scripts/sql/load_staging.sql
        intent: "Load raw rows from source into staging table."
        read_only: false
        expected_duration_s: 60
      - id: reconcile
        file: scripts/sql/reconcile.sql
        intent: "Match staging rows to canonical IDs."
        read_only: true

`intent`, `read_only`, and `expected_duration_s` are optional but
encouraged — they're how a future tool layer (alerts, scheduling,
write-protection guards) can tell which calls are safe and which are
slow.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from ruamel.yaml import YAML


@dataclass(frozen=True, slots=True)
class CatalogEntry:
    """One registered SQL file."""
    sql_id: str
    file: str
    intent: str | None = None
    read_only: bool = False
    expected_duration_s: float | None = None


class CatalogError(ValueError):
    """Raised on catalog parse failures or unresolved sql_id lookups."""


@dataclass(frozen=True, slots=True)
class SqlCatalog:
    """An immutable lookup of `sql_id` to `CatalogEntry`."""
    entries: Mapping[str, CatalogEntry]

    @classmethod
    def empty(cls) -> "SqlCatalog":
        """A catalog with no entries — every resolve() raises. Useful as
        a default when no `sql-catalog.yaml` is present."""
        return cls(entries={})

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "SqlCatalog":
        """Build from already-loaded YAML structure."""
        if not isinstance(raw, Mapping):
            raise CatalogError(
                f"catalog must be a mapping, got {type(raw).__name__}"
            )
        sql_list = raw.get("sql") or []
        if not isinstance(sql_list, list):
            raise CatalogError("catalog 'sql' must be a list")
        entries: dict[str, CatalogEntry] = {}
        for item in sql_list:
            if not isinstance(item, Mapping):
                raise CatalogError(f"catalog entry must be a mapping: {item!r}")
            if "id" not in item:
                raise CatalogError(f"catalog entry missing 'id': {dict(item)!r}")
            if "file" not in item:
                raise CatalogError(
                    f"catalog entry '{item['id']}' missing 'file'"
                )
            sid = str(item["id"])
            if sid in entries:
                raise CatalogError(f"duplicate sql_id in catalog: {sid!r}")
            entries[sid] = CatalogEntry(
                sql_id=sid,
                file=str(item["file"]),
                intent=item.get("intent"),
                read_only=bool(item.get("read_only", False)),
                expected_duration_s=(
                    float(item["expected_duration_s"])
                    if item.get("expected_duration_s") is not None else None
                ),
            )
        return cls(entries=entries)

    @classmethod
    def from_yaml(cls, path: Path) -> "SqlCatalog":
        """Load a catalog from `path`. Raises FileNotFoundError if the
        file does not exist; callers wanting "load if present" should
        use `load_if_exists` instead."""
        yaml = YAML(typ="safe")
        with open(path, "r", encoding="utf-8") as fh:
            raw = yaml.load(fh) or {}
        return cls.from_dict(raw)

    @classmethod
    def load_if_exists(cls, path: Path) -> "SqlCatalog":
        """Load `path` if it exists; otherwise return an empty catalog.
        The intended way to wire optional catalogs into a manifest run."""
        if not path.exists():
            return cls.empty()
        return cls.from_yaml(path)

    def resolve(self, sql_id: str) -> CatalogEntry:
        """Return the catalog entry for `sql_id` or raise CatalogError."""
        if sql_id not in self.entries:
            raise CatalogError(
                f"sql_id {sql_id!r} not found in catalog. "
                f"Registered ids: {sorted(self.entries.keys()) or 'none'}"
            )
        return self.entries[sql_id]
