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
import os
import re
from dataclasses import dataclass, field, replace
from typing import Any, Mapping, NewType

# `${VAR}` num valor de `params:` vem do ambiente (e portanto do `.env`, que o
# config/settings.py carrega com load_dotenv). Existe para que as RAIZES
# (`out`, `bronze`, `silver`, `oracle`) morem num lugar so, em vez de repetidas
# como literal em cada manifesto — mudar de disco passa a ser editar o `.env`.
_ENV = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _expande_env(valor: Any, passo: str, chave: str) -> Any:
    """Troca ${VAR} pelo ambiente. ERRA se a variavel nao existe.

    Falhar alto e deliberado: cair para string vazia transformaria
    `${LABMA_OUT}/eventos/...` em `/eventos/...` e o passo escreveria na raiz
    do sistema de arquivos — o tipo de erro que so aparece depois de gravar.
    """
    if not isinstance(valor, str):
        return valor

    def troca(m: "re.Match[str]") -> str:
        nome = m.group(1)
        v = os.environ.get(nome)
        if v is None or v == "":
            raise ValueError(
                f"passo '{passo}', param '{chave}': a variavel de ambiente "
                f"'{nome}' nao esta definida (nem no .env). Sem ela o caminho "
                f"viraria relativo a raiz do sistema de arquivos."
            )
        return v

    return _ENV.sub(troca, valor)


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
        "produces",
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

    # What the step leaves on disk, as a path template rendered with the
    # step's params (`"{{ out }}/tabuas/qx_bkt{{ bkt }}.parquet"`). Purely
    # a resume witness: declaring it lets `--resume` CHECK that a
    # completed step's output survived instead of trusting it. Steps that
    # write via `COPY … TO` — which the orchestrator cannot see into —
    # are exactly the ones worth declaring.
    produces: str | None = None

    # Notification controls
    notify: bool = False
    ping_on_end: str | None = None
    ping_on_error: str | None = None

    # Declared repetition. Each key is a param name, each value the list
    # it ranges over; several keys mean their cross product. Empty on
    # every step the executor ever sees — `ManifestConfig.from_dict`
    # expands it away at load.
    foreach: Mapping[str, list[Any]] = field(default_factory=dict)

    foreach_origin: StepName | None = None
    """De qual passo DECLARADO esta execucao saiu, quando veio de `foreach:`.

    `expand_foreach` zera o `foreach` das copias que produz, entao sem isto a
    execucao expandida nao sabe mais que era uma de N irmas independentes. Quem
    precisa saber e o executor concorrente: as execucoes de um mesmo `foreach`
    por BALDE sao independentes por construcao (`bkt = cpf_id % 16`, cada uma le
    e escreve so o seu `bkt=N`), e sao o unico lote que ele pode paralelizar com
    seguranca.

    Deduzir isso de "passos consecutivos com o mesmo `file`" tambem funcionaria
    hoje e passaria a mentir no dia em que dois passos distintos apontassem para
    o mesmo .sql. Marcar na expansao e barato e nao tem esse dia."""

    foreach_axes: tuple[str, ...] = ()
    """Quais eixos o `foreach:` de origem declarava, em ordem.

    Existe para o executor concorrente poder exigir `('bkt',)` EXATO em vez de
    "tem bkt entre os eixos". A diferenca importa: num produto `bkt x year` as
    execucoes de um mesmo balde cobrem varios anos, e passo por ano nao tem
    prova de independencia — varios leem a saida do ano anterior. Hoje o
    pipeline nao tem esse produto (18 passos so-`bkt`, 3 so-`year`), entao a
    regra estrita nao custa cobertura; ela custa se alguem criar o produto
    depois, que e exatamente quando se quer custar."""

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
        params = {
            k: _expande_env(v, raw["name"], k)
            for k, v in (raw.get("params") or {}).items()
        }
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
            produces=raw.get("produces"),
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
    would hide it.

    An axis may also be a STRING containing `${VAR}`: the variable is
    env-expanded (same rules as params — missing or empty is a hard
    error) and split on whitespace/commas, with pure-digit items parsed
    as ints so `_suffix` zero-pads them like a literal list. This is
    what lets `manifest_refresh.yaml` take its year axis from
    `LABMA_REFRESH_ANOS` at dispatch time while staying a versioned
    file. The native runner (scripts/plano.py) deliberately does NOT
    copy this: it only ever parses manifest_pipeline.yaml, whose axes
    are all literal lists."""
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
        if isinstance(values, str):
            expandido = _expande_env(values, step_name, f"foreach:{axis}")
            itens = [t for t in re.split(r"[\s,]+", expandido) if t]
            if not itens:
                raise ValueError(
                    f"step '{step_name}': foreach axis '{axis}' expanded to an "
                    f"empty list from {values!r} — an empty axis would silently "
                    f"run zero steps."
                )
            values = [int(t) if t.isdigit() else t for t in itens]
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
                foreach_origin=step.name,
                foreach_axes=tuple(axes),
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
            # manifest is never rewritten. Resume for these steps is the
            # run ledger (`src/ledger.py`, `--resume`), which keys off the
            # expanded plan rather than off the source file.
            log.info(
                "foreach expanded %d declared steps into %d — expanded steps "
                "are not auto-disabled in the manifest; use --resume to skip "
                "completed ones.",
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
