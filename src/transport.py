"""Transports — how a SQL statement actually reaches a database.

The implementations live here:

  - `DirectTransport` connects to a database via SQLAlchemy. Use when
    the caller has a network-reachable host:port (local Postgres,
    Oracle, or pgduckdb container with an exposed port).

  - `SshWslTransport` runs SQL on a remote machine via `ssh + wsl
    docker exec psql`. Use when the database lives inside a container
    on a remote host and only SSH is available — for example, MR3's
    pgduckdb (the container binds to 5434 inside WSL; Tailscale
    terminates at the Windows host, so direct connections are
    refused).

  - `DuckDbSshTransport` and `DuckDbLocalTransport` both run the same
    DuckDB helper program: one over ssh, one as a child process here.
    Which one a manifest wants is decided by where `main.py` runs
    relative to the data, not by preference — see `DuckDbLocalTransport`.

All transports return a `RawResult` with columns + rows + elapsed_ms.
The `run_sql` entry point in `src.api` picks one and types the output.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Mapping, Protocol

from src.database import DatabaseManager

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RawResult:
    """The shape every transport returns. `run_sql` wraps this into the
    public `QueryResult` after computing a SQL hash and writing
    provenance."""

    columns: list[str]
    rows: list[tuple]
    elapsed_ms: int


class Transport(Protocol):
    """Common shape for any way of getting SQL onto a database."""

    name: str
    """Short identifier used in provenance lines (e.g. 'direct',
    'ssh+wsl+pgduckdb')."""

    def execute(
        self, sql: str, params: Mapping[str, Any] | None = None
    ) -> RawResult: ...


class DirectTransport:
    """SQLAlchemy connection to a network-reachable host:port.

    The DB URL is constructed from `db_config` exactly the way
    `Executor` does — same dialect/user/password/host/port/database
    fields. Use this for local DBs, exposed-port containers, or
    SSH-tunnelled connections (pre-tunneled to localhost)."""

    name = "direct"

    def __init__(self, db_config: Mapping[str, Any]) -> None:
        self._db_config = dict(db_config)

    def execute(self, sql: str, params: Mapping[str, Any] | None = None) -> RawResult:
        url = _build_db_url(self._db_config)
        db = DatabaseManager(url)
        session = db.get_session()
        try:
            log.info("DirectTransport executing (%d chars)...", len(sql))
            start = time.monotonic()
            result = db.execute_query(
                sql, params=dict(params) if params else None, session=session
            )
            columns = list(result.keys())
            rows = [tuple(r) for r in result.fetchall()]
            elapsed_ms = int((time.monotonic() - start) * 1000)
            log.info("DirectTransport returned %d rows (%dms).", len(rows), elapsed_ms)
            return RawResult(columns=columns, rows=rows, elapsed_ms=elapsed_ms)
        finally:
            session.close()
            db.close()


class SshWslTransport:
    """Run SQL on a remote machine via `ssh + wsl docker exec psql`.

    Targets the LabMA pattern where pgduckdb (or similar) runs inside a
    Docker container on an MR3-like Windows-with-WSL host. The
    transport ships SQL over stdin to `psql --csv` running inside the
    container; CSV output comes back over stdout.

    Args:
        ssh: ssh target, e.g. ``user@host``.
        container: docker container name, e.g. ``pgduckdb``.
        pg_user: postgres user inside the container.
        pg_database: postgres database inside the container.
        wsl: prepend ``wsl`` (i.e. host is Windows running WSL). Default
            True since that's the only deployment we have so far.
        sudo: prepend ``sudo`` to docker (rootful docker installs).
        ssh_options: extra ssh options as a list of `-o KEY=VALUE`
            strings. Empty list by default.
    """

    name = "ssh+wsl"

    def __init__(
        self,
        *,
        ssh: str,
        container: str,
        pg_user: str = "postgres",
        pg_database: str = "postgres",
        wsl: bool = True,
        sudo: bool = True,
        ssh_options: list[str] | None = None,
    ) -> None:
        self.ssh = ssh
        self.container = container
        self.pg_user = pg_user
        self.pg_database = pg_database
        self.wsl = wsl
        self.sudo = sudo
        self.ssh_options = list(ssh_options or [])

    def execute(self, sql: str, params: Mapping[str, Any] | None = None) -> RawResult:
        if params:
            # SQLAlchemy-style :name binding doesn't survive a raw psql
            # call. Callers that need parameter substitution should
            # render the SQL before invoking the transport (the
            # orchestrator's existing `render_template` is one option).
            raise NotImplementedError(
                "SshWslTransport does not support :name bind params. "
                "Render the SQL before calling execute()."
            )
        cmd = self._build_command()
        log.info(
            "SshWslTransport executing on %s/%s (%d chars)...",
            self.ssh,
            self.container,
            len(sql),
        )
        start = time.monotonic()
        proc = subprocess.run(
            cmd,
            input=sql.encode("utf-8"),
            capture_output=True,
            check=False,
        )
        elapsed_ms = int((time.monotonic() - start) * 1000)
        if proc.returncode != 0:
            stderr = proc.stderr.decode("utf-8", errors="replace")
            raise RuntimeError(
                f"ssh+wsl psql failed (rc={proc.returncode}): {stderr[:500]}"
            )
        body = proc.stdout.decode("utf-8", errors="replace")
        columns, rows = _parse_psql_csv(body)
        log.info("SshWslTransport returned %d rows (%dms).", len(rows), elapsed_ms)
        return RawResult(columns=columns, rows=rows, elapsed_ms=elapsed_ms)

    def _build_command(self) -> list[str]:
        ssh_part = ["ssh"] + self.ssh_options + [self.ssh]
        wrapper = ["wsl"] if self.wsl else []
        docker_part = (["sudo"] if self.sudo else []) + [
            "docker",
            "exec",
            "-i",
            self.container,
            "psql",
            "-U",
            self.pg_user,
            "-d",
            self.pg_database,
            "-v",
            "ON_ERROR_STOP=1",
            "--csv",
            "-f",
            "-",
        ]
        return ssh_part + wrapper + docker_part


_SIZE = re.compile(r"^\d+(\.\d+)?\s*(B|K|M|G|T|KB|MB|GB|TB|KIB|MIB|GIB|TIB)$", re.I)

_UNIDADE_MIB = {
    "B": 1 / 1048576,
    "K": 1 / 1024,
    "KB": 1 / 1024,
    "KIB": 1 / 1024,
    "M": 1,
    "MB": 1,
    "MIB": 1,
    "G": 1024,
    "GB": 1024,
    "GIB": 1024,
    "T": 1048576,
    "TB": 1048576,
    "TIB": 1048576,
}


def _divide_size(tamanho: str, partes: int) -> str:
    """`('16GB', 2) -> '8192MiB'`. Divide um tamanho DuckDB em N fatias.

    Devolve sempre MiB inteiro: dividir '1GB' por 3 em 'GB' daria '0GB', que o
    DuckDB aceita como ZERO e derruba a sessao na primeira alocacao. MiB tem
    resolucao suficiente para qualquer divisao plausivel e nunca arredonda para
    baixo ate zero — o piso e 1 MiB, que falha alto e claro em vez de silencioso.
    """
    m = _SIZE.match(tamanho.strip())
    if not m:
        raise ValueError(f"tamanho invalido: {tamanho!r}")
    numero = float(tamanho.strip()[: m.start(2)].strip())
    mib = numero * _UNIDADE_MIB[m.group(2).upper()]
    return f"{max(1, int(mib // partes))}MiB"


def coerce_bool(value: Any) -> bool:
    """`.env` values arrive as strings; this is the one place that
    decides what counts as true."""
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True, slots=True)
class DuckDbSettings:
    """DuckDB knobs that belong to the run, not to the SQL.

    These used to be set by whichever script happened to open the
    connection, which meant every consumer re-derived them and none of
    them were reviewable. Defaults are the values measured on MR3 —
    8 threads at 16GB is ~2GB/thread — and changing them without
    measuring is how a shared machine starts thrashing.

    `temp_directory` defaults under `$HOME`, which on MR3 is NVMe.
    Pointing it at `/mnt/BANCOS` would put spill on a rotational RAID1,
    and spill is write-heavy — the worst case for that device.

    `max_temp_directory_size` is mandatory and bounded: the box is
    shared, and an unbounded spill fills the root filesystem and takes
    down somebody else's session.
    """

    memory_limit: str = "16GB"
    threads: int = 8
    temp_directory: str = "~/duckdb_spill"
    max_temp_directory_size: str = "512GB"
    preserve_insertion_order: bool = False
    profile: bool = False

    concurrency: int = 1
    """Quantas sessoes DuckDB rodam LADO A LADO as execucoes de um mesmo
    `foreach` por balde. 1 (padrao) = comportamento historico, sessao unica.

    ⚠ `memory_limit` e o orcamento do RUN INTEIRO, nao de cada sessao: com
    `concurrency=N` cada worker recebe `memory_limit / N`. Isso e deliberado e e
    o unico default seguro numa maquina compartilhada — o MR3 tem 30 GB, e dois
    workers a 16 GB pediriam 32 GB. Com `concurrency=1` a conta e identica a
    hoje, entao nenhuma configuracao existente muda de comportamento.

    O preco e mais derramamento por worker. Medido nesta base: subir o teto de
    16 para 20 GB valia ~9 s no total, porque o spill vai para NVMe — barato.
    Dividir deve custar na mesma ordem, mas ISSO SE MEDE, nao se assume."""

    slot: str = ""
    """Sufixo que torna o arquivo de perfil EXCLUSIVO desta sessao.

    O caminho do perfil e `<temp_directory>/_orch_profile<slot>.json`, e
    `temp_directory` e COMPARTILHADO entre sessoes. Com o slot vazio (uma sessao
    so, o padrao histórico) nada muda. Com sessoes concorrentes, duas delas
    escreveriam no MESMO arquivo e cada uma leria o perfil da outra — os planos
    capturados sairiam trocados sem nenhum erro, e todas as medicoes desta
    campanha saem desses planos. Por isso o slot entra no NOME do arquivo, e nao
    numa pasta por sessao: o `PlanStore` ja sabe achar o arquivo pelo caminho que
    o helper devolve."""

    def __post_init__(self) -> None:
        if not _SIZE.match(self.memory_limit.strip()):
            raise ValueError(
                f"memory_limit must be a DuckDB size like '16GB', got "
                f"{self.memory_limit!r}"
            )
        if self.threads < 1:
            raise ValueError(f"threads must be >= 1, got {self.threads}")
        if not self.temp_directory.strip():
            raise ValueError(
                "temp_directory must name a directory — spill has to land "
                "somewhere chosen, not wherever DuckDB defaults to."
            )
        if not _SIZE.match(self.max_temp_directory_size.strip()):
            raise ValueError(
                f"max_temp_directory_size must be a bounded DuckDB size like "
                f"'512GB', got {self.max_temp_directory_size!r} — the host is "
                f"shared, so an unbounded spill is not an option."
            )
        if self.concurrency < 1:
            raise ValueError(f"concurrency must be >= 1, got {self.concurrency}")

    def for_slot(self, indice: int) -> "DuckDbSettings":
        """As settings de UM worker: perfil proprio e a sua fatia de memoria.

        Divide `memory_limit` por `concurrency` (ver o campo) e carimba um slot,
        para que dois workers nao escrevam no mesmo `_orch_profile.json`. Com
        `concurrency == 1` devolve `self` intacto — mesmo objeto, mesmo nome de
        arquivo de perfil que sempre teve."""
        if self.concurrency == 1:
            return self
        return replace(
            self,
            memory_limit=_divide_size(self.memory_limit, self.concurrency),
            slot=f"_w{indice}",
        )

    @classmethod
    def from_mapping(cls, cfg: Mapping[str, Any]) -> "DuckDbSettings":
        """Build from a `DB_TARGET_<NAME>_*` entry. Absent keys keep the
        documented default; they are not sentinels."""
        defaults = cls()
        return cls(
            memory_limit=str(cfg.get("memory_limit") or defaults.memory_limit),
            threads=int(cfg.get("threads") or defaults.threads),
            temp_directory=str(cfg.get("temp_directory") or defaults.temp_directory),
            max_temp_directory_size=str(
                cfg.get("max_temp_directory_size") or defaults.max_temp_directory_size
            ),
            preserve_insertion_order=coerce_bool(
                cfg.get("preserve_insertion_order", defaults.preserve_insertion_order)
            ),
            profile=coerce_bool(cfg.get("profile", defaults.profile)),
            concurrency=int(cfg.get("concurrency") or defaults.concurrency),
        )

    def as_payload(self) -> dict[str, Any]:
        """The JSON the remote helper reads off its first stdin line.

        `temp_directory` ships unexpanded on purpose: `~` means the
        remote user's home, and only the remote side knows where that
        is."""
        return {
            "memory_limit": self.memory_limit,
            "threads": self.threads,
            "temp_directory": self.temp_directory,
            "max_temp_directory_size": self.max_temp_directory_size,
            "preserve_insertion_order": self.preserve_insertion_order,
            "profile": self.profile,
            "slot": self.slot,
        }


DUCKDB_HELPER = r'''"""Remote program for the orchestrator's DuckDB transports.

Reads a settings JSON object off the first stdin line, opens ONE DuckDB
connection, and then either (a) runs the rest of stdin as a single
statement and writes CSV, or (b) with `--serve`, loops reading
line-delimited JSON requests and answering them on that same connection
— which is what keeps TEMP TABLEs and macros alive across steps.

Uploaded by `DuckDbSshTransport._ensure_helper`; never imported locally.
"""
import csv
import json
import os
import sys
import time

import duckdb

PROFILE_FILE = "_orch_profile.json"


def _lit(value):
    return str(value).replace("'", "''")


def apply_settings(con, cfg):
    """Apply the run's knobs. Returns the profile path, or None when
    profiling is off."""
    tmp = os.path.expanduser(cfg["temp_directory"])
    os.makedirs(tmp, exist_ok=True)
    con.execute("SET memory_limit='%s'" % _lit(cfg["memory_limit"]))
    con.execute("SET threads=%d" % int(cfg["threads"]))
    # Cacheia o RODAPE dos parquets entre aberturas do mesmo arquivo. O pipeline
    # abre o mesmo artefato dezenas de vezes na MESMA sessao — o
    # `ano_nao_fechado_staged` sozinho e lido 96 vezes por 4 consumidores — e o
    # orquestrador mantem UMA conexao para a execucao inteira, que e a condicao
    # em que a doc do DuckDB diz que isto ajuda. So metadado; nao cacheia dado.
    # (`enable_object_cache` NAO serve: virou placeholder documentado que nao faz nada.)
    con.execute("SET parquet_metadata_cache=true")
    con.execute("SET temp_directory='%s'" % _lit(tmp))
    con.execute(
        "SET max_temp_directory_size='%s'" % _lit(cfg["max_temp_directory_size"])
    )
    con.execute(
        "SET preserve_insertion_order=%s"
        % ("true" if cfg["preserve_insertion_order"] else "false")
    )
    if not cfg.get("profile"):
        return None
    # o slot separa sessoes CONCORRENTES: `tmp` e compartilhado, entao sem ele
    # duas sessoes escreveriam e liriam o mesmo _orch_profile.json e trocariam
    # os planos entre si — em silencio. Slot vazio == nome historico.
    slot = str(cfg.get("slot") or "")
    path = os.path.join(tmp, PROFILE_FILE.replace(".json", "%s.json" % slot))
    con.execute("SET enable_profiling='json'")
    con.execute("SET profiling_output='%s'" % _lit(path))
    # profiling_coverage='SELECT' e o PADRAO, e ele NAO perfila CREATE TABLE AS.
    # Isso escondia o passo mais caro do contratos_por_ano: o PASS 1 dele e um
    # `CREATE OR REPLACE TEMP TABLE ... AS`, onde mora ~84% do tempo. Com o
    # padrao, o perfil daquele passo mostrava so o COPY final e mentia por
    # omissao. 'ALL' cobre DDL e COPY tambem.
    con.execute("SET profiling_coverage='ALL'")
    # SEM custom_profiling_settings, de proposito. Medido na bancada
    # (tests/perfil/sonda_duckdb.py, 2026-07-31): o PADRAO traz 21 campos de topo
    # e 12 por operador; uma lista curada por mim trazia 14 e 12. Ou seja, curar
    # so PERDE informacao — o default ja e o superconjunto, e nao cobra por isso
    # (1,47s vs 1,26s na mesma consulta, dentro do ruido).
    #
    # profiling_mode='DETAILED' tambem nao entra: medido, da exatamente o mesmo
    # conjunto de campos que STANDARD.
    #
    # ⚠ Sobre o que esses campos NAO sao: TOTAL_BYTES_READ/WRITTEN nao medem
    #   leitura de arquivo externo (varrer 0,84 GB de parquet reporta 209 KB —
    #   sao bytes do buffer manager). Quem responde "I/O ou CPU?" e
    #   blocked_thread_time contra cpu_time/latency.
    return path


def take_profile(path):
    """Read and DELETE the profile DuckDB wrote for the statement that
    just ran. Deleting is what makes a missing file mean 'no profile for
    this statement' instead of 'the profile of some earlier one'."""
    if not path:
        return None
    try:
        with open(path) as handle:
            profile = json.load(handle)
    except (OSError, ValueError):
        return None
    try:
        os.remove(path)
    except OSError:
        pass
    return profile


def run(con, sql, profile_path):
    """Roda o SQL do passo e devolve UM perfil POR STATEMENT.

    Um .sql de passo pode ter varios statements (o contratos_por_ano tem tres:
    CREATE TEMP TABLE, COPY, DROP). Executar tudo num `con.execute(sql)` so
    fazia o DuckDB sobrescrever `profiling_output` a cada statement, entao
    sobrava apenas o perfil do ULTIMO — no contratos_por_ano, o do DROP. Pior:
    `plans.py` usa a latencia do perfil como tempo do passo, entao o passo
    aparecia com 0,05 s em vez dos ~20 s reais. Dividir com
    `con.extract_statements` (que respeita strings e comentarios, ao contrario
    de partir por ';') e colher o perfil apos CADA statement conserta os dois.
    """
    start = time.perf_counter()
    statements = None
    if profile_path:
        try:
            statements = con.extract_statements(sql)
        except Exception:  # noqa: BLE001 — split e otimizacao, nao correcao
            statements = None
    if not statements or len(statements) <= 1:
        cur = con.execute(sql)
        description = cur.description
        columns = [d[0].lower() for d in description] if description else []
        rows = cur.fetchall() if description else []
        profile = take_profile(profile_path)
        return {
            "columns": columns,
            "rows": rows,
            "elapsed_ms": int((time.perf_counter() - start) * 1000),
            "profile": profile,
            "profiles": [profile] if profile else [],
        }
    profiles = []
    columns, rows = [], []
    for statement in statements:
        cur = con.execute(statement)
        description = cur.description
        # so o ultimo statement com resultado define a resposta; os anteriores
        # sao DDL/COPY. Manter o comportamento de antes para quem consome linhas.
        if description:
            columns = [d[0].lower() for d in description]
            rows = cur.fetchall()
        profile = take_profile(profile_path)
        if profile:
            profiles.append(profile)
    return {
        "columns": columns,
        "rows": rows,
        "elapsed_ms": int((time.perf_counter() - start) * 1000),
        "profile": profiles[-1] if profiles else None,
        "profiles": profiles,
    }


def snapshot_settings(con):
    """Toda a configuracao efetiva da sessao, para gravar junto da execucao.

    Comparar duas execucoes sem saber com que `threads`/`memory_limit` cada uma
    rodou e comparar dois numeros sem unidade."""
    try:
        cur = con.execute(
            "SELECT name, value FROM duckdb_settings() WHERE value <> '' ORDER BY name"
        )
        return [{"name": n, "value": str(v)} for n, v in cur.fetchall()]
    except Exception:  # noqa: BLE001 — snapshot e diagnostico, nunca bloqueia
        return []


def serve(con, profile_path, stdin, stdout):
    hello = {"ready": True, "duckdb": duckdb.__version__}
    # So manda a configuracao quando ha profiling: com profiling desligado a
    # execucao nao deve deixar artefato nenhum, e o snapshot faz parte do
    # conjunto de artefatos de perfil, nao do caminho normal.
    if profile_path:
        hello["settings"] = snapshot_settings(con)
    stdout.write(json.dumps(hello) + "\n")
    stdout.flush()
    while True:
        line = stdin.readline()
        if not line:
            return
        line = line.strip()
        if not line:
            continue
        request = json.loads(line)
        if request.get("cmd") == "shutdown":
            return
        try:
            response = run(con, request["sql"], profile_path)
            response.update({"id": request.get("id"), "ok": True, "error": None})
        except Exception as exc:
            # A failing statement must not kill the session: the caller
            # decides whether to abandon the run, and it needs the error
            # text to decide. Catch-all because DuckDB raises a dozen
            # unrelated types and every one of them means the same thing
            # here.
            response = {
                "id": request.get("id"),
                "ok": False,
                "error": "%s: %s" % (type(exc).__name__, exc),
            }
        stdout.write(json.dumps(response, default=str) + "\n")
        stdout.flush()


def main(argv):
    cfg = json.loads(sys.stdin.readline())
    con = duckdb.connect()
    profile_path = apply_settings(con, cfg)
    if "--serve" in argv:
        serve(con, profile_path, sys.stdin, sys.stdout)
        return 0
    out = run(con, sys.stdin.read().strip().rstrip(";"), profile_path)
    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow(out["columns"])
    for row in out["rows"]:
        writer.writerow(row)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
'''
"""Source of the helper program. Kept as a string because the ssh path has
to run it on the *other* machine, where this package does not exist."""


def write_helper(path: Path) -> None:
    """Drop the DuckDB helper on the LOCAL filesystem.

    The ssh path uploads it with `tee`; this is how the local transport
    and the tests get the exact same program — the session protocol is
    only worth testing against the code that will actually serve it.

    ⚠ ATOMICO (2026-08-01), e nao por elegancia. Com `concurrency > 1` as N
    sessoes compartilham UM transporte e chamam `_ensure_helper()` quase ao
    mesmo tempo; o flag `_helper_written` nao e lock, entao as N passam pela
    checagem e as N escrevem o mesmo arquivo. Com `write_text` isso e
    truncar-e-escrever concorrente: um worker chega a iniciar o Python sobre um
    arquivo PELA METADE. Foi exatamente o que derrubou a bancada em W=3
    ("Expecting ':' delimiter ... char 8203", com o helper tendo 8795 bytes).
    Escrever ao lado e renomear torna a publicacao atomica: todo leitor ve ou o
    arquivo antigo inteiro ou o novo inteiro, nunca um meio-termo."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporario = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    temporario.write_text(DUCKDB_HELPER, encoding="utf-8")
    os.replace(temporario, path)


def _run_duckdb_helper(
    argv: list[str], settings: DuckDbSettings, sql: str, label: str
) -> RawResult:
    """One-shot helper invocation: settings on the first stdin line, SQL
    after it, CSV back on stdout.

    Shared by both DuckDB transports on purpose. They differ only in the
    argv that fronts the helper; a local transport carrying its own copy
    of this would be a second execution path to keep in step with the
    remote one, which is exactly the divergence it exists to remove."""
    log.info("%s executing (%d chars)...", label, len(sql))
    stdin = json.dumps(settings.as_payload()) + "\n" + sql
    start = time.monotonic()
    proc = subprocess.run(
        argv,
        input=stdin.encode("utf-8"),
        capture_output=True,
        check=False,
    )
    elapsed_ms = int((time.monotonic() - start) * 1000)
    if proc.returncode != 0:
        stderr = proc.stderr.decode("utf-8", errors="replace")
        raise RuntimeError(f"{label} failed (rc={proc.returncode}): {stderr[:500]}")
    columns, rows = _parse_psql_csv(proc.stdout.decode("utf-8", errors="replace"))
    log.info("%s returned %d rows (%dms).", label, len(rows), elapsed_ms)
    return RawResult(columns=columns, rows=rows, elapsed_ms=elapsed_ms)


class DuckDbTransport(Protocol):
    """A transport that can host a persistent DuckDB session.

    Narrower than `Transport`: `Executor`'s DuckDB path does not call
    `execute()` at all — it asks for the argv of a helper that will stay
    alive for the whole run, and for the settings that helper opens with.
    `DuckDbSshTransport` and `DuckDbLocalTransport` both satisfy it, and
    `src.duckdb_session.open_transport_session` accepts either."""

    name: str
    settings: DuckDbSettings

    def session_command(self) -> list[str]: ...


class DuckDbSshTransport:
    """Run DuckDB SQL on a remote host via `ssh [wsl] python3 <helper>`.

    The companion of `SshWslTransport`: where that one reaches a
    containerised Postgres, this one reaches **DuckDB running directly on
    the host**, so queries that read on-disk parquet trees
    (`read_parquet([...])`, `glob()`, `filename=true`, `strftime`,
    `hash`) work — the full DuckDB dialect, which pgduckdb-in-Postgres
    does not expose.

    The helper script is shipped to `helper_path` on first use (idempotent,
    once per process) and invoked with the settings on the first stdin
    line and the SQL after it. No bind params: callers render the SQL
    before calling.

    `execute()` opens a fresh remote DuckDB per call, which is right for
    ad-hoc queries and wrong for a pipeline. For a run whose steps share
    TEMP TABLEs, use `src.duckdb_session.open_ssh_session(transport)`.

    Args:
        ssh: ssh target, e.g. ``user@host``.
        helper_path: where the helper lands on the host.
        wsl: prepend ``wsl`` (host is Windows running WSL). Default True.
        settings: DuckDB knobs for the connection.
    """

    name = "ssh+duckdb"

    def __init__(
        self,
        *,
        ssh: str,
        helper_path: str = "/tmp/_orch_duckdb.py",
        wsl: bool = True,
        settings: DuckDbSettings | None = None,
        python: str = "python3",
        ssh_options: list[str] | None = None,
    ) -> None:
        self.ssh = ssh
        self.helper_path = helper_path
        self.wsl = wsl
        self.settings = settings or DuckDbSettings()
        # The helper needs a `duckdb` import, which a host's *system* python
        # often lacks while a project venv on the same host has it. Naming the
        # interpreter is the difference between "this host can't run DuckDB"
        # and "point at the venv that already can".
        self.python = python
        self.ssh_options = list(ssh_options or [])
        self._helper_synced = False

    @property
    def threads(self) -> int:
        return self.settings.threads

    def _ssh_prefix(self) -> list[str]:
        return ["ssh"] + self.ssh_options + [self.ssh] + (["wsl"] if self.wsl else [])

    def session_command(self) -> list[str]:
        """argv that starts a persistent remote DuckDB. Uploads the
        helper first — the process cannot start without it."""
        self._ensure_helper()
        return self._ssh_prefix() + [self.python, self.helper_path, "--serve"]

    def _ensure_helper(self) -> None:
        if self._helper_synced:
            return
        body = DUCKDB_HELPER.encode("utf-8")
        proc = subprocess.run(
            self._ssh_prefix() + ["tee", self.helper_path],
            input=body,
            capture_output=True,
            check=False,
        )
        if proc.returncode != 0:
            raise RuntimeError(
                f"failed to upload duckdb helper to {self.ssh}:{self.helper_path} "
                f"(rc={proc.returncode}): {proc.stderr.decode('utf-8', 'replace')[:300]}"
            )
        self._helper_synced = True

    def execute(self, sql: str, params: Mapping[str, Any] | None = None) -> RawResult:
        if params:
            raise NotImplementedError(
                "DuckDbSshTransport does not support :name bind params. "
                "Render the SQL before calling execute()."
            )
        self._ensure_helper()
        return _run_duckdb_helper(
            self._ssh_prefix() + [self.python, self.helper_path],
            self.settings,
            sql,
            f"{self.name} on {self.ssh}",
        )


class DuckDbLocalTransport:
    """Run DuckDB in a helper process on THIS machine — no ssh at all.

    The sibling of `DuckDbSshTransport`, and the one a manifest wants
    whenever the orchestrator already runs where the data is. MR3 invokes
    `main.py` on MR3: an ssh transport there would `ssh` the box to
    itself, back through the comp20 hop, with no key installed. Moving
    `main.py` to the laptop fixes the ssh but breaks everything the
    executor does with paths — `_prepare_output_dir` and `--resume` stat
    the local filesystem, so directories get made on the wrong machine
    and resume verifies files that were never going to be there. Neither
    placement works; running DuckDB locally is what makes one of them
    work.

    Everything below the argv is shared with the ssh transport: same
    helper source, same JSON-line session protocol, same
    `DuckDbSettings`. The only thing that differs is that the helper is
    written with `write_helper` instead of shipped through `tee`.

    Args:
        helper_path: where the helper program is written on this machine.
        settings: DuckDB knobs for the connection.
        python: interpreter that runs the helper. Defaults to the one
            running the orchestrator, which is the interpreter whose
            environment was already resolved to have `duckdb`.
    """

    name = "duckdb"

    def __init__(
        self,
        *,
        helper_path: str = "/tmp/_orch_duckdb.py",
        settings: DuckDbSettings | None = None,
        python: str | None = None,
    ) -> None:
        # Expanded here rather than at use: the path goes into an argv,
        # and no shell is involved to expand a `~` on the way.
        self.helper_path = str(Path(helper_path).expanduser())
        self.settings = settings or DuckDbSettings()
        self.python = python or sys.executable
        self._helper_written = False

    @property
    def threads(self) -> int:
        return self.settings.threads

    def session_command(self) -> list[str]:
        """argv that starts a persistent local DuckDB. Writes the helper
        first — the process cannot start without it."""
        self._ensure_helper()
        return [self.python, self.helper_path, "--serve"]

    def _ensure_helper(self) -> None:
        if self._helper_written:
            return
        path = Path(self.helper_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        write_helper(path)
        self._helper_written = True

    def execute(self, sql: str, params: Mapping[str, Any] | None = None) -> RawResult:
        if params:
            raise NotImplementedError(
                "DuckDbLocalTransport does not support :name bind params. "
                "Render the SQL before calling execute()."
            )
        self._ensure_helper()
        return _run_duckdb_helper(
            [self.python, self.helper_path], self.settings, sql, self.name
        )


class ClickHouseSshTransport:
    """Run ClickHouse SQL on a remote machine via `ssh + wsl docker exec
    clickhouse-client`.

    The ClickHouse sibling of `SshWslTransport`: same `ssh + [wsl]
    [sudo] docker exec -i <container>` envelope, but the in-container
    program is `clickhouse-client` instead of `psql`. SQL ships over
    stdin; results come back as CSV with a header row
    (`--format CSVWithNames`), so the existing `_parse_psql_csv` helper
    parses them unchanged.

    Performance settings that matter for the LabMA biometric tables
    (large GROUP BY / JOIN over hundreds of millions of CNIS rows) are
    exposed as constructor params and passed as `--<setting>=<value>`
    flags. Defaults are sized for the MR3 rig (~27 GB host RAM).

    Args:
        ssh: ssh target, e.g. ``user@host``.
        container: docker container name, e.g. ``clickhouse-tabua``.
        ch_database: ClickHouse database to connect to (``-d <db>``).
            Optional — leave None to qualify tables in the SQL instead
            (``SELECT … FROM mydb.mytable``) or rely on ``USE``.
        wsl: prepend ``wsl`` (i.e. host is Windows running WSL). Default
            True since that's the only deployment we have so far.
        sudo: prepend ``sudo`` to docker (rootful docker installs).
        max_threads: ClickHouse ``max_threads`` setting.
        max_bytes_before_external_group_by: spill threshold for GROUP BY.
        join_algorithm: ClickHouse ``join_algorithm`` (e.g.
            ``grace_hash`` to spill large joins to disk).
        max_memory_usage: per-query memory ceiling in bytes.
        ssh_options: extra ssh options as a list of `-o KEY=VALUE`
            strings. Empty list by default.
    """

    name = "ssh+clickhouse"

    def __init__(
        self,
        *,
        ssh: str,
        container: str,
        ch_database: str | None = None,
        wsl: bool = True,
        sudo: bool = True,
        max_threads: int = 4,
        max_bytes_before_external_group_by: int = 2_000_000_000,
        join_algorithm: str = "grace_hash",
        max_memory_usage: int = 22_000_000_000,
        ssh_options: list[str] | None = None,
    ) -> None:
        self.ssh = ssh
        self.container = container
        self.ch_database = ch_database
        self.wsl = wsl
        self.sudo = sudo
        self.max_threads = max_threads
        self.max_bytes_before_external_group_by = max_bytes_before_external_group_by
        self.join_algorithm = join_algorithm
        self.max_memory_usage = max_memory_usage
        self.ssh_options = list(ssh_options or [])

    def execute(self, sql: str, params: Mapping[str, Any] | None = None) -> RawResult:
        if params:
            raise NotImplementedError(
                "ClickHouseSshTransport does not support :name bind params. "
                "Render the SQL before calling execute()."
            )
        cmd = self._build_command()
        log.info(
            "ClickHouseSshTransport executing on %s/%s (%d chars)...",
            self.ssh,
            self.container,
            len(sql),
        )
        start = time.monotonic()
        proc = subprocess.run(
            cmd,
            input=sql.encode("utf-8"),
            capture_output=True,
            check=False,
        )
        elapsed_ms = int((time.monotonic() - start) * 1000)
        if proc.returncode != 0:
            stderr = proc.stderr.decode("utf-8", errors="replace")
            raise RuntimeError(
                f"ssh+clickhouse clickhouse-client failed "
                f"(rc={proc.returncode}): {stderr[:500]}"
            )
        body = proc.stdout.decode("utf-8", errors="replace")
        columns, rows = _parse_psql_csv(body)
        log.info(
            "ClickHouseSshTransport returned %d rows (%dms).", len(rows), elapsed_ms
        )
        return RawResult(columns=columns, rows=rows, elapsed_ms=elapsed_ms)

    def _build_command(self) -> list[str]:
        ssh_part = ["ssh"] + self.ssh_options + [self.ssh]
        wrapper = ["wsl"] if self.wsl else []
        client = [
            "clickhouse-client",
            "--format",
            "CSVWithNames",
            "--multiquery",
            f"--max_threads={self.max_threads}",
            "--max_bytes_before_external_group_by="
            f"{self.max_bytes_before_external_group_by}",
            f"--join_algorithm={self.join_algorithm}",
            f"--max_memory_usage={self.max_memory_usage}",
        ]
        if self.ch_database:
            client += ["-d", self.ch_database]
        docker_part = (
            (["sudo"] if self.sudo else [])
            + [
                "docker",
                "exec",
                "-i",
                self.container,
            ]
            + client
        )
        return ssh_part + wrapper + docker_part


def build_transport(
    db_config: Mapping[str, Any] | None = None,
    *,
    transport: str | None = None,
    ssh: str | None = None,
    container: str | None = None,
    pg_user: str = "postgres",
    pg_database: str = "postgres",
    ch_database: str | None = None,
    wsl: bool = True,
    sudo: bool = True,
    helper_path: str = "/tmp/_orch_duckdb.py",
    threads: int = 8,
    settings: DuckDbSettings | None = None,
    python: str | None = None,
) -> Transport:
    """Return a transport based on the supplied arguments.

    `transport` is a string selector. If omitted, `direct` is the
    default. The dispatch lives here (rather than in run_sql) so other
    callers can construct transports for testing or for non-run_sql
    workflows.

    `threads` is the one-knob shorthand kept for existing callers;
    `settings` carries the full `DuckDbSettings` and wins when both are
    given.

    `python` has no single sensible default across transports: the remote
    helper wants whatever the *host* calls python3, the local one wants
    the interpreter already running, so each branch names its own."""
    kind = (transport or "direct").lower()
    if kind == "direct":
        if db_config is None:
            raise ValueError("DirectTransport requires db_config")
        return DirectTransport(db_config)
    if kind in ("ssh+wsl", "ssh_wsl"):
        if not ssh or not container:
            raise ValueError(
                "SshWslTransport requires `ssh` (host target) and "
                "`container` (docker container name)."
            )
        return SshWslTransport(
            ssh=ssh,
            container=container,
            pg_user=pg_user,
            pg_database=pg_database,
            wsl=wsl,
            sudo=sudo,
        )
    if kind in ("ssh+duckdb", "ssh_duckdb", "duckdb+ssh"):
        if not ssh:
            raise ValueError("DuckDbSshTransport requires `ssh` (host target).")
        return DuckDbSshTransport(
            ssh=ssh,
            helper_path=helper_path,
            wsl=wsl,
            settings=settings or DuckDbSettings(threads=threads),
            python=python or "python3",
        )
    if kind in ("duckdb", "duckdb+local", "local+duckdb"):
        return DuckDbLocalTransport(
            helper_path=helper_path,
            settings=settings or DuckDbSettings(threads=threads),
            python=python,
        )
    if kind in ("ssh+clickhouse", "ssh_clickhouse", "clickhouse+ssh"):
        if not ssh or not container:
            raise ValueError(
                "ClickHouseSshTransport requires `ssh` (host target) and "
                "`container` (docker container name)."
            )
        return ClickHouseSshTransport(
            ssh=ssh,
            container=container,
            ch_database=ch_database,
            wsl=wsl,
            sudo=sudo,
        )
    raise ValueError(f"Unknown transport: {kind!r}")


def _build_db_url(db_config: Mapping[str, Any]) -> str:
    """Build a SQLAlchemy URL. Lives here so transport doesn't import
    api (api imports transport). The api re-exports this for the CLI."""
    dialect = db_config["dialect"]
    user = db_config["user"]
    password = db_config["password"]
    host = db_config["host"]
    port = db_config["port"]
    if "oracle" in dialect:
        return f"{dialect}://{user}:{password}@{host}:{port}/{db_config['service']}"
    return f"{dialect}://{user}:{password}@{host}:{port}/{db_config['database']}"


def _parse_psql_csv(text: str) -> tuple[list[str], list[tuple]]:
    """Parse `psql --csv` output. First non-empty line is the header.

    Returns ([], []) for empty stdout (e.g. DDL with no result set,
    though the ssh transport is mostly used for SELECTs)."""
    body = text.strip()
    if not body:
        return [], []
    reader = csv.reader(io.StringIO(body))
    rows = list(reader)
    if not rows:
        return [], []
    columns = rows[0]
    data = [tuple(r) for r in rows[1:]]
    return columns, data
