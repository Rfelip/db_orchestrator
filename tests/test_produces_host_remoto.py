"""A `produces:` path belongs to the host that writes it.

The orchestrator runs on `deploy` and the DuckDB that writes every parquet
runs on MR3 (`transport=ssh+duckdb`), so `/srv/labma/out` exists only on the
far side. A pre-flight that calls `Path(...).mkdir()` in-process dies with
`Permission denied: '/srv/labma'` before any SQL is sent, which is how the
`cod_emp_control` step failed once dispatch moved to `deploy`.
"""

import shlex
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.executor import Executor
from src.transport import DuckDbLocalTransport, DuckDbSettings, DuckDbSshTransport
from src.types import Step


def ssh_transporte(**kw):
    return DuckDbSshTransport(ssh="mr3-lan", wsl=False, **kw)


def passo(nome, produces):
    return Step(name=nome, type="sql", produces=str(produces))


class TransporteQueRegista:
    """A DuckDbTransport that records what it was asked to prepare."""

    name = "registador"
    paths_are_local = False

    def __init__(self):
        self.settings = DuckDbSettings()
        self.pedidos = []

    def session_command(self):
        return ["true"]

    def ensure_parent(self, path):
        self.pedidos.append(path)

    def existing_paths(self, paths):
        return set()


# ── ensure_parent: the directory is made where the SQL will write ───────
def test_transporte_remoto_nao_toca_no_disco_local(tmp_path):
    longe = tmp_path / "so-no-mr3" / "referencia" / "cod_emp_control.parquet"
    t = TransporteQueRegista()

    Executor._prepare_output_dir(
        SimpleNamespace(duckdb_transport=t), passo("cod_emp_control", longe)
    )

    assert t.pedidos == [str(longe)]
    assert not longe.parent.exists(), "created on the orchestrator's disk"


def test_transporte_local_cria_o_directorio(tmp_path):
    dest = tmp_path / "out" / "referencia" / "x.parquet"
    t = DuckDbLocalTransport(helper_path=str(tmp_path / "helper.py"))

    Executor._prepare_output_dir(SimpleNamespace(duckdb_transport=t), passo("x", dest))

    assert dest.parent.is_dir()


def test_sem_transporte_duckdb_cria_localmente(tmp_path):
    """The SQLAlchemy path keeps the in-process mkdir it has always had."""
    dest = tmp_path / "out" / "tabuas" / "qx.parquet"

    Executor._prepare_output_dir(
        SimpleNamespace(duckdb_transport=None), passo("qx", dest)
    )

    assert dest.parent.is_dir()


def test_ssh_faz_o_mkdir_no_host_remoto():
    t = ssh_transporte(ssh_options=["-o", "BatchMode=yes"])

    with patch("src.transport.subprocess.run") as corrido:
        corrido.return_value = SimpleNamespace(returncode=0, stdout=b"", stderr=b"")
        t.ensure_parent("/srv/labma/out/referencia/cod_emp_control.parquet")

    assert corrido.call_args[0][0] == [
        "ssh", "-o", "BatchMode=yes", "mr3-lan",
        "mkdir", "-p", "/srv/labma/out/referencia",
    ]


def test_ssh_levanta_quando_o_mkdir_falha():
    t = ssh_transporte()

    with patch("src.transport.subprocess.run") as corrido:
        corrido.return_value = SimpleNamespace(
            returncode=1, stdout=b"", stderr=b"mkdir: '/srv/labma': Permission denied"
        )
        with pytest.raises(RuntimeError, match="Permission denied"):
            t.ensure_parent("/srv/labma/out/referencia/x.parquet")


# ── existing_paths: one round trip for the whole window ─────────────────
def test_existing_paths_local_le_o_disco(tmp_path):
    ha = tmp_path / "ha.parquet"
    ha.write_bytes(b"x")
    pasta = tmp_path / "pasta-vazia"
    pasta.mkdir()
    nao = tmp_path / "nao.parquet"
    t = DuckDbLocalTransport(helper_path=str(tmp_path / "helper.py"))

    assert t.existing_paths([str(ha), str(pasta), str(nao)]) == {str(ha), str(pasta)}
    assert t.paths_are_local is True


def test_existing_paths_ssh_gasta_um_so_round_trip():
    t = ssh_transporte()
    caminhos = [
        "/srv/labma/out/a.parquet",
        "/srv/labma/silver/at/at_2025",
        "/srv/labma/out/nao.parquet",
    ]

    with patch("src.transport.subprocess.run") as corrido:
        corrido.return_value = SimpleNamespace(
            returncode=0,
            stdout=b"/srv/labma/out/a.parquet\n/srv/labma/silver/at/at_2025\n",
            stderr=b"",
        )
        got = t.existing_paths(caminhos)

    assert corrido.call_count == 1, "one ssh for the whole window, not one per path"
    argv = corrido.call_args[0][0]
    assert argv[:4] == ["ssh", "mr3-lan", "sh", "-c"]
    # Quoted as one element: ssh joins argv with spaces before the far shell
    # sees it, so an unquoted loop would be re-split into words.
    assert argv[4] == shlex.quote(shlex.split(argv[4])[0])
    # The loop's status is its last command, so without this a final absent
    # path would read as a broken ssh. Cost us one live call to find.
    assert argv[4].rstrip("'").endswith("exit 0")
    # Paths never reach a command line, so the far shell cannot re-split them.
    assert corrido.call_args[1]["input"] == "\n".join(caminhos).encode("utf-8")
    assert got == {"/srv/labma/out/a.parquet", "/srv/labma/silver/at/at_2025"}
    assert t.paths_are_local is False


def test_existing_paths_ssh_vazio_nao_abre_ligacao():
    t = ssh_transporte()

    with patch("src.transport.subprocess.run") as corrido:
        assert t.existing_paths([]) == set()

    assert corrido.call_count == 0


def test_existing_paths_ssh_levanta_quando_falha():
    t = ssh_transporte()

    with patch("src.transport.subprocess.run") as corrido:
        corrido.return_value = SimpleNamespace(
            returncode=255, stdout=b"", stderr=b"ssh: connect: No route to host"
        )
        with pytest.raises(RuntimeError, match="No route to host"):
            t.existing_paths(["/srv/labma/out/a.parquet"])


# ── _step_evidence: known, unknown, absent ──────────────────────────────
def test_evidencia_usa_o_lote():
    ex = SimpleNamespace(duckdb_transport=TransporteQueRegista())
    p = passo("cod_emp_control", "/srv/labma/out/referencia/cod_emp_control.parquet")

    presente = Executor._step_evidence(
        ex, p, existentes={"/srv/labma/out/referencia/cod_emp_control.parquet"}
    )
    ausente = Executor._step_evidence(ex, p, existentes=set())

    assert presente.output_present is True
    assert ausente.output_present is False


def test_evidencia_remota_sem_lote_e_desconhecida():
    """None, not False. The file is on the far host, and `False` would print
    a warning about an output that is there."""
    ex = SimpleNamespace(duckdb_transport=TransporteQueRegista())

    got = Executor._step_evidence(ex, passo("x", "/srv/labma/out/referencia/x.parquet"))

    assert got.output_present is None


def test_evidencia_local_sem_lote_ve_o_disco(tmp_path):
    ha = tmp_path / "ha.parquet"
    ha.write_bytes(b"x")
    t = DuckDbLocalTransport(helper_path=str(tmp_path / "helper.py"))
    ex = SimpleNamespace(duckdb_transport=t)

    assert Executor._step_evidence(ex, passo("ha", ha)).output_present is True
    assert (
        Executor._step_evidence(ex, passo("nao", tmp_path / "nao.parquet")).output_present
        is False
    )


def test_evidencia_sem_produces():
    ex = SimpleNamespace(duckdb_transport=TransporteQueRegista())

    got = Executor._step_evidence(ex, Step(name="seed", type="sql"))

    assert got.output_present is None
