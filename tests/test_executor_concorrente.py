"""Sessoes DuckDB concorrentes para os lotes de `foreach` por balde.

O caminho serial (`concurrency=1`) e o padrao e tem os seus proprios testes; o
que se prova aqui e que o concorrente produz a MESMA coisa e que as tres
invariantes que o tornam seguro estao mesmo valendo:

  1. so lotes cujo UNICO eixo e `bkt` sao paralelizados;
  2. a escrituracao (ledger) sai na ORDEM DA FILA, porque `--resume` le o
     ledger de cima para baixo;
  3. cada sessao tem arquivo de perfil proprio — sem isso duas sessoes
     escreveriam no mesmo `_orch_profile.json` e trocariam os planos entre si
     EM SILENCIO, que e o modo de falha mais caro possivel num repo onde toda
     decisao de desempenho sai desses planos.
"""

import pytest

from src.executor import Executor
from src.plans import PlanStore
from src.transport import DuckDbLocalTransport, DuckDbSettings, _divide_size
from src.types import Step, expand_foreach

pytest.importorskip("duckdb")


def _transport(tmp_path, concurrency):
    return DuckDbLocalTransport(
        helper_path=str(tmp_path / "helper.py"),
        settings=DuckDbSettings(
            memory_limit="1GB",
            threads=2,
            temp_directory=str(tmp_path / "spill"),
            max_temp_directory_size="1GB",
            profile=True,
            concurrency=concurrency,
        ),
    )


def _manifest(tmp_path, out_dir):
    """4 baldes que escrevem um parquet cada, mais um passo por ANO (que NAO
    pode ser paralelizado) e um passo solto."""
    balde = tmp_path / "balde.sql"
    balde.write_text(
        "COPY (SELECT {{ bkt }} AS bkt, count(*) AS n FROM range(100) t(i)) "
        "TO '{{ out }}/b{{ bkt }}.parquet' (FORMAT 'parquet')",
        encoding="utf-8",
    )
    ano = tmp_path / "ano.sql"
    ano.write_text(
        "COPY (SELECT {{ year }} AS year) TO '{{ out }}/y{{ year }}.parquet' "
        "(FORMAT 'parquet')",
        encoding="utf-8",
    )
    solto = tmp_path / "solto.sql"
    solto.write_text(
        "COPY (SELECT 1 AS v) TO '{{ out }}/solto.parquet' (FORMAT 'parquet')",
        encoding="utf-8",
    )
    manifest = tmp_path / "manifest.yaml"
    manifest.write_text(
        f"""
steps:
  - name: solto
    type: sql
    file: "{solto}"
    params: {{ out: "{out_dir}" }}
  - name: fan
    type: sql
    file: "{balde}"
    params: {{ out: "{out_dir}" }}
    foreach: {{ bkt: [0, 1, 2, 3] }}
  - name: porano
    type: sql
    file: "{ano}"
    params: {{ out: "{out_dir}" }}
    foreach: {{ year: [2017, 2018] }}
""",
        encoding="utf-8",
    )
    return manifest


def _roda(tmp_path, concurrency, monkeypatch, ledger=None):
    out_dir = tmp_path / "out"
    out_dir.mkdir(parents=True)
    monkeypatch.chdir(tmp_path)
    plans = PlanStore(tmp_path / "plans", run_id="RUN")
    Executor(
        manifest_path=_manifest(tmp_path, out_dir),
        db_config={},
        notifier_config={},
        force=True,
        duckdb_transport=_transport(tmp_path, concurrency),
        plan_store=plans,
        **({"ledger": ledger} if ledger is not None else {}),
    ).run()
    return out_dir, plans


class TestLoteamento:
    def _fila(self):
        base = Step(name="fan", type="sql", file="x.sql", foreach={"bkt": [0, 1, 2]})
        ano = Step(name="ano", type="sql", file="y.sql", foreach={"year": [2017, 2018]})
        prod = Step(
            name="prod", type="sql", file="z.sql", foreach={"bkt": [0, 1], "year": [9]}
        )
        solto = Step(name="solto", type="sql", file="w.sql")
        return (
            [solto] + expand_foreach(base) + expand_foreach(ano) + expand_foreach(prod)
        )

    def test_so_o_eixo_bkt_puro_vira_lote_paralelo(self):
        lotes = Executor._lotes_por_balde(self._fila())
        paralelos = [(p, [s.name for s in ss]) for p, ss in lotes if p]
        assert paralelos == [(True, ["fan_bkt0", "fan_bkt1", "fan_bkt2"])]

    def test_foreach_por_ano_nao_e_paralelizado(self):
        lotes = Executor._lotes_por_balde(self._fila())
        anos = [s.name for p, ss in lotes if not p for s in ss if "ano" in s.name]
        assert anos == ["ano_year2017", "ano_year2018"]

    def test_produto_bkt_x_year_nao_e_paralelizado(self):
        """`bkt` esta entre os eixos, mas nao sozinho: passo por ano nao tem
        prova de independencia, entao o produto fica serial."""
        lotes = Executor._lotes_por_balde(self._fila())
        assert all(
            not p for p, ss in lotes if any(s.name.startswith("prod") for s in ss)
        )

    def test_a_ordem_da_fila_e_preservada(self):
        lotes = Executor._lotes_por_balde(self._fila())
        achatado = [s.name for _, ss in lotes for s in ss]
        assert achatado == [s.name for s in self._fila()]


class TestMemoriaPorWorker:
    def test_concurrency_1_nao_mexe_em_nada(self):
        s = DuckDbSettings(memory_limit="16GB", profile=True)
        assert s.for_slot(0) is s
        assert s.for_slot(0).slot == ""

    def test_o_teto_e_o_orcamento_do_run_e_se_divide(self):
        s = DuckDbSettings(memory_limit="16GB", profile=True, concurrency=2)
        assert s.for_slot(0).memory_limit == "8192MiB"
        assert s.for_slot(1).memory_limit == "8192MiB"

    def test_cada_worker_tem_arquivo_de_perfil_proprio(self):
        s = DuckDbSettings(memory_limit="16GB", profile=True, concurrency=3)
        assert len({s.for_slot(i).slot for i in range(3)}) == 3

    def test_divisao_nunca_chega_a_zero(self):
        """'1GB' / 3 em GB daria '0GB', que o DuckDB aceita como zero e derruba
        a sessao na primeira alocacao."""
        assert _divide_size("1GB", 3) == "341MiB"
        assert _divide_size("1MB", 999) == "1MiB"


class TestExecucaoConcorrente:
    def test_saida_identica_a_serial(self, tmp_path, monkeypatch):
        serial, _ = _roda(tmp_path / "s", 1, monkeypatch)
        conc, _ = _roda(tmp_path / "c", 3, monkeypatch)
        assert sorted(p.name for p in serial.glob("*.parquet")) == sorted(
            p.name for p in conc.glob("*.parquet")
        )
        assert sorted(p.name for p in conc.glob("*.parquet")) == [
            "b0.parquet",
            "b1.parquet",
            "b2.parquet",
            "b3.parquet",
            "solto.parquet",
            "y2017.parquet",
            "y2018.parquet",
        ]

    def test_todo_passo_tem_plano_capturado(self, tmp_path, monkeypatch):
        """Se as sessoes dividissem um `_orch_profile.json`, os planos sairiam
        trocados ou faltando — e sem erro nenhum."""
        _, plans = _roda(tmp_path, 3, monkeypatch)
        capturados = {p.step for p in plans.summary().steps}
        for esperado in ("fan_bkt0", "fan_bkt1", "fan_bkt2", "fan_bkt3", "solto"):
            assert esperado in capturados, f"faltou plano de {esperado}"

    def test_o_ledger_sai_na_ordem_da_fila(self, tmp_path, monkeypatch):
        """`--resume` le o ledger de cima para baixo: gravar fora de ordem faria
        uma retomada parar no lugar errado.

        E o LEDGER que precisa dessa garantia, e ele vem de
        `_post_process_step`, que o executor concorrente chama depois da
        barreira, na ordem da fila — nao das threads."""
        import json

        from src.ledger import RunLedger

        registro = RunLedger(tmp_path / "ledgers", run_id="RUN")
        _roda(tmp_path, 3, monkeypatch, ledger=registro)
        nomes = [
            json.loads(linha)["step"]
            for linha in registro.path.read_text(encoding="utf-8").splitlines()
            if linha.strip()
        ]
        baldes = [n for n in nomes if n.startswith("fan_")]
        assert baldes == ["fan_bkt0", "fan_bkt1", "fan_bkt2", "fan_bkt3"]

    def test_a_ordem_do_plano_e_de_CONCLUSAO_nao_de_fila(self, tmp_path, monkeypatch):
        """⚠ Contrato explicito, para ninguem se apoiar no contrario.

        O `seq` do PlanStore e atribuido quando a sessao grava o perfil, isto e,
        quando o statement TERMINA — com N sessoes isso e ordem de conclusao,
        nao de fila. E inofensivo (toda analise agrupa por NOME do passo, nao
        por seq), mas quem escrever um script novo tem de saber. O ledger, esse
        sim, e ordenado — ver o teste acima."""
        _, plans = _roda(tmp_path, 3, monkeypatch)
        capturados = {p.step for p in plans.summary().steps}
        assert {"fan_bkt0", "fan_bkt1", "fan_bkt2", "fan_bkt3"} <= capturados
