"""O manifesto como rastro da execução, e o rearme que o zera.

O YAML guarda o que a execução terminou, para que uma execução seguinte
retome de onde parou. Duas coisas quebravam isso:

- `disable_step` casa nomes DECLARADOS, e as expansões de um `foreach` não
  existem no arquivo. Só 59 dos 116 passos declarados eram alcançáveis, e
  eram exatamente os módulos 04 e 05 (Tábuas e Marts), os únicos sem
  `foreach`. Toda execução completa terminava desarmando a Tábua e os
  marts, e a seguinte os pulava em silêncio.
- Nada rearmava o manifesto. O sucesso de ontem virava o plano de hoje.
"""

from collections import Counter
from pathlib import Path
from unittest.mock import MagicMock

from ruamel.yaml import YAML

from src.executor import Executor
from src.types import Step
from src.yaml_manager import YamlManager


def passo(nome, origem=None):
    return Step(name=nome, type="duckdb", foreach_origin=origem)


def executor_com(expansoes):
    """Um Executor com só o que o rastro no manifesto usa."""
    ex = Executor.__new__(Executor)
    ex.yaml_manager = MagicMock()
    ex._expansoes_por_origem = expansoes
    ex._concluidas_por_origem = Counter()
    return ex


class TestRastroDeForeach:
    def test_passo_sem_foreach_se_desarma_sozinho(self):
        ex = executor_com({})
        ex._marca_no_manifesto(passo("tabua_final"))
        ex.yaml_manager.disable_step.assert_called_once_with("tabua_final")

    def test_expansao_sozinha_nao_desarma_o_pai(self):
        ex = executor_com({"pessoa_resumo": 16})
        ex._marca_no_manifesto(passo("pessoa_resumo_bkt00", origem="pessoa_resumo"))
        ex.yaml_manager.disable_step.assert_not_called()

    def test_o_pai_desarma_quando_a_ultima_expansao_termina(self):
        ex = executor_com({"pessoa_resumo": 16})
        for i in range(16):
            ex._marca_no_manifesto(
                passo(f"pessoa_resumo_bkt{i:02d}", origem="pessoa_resumo")
            )
        ex.yaml_manager.disable_step.assert_called_once_with("pessoa_resumo")

    def test_leque_incompleto_deixa_o_pai_armado(self):
        """Uma execução que morreu no meio do leque não pode voltar
        acreditando que o leque inteiro terminou."""
        ex = executor_com({"pessoa_resumo": 16})
        for i in range(15):
            ex._marca_no_manifesto(
                passo(f"pessoa_resumo_bkt{i:02d}", origem="pessoa_resumo")
            )
        ex.yaml_manager.disable_step.assert_not_called()

    def test_dois_leques_nao_se_contaminam(self):
        ex = executor_com({"a": 2, "b": 2})
        ex._marca_no_manifesto(passo("a_bkt00", origem="a"))
        ex._marca_no_manifesto(passo("b_bkt00", origem="b"))
        ex.yaml_manager.disable_step.assert_not_called()
        ex._marca_no_manifesto(passo("a_bkt01", origem="a"))
        ex.yaml_manager.disable_step.assert_called_once_with("a")


class TestRearme:
    def escreve(self, tmp_path: Path, dados) -> YamlManager:
        caminho = tmp_path / "manifest.yaml"
        with open(caminho, "w", encoding="utf-8") as f:
            YAML().dump(dados, f)
        return YamlManager(caminho)

    def le(self, gerente: YamlManager):
        with open(gerente.manifest_path, encoding="utf-8") as f:
            return YAML().load(f)

    def test_rearme_liga_tudo_e_conta(self, tmp_path):
        gerente = self.escreve(
            tmp_path,
            {
                "steps": [
                    {"name": "a", "type": "duckdb", "enabled": False},
                    {"name": "b", "type": "duckdb", "enabled": False},
                    {"name": "c", "type": "duckdb"},
                ]
            },
        )
        assert gerente.enable_all_steps() == 2
        passos = self.le(gerente)["steps"]
        assert all(p.get("enabled", True) is True for p in passos)

    def test_rearme_de_manifesto_ja_armado_nao_conta_nada(self, tmp_path):
        gerente = self.escreve(tmp_path, {"steps": [{"name": "a", "type": "duckdb"}]})
        assert gerente.enable_all_steps() == 0

    def test_rearme_apaga_o_carimbo_de_conclusao(self, tmp_path):
        """`disable_step` deixa um `# Done: <hora>` colado no `enabled`.
        Rearmar sem tirar o carimbo deixaria o arquivo mentindo sobre a
        execução anterior."""
        gerente = self.escreve(tmp_path, {"steps": [{"name": "a", "type": "duckdb"}]})
        gerente.disable_step("a")
        assert "# Done:" in gerente.manifest_path.read_text(encoding="utf-8")
        gerente.enable_all_steps()
        assert "# Done:" not in gerente.manifest_path.read_text(encoding="utf-8")


class TestSilencioNoMeio:
    def test_grupo_concluido_nao_vai_mais_para_o_discord(self, caplog):
        """A cadência por grupo continua no stdout, que é o que um monitor
        segue. O canal do Discord fica com começo e fim."""
        ex = Executor.__new__(Executor)
        ex.notifier = MagicMock()
        with caplog.at_level("INFO"):
            ex.avisa_ultimo_grupo(
                [{"name": "pessoas_base", "duration": 58.0, "group": 6}]
            )
        ex.notifier.send_alert.assert_not_called()
        assert "[grupo] pessoas_base — 1 passo em 58.0s" in caplog.text


class TestResumoFinal:
    def test_sucesso_traz_so_o_veredicto_e_a_duracao(self):
        ex = Executor.__new__(Executor)
        corpo = ex._resumo_de_sucesso(5390.39)
        assert "Job finished successfully." in corpo
        assert "5390.39s" in corpo
        assert "Executed tasks" not in corpo


MANIFESTO_COM_FORMATACAO = """\
anos: &anos
  [2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016,
   2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]

steps:
  - name: silver_at
    type: duckdb
    params: { <<: *raizes }
    foreach: { year: *anos }
    description: "Uma linha longa que o dump do ruamel dobraria em 80 colunas se
      alguem reserializasse o arquivo inteiro em vez de mexer numa linha so"

  - name: tabua_final
    type: duckdb
    params: { <<: *raizes }
"""


class TestDiffMinimo:
    """O manifesto é fonte E rastro: um humano e o git leem o diff entre
    execuções, e a cadeia de deploy recusa puxar sobre uma árvore suja. Um
    write que reformata o que não tocou some com as duas coisas.
    """

    def manifesto(self, tmp_path: Path) -> YamlManager:
        caminho = tmp_path / "manifest.yaml"
        caminho.write_text(MANIFESTO_COM_FORMATACAO, encoding="utf-8")
        return YamlManager(caminho)

    def test_desarmar_um_passo_mexe_em_uma_linha_so(self, tmp_path):
        gerente = self.manifesto(tmp_path)
        antes = MANIFESTO_COM_FORMATACAO.splitlines()
        gerente.disable_step("tabua_final")
        depois = gerente.manifest_path.read_text(encoding="utf-8").splitlines()
        novas = [l for l in depois if l not in antes]
        assert len(depois) == len(antes) + 1
        assert len(novas) == 1
        assert novas[0].strip().startswith("enabled: false  # Done:")

    def test_a_linha_entra_no_bloco_do_passo_pedido(self, tmp_path):
        gerente = self.manifesto(tmp_path)
        gerente.disable_step("silver_at")
        linhas = gerente.manifest_path.read_text(encoding="utf-8").splitlines()
        i = linhas.index("  - name: silver_at")
        assert linhas[i + 1].strip().startswith("enabled: false")

    def test_desarmar_e_rearmar_devolve_o_arquivo_intacto(self, tmp_path):
        gerente = self.manifesto(tmp_path)
        for nome in ("silver_at", "tabua_final"):
            gerente.disable_step(nome)
        assert gerente.enable_all_steps() == 2
        # Espaçamento do flow, dobra da lista e da descrição, tudo de pé.
        assert (
            gerente.manifest_path.read_text(encoding="utf-8")
            == MANIFESTO_COM_FORMATACAO
        )

    def test_passo_ausente_nao_escreve_nada(self, tmp_path):
        gerente = self.manifesto(tmp_path)
        gerente.disable_step("passo_que_nao_existe")
        assert (
            gerente.manifest_path.read_text(encoding="utf-8")
            == MANIFESTO_COM_FORMATACAO
        )

    def test_desarmar_duas_vezes_nao_duplica_a_linha(self, tmp_path):
        gerente = self.manifesto(tmp_path)
        gerente.disable_step("tabua_final")
        gerente.disable_step("tabua_final")
        texto = gerente.manifest_path.read_text(encoding="utf-8")
        assert texto.count("enabled: false") == 1
