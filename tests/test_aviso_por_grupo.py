"""A cadência de notificação: um aviso por grupo de transação, e o nome da
tarefa saindo dos nomes dos passos.

Antes de 2026-08-18 o alerta implícito disparava acima de 5 s por passo — 141
mensagens numa execução de 492 passos — e o resumo imprimia `Group 7` no lugar
dos nomes, justamente nos grupos maiores.
"""

from unittest.mock import MagicMock

from src.executor import SEGUNDOS_ALERTA_PASSO, Executor


def nome(*nomes):
    return Executor.nome_do_grupo(nomes)


class TestNomeDoGrupo:
    def test_leque_de_foreach_por_ano_mostra_o_eixo(self):
        assert nome(*[f"silver_at_year{a}" for a in range(2005, 2026)]) == (
            "silver_at_year 2005…2025"
        )

    def test_leque_por_balde_nao_corta_no_meio_do_numero(self):
        assert nome(*[f"pessoa_resumo_bkt{i:02d}" for i in range(16)]) == (
            "pessoa_resumo_bkt 00…15"
        )

    def test_passo_unico_e_o_proprio_nome(self):
        assert nome("eventos_base") == "eventos_base"

    def test_sem_prefixo_comum_cai_para_o_par(self):
        assert nome("dim_empresas", "dim_subpops", "correction_state") == (
            "dim_empresas … correction_state"
        )

    def test_prefixo_curto_demais_nao_identifica(self):
        assert nome("a1", "b2") == "a1 … b2"

    def test_grupo_vazio_nao_estoura(self):
        assert nome() == "(vazio)"


class TestCadencia:
    def test_o_corte_do_alerta_por_passo_e_cinco_minutos(self):
        assert SEGUNDOS_ALERTA_PASSO == 300

    def test_avisa_quando_o_grupo_troca(self):
        ex = Executor.__new__(Executor)
        ex.notifier = MagicMock()
        executados = [
            {"name": "silver_at_year2005", "duration": 10.0, "group": 5},
            {"name": "silver_at_year2006", "duration": 12.0, "group": 5},
            {"name": "pessoas_base", "duration": 58.0, "group": 6},
        ]
        ex._avisa_grupo_fechado(executados)
        ex.notifier.send_alert.assert_called_once()
        assunto, corpo = ex.notifier.send_alert.call_args[0]
        assert assunto == "Grupo concluído"
        assert "silver_at_year 2005…2006" in corpo
        assert "2 passos" in corpo
        assert "22.0s" in corpo

    def test_nao_avisa_dentro_do_mesmo_grupo(self):
        ex = Executor.__new__(Executor)
        ex.notifier = MagicMock()
        ex._avisa_grupo_fechado(
            [
                {"name": "a", "duration": 1.0, "group": 5},
                {"name": "b", "duration": 1.0, "group": 5},
            ]
        )
        ex.notifier.send_alert.assert_not_called()

    def test_o_ultimo_grupo_e_fechado_no_fim_do_run(self):
        ex = Executor.__new__(Executor)
        ex.notifier = MagicMock()
        ex.avisa_ultimo_grupo(
            [
                {"name": "sim_match", "duration": 14.0, "group": 24},
                {"name": "cnis_sim_enriquecido", "duration": 3.0, "group": 25},
            ]
        )
        _, corpo = ex.notifier.send_alert.call_args[0]
        assert "cnis_sim_enriquecido" in corpo
        assert "1 passo" in corpo

    def test_fila_vazia_nao_avisa_nada(self):
        ex = Executor.__new__(Executor)
        ex.notifier = MagicMock()
        ex.avisa_ultimo_grupo([])
        ex.notifier.send_alert.assert_not_called()
