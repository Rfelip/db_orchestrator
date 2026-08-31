"""O rastro é do RUN: começa limpo e, se o run passou, termina limpo.

`--rearmar` é simétrico de propósito. Na entrada, para o sucesso de ontem
não virar o plano de hoje. Na saída, e SÓ quando o run passou, para a
árvore ficar limpa: o `pull_main` da cadeia de deploy recusa puxar sobre
árvore suja, então um rastro deixado para trás trava a execução SEGUINTE
-- que, com o vigia de 12 em 12 horas, é automática e ninguém está olhando.

Numa FALHA o rastro fica: é o que `--retomar` lê, e é onde se enxerga até
onde o plano chegou.
"""

import sys
from pathlib import Path

import pytest

import main as orquestrador
from src.yaml_manager import YamlManager

MANIFESTO = """\
steps:
  - name: um
    type: duckdb
  - name: dois
    type: duckdb
"""


@pytest.fixture
def manifesto(tmp_path, monkeypatch):
    caminho = tmp_path / "manifest.yaml"
    caminho.write_text(MANIFESTO, encoding="utf-8")
    monkeypatch.setattr(
        orquestrador, "load_settings", lambda: {"db": {}, "notifier": {}}, raising=False
    )
    return caminho


def roda(monkeypatch, manifesto, *, falha: bool):
    """Roda o main() com o executor trocado por um que marca os passos."""

    def fake_run_manifest(caminho, **kwargs):
        gerente = YamlManager(caminho)
        gerente.disable_step("um")
        if falha:
            raise RuntimeError("passo dois explodiu")
        gerente.disable_step("dois")

    monkeypatch.setattr(orquestrador, "run_manifest", fake_run_manifest)
    monkeypatch.setattr(
        sys, "argv", ["main.py", "--manifest", str(manifesto), "--force", "--rearmar"]
    )
    try:
        orquestrador.main()
    except SystemExit as e:
        return e.code
    return 0


def desarmados(caminho: Path) -> int:
    return caminho.read_text(encoding="utf-8").count("enabled: false")


class TestRearmeNoFim:
    def test_run_que_passa_deixa_a_arvore_limpa(self, monkeypatch, manifesto):
        codigo = roda(monkeypatch, manifesto, falha=False)
        assert codigo == 0
        assert desarmados(manifesto) == 0
        assert manifesto.read_text(encoding="utf-8") == MANIFESTO

    def test_run_que_falha_preserva_o_rastro(self, monkeypatch, manifesto):
        codigo = roda(monkeypatch, manifesto, falha=True)
        assert codigo == 1
        # `um` terminou antes de `dois` explodir: o rastro dele fica, e é o
        # que diz por onde `--retomar` recomeça.
        assert desarmados(manifesto) == 1
        assert "- name: um" in manifesto.read_text(encoding="utf-8")

    def test_sem_rearmar_o_rastro_fica_nos_dois_casos(self, monkeypatch, manifesto):
        """Um run com escopo (`--modulo`, `--retomar`) não passa `--rearmar`
        e não é dono do rastro: não limpa nem na entrada nem na saída."""

        def fake_run_manifest(caminho, **kwargs):
            YamlManager(caminho).disable_step("um")

        monkeypatch.setattr(orquestrador, "run_manifest", fake_run_manifest)
        monkeypatch.setattr(
            sys, "argv", ["main.py", "--manifest", str(manifesto), "--force"]
        )
        try:
            orquestrador.main()
        except SystemExit:
            pass
        assert desarmados(manifesto) == 1
