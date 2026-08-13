"""Tests for the typed step contract — validation behaviour at parse time."""

import pytest

from src.types import ManifestConfig, Step, expand_foreach


class TestStepFromDict:
    def test_minimal_step_parses(self):
        s = Step.from_dict({"name": "x", "type": "sql"})
        assert s.name == "x"
        assert s.type == "sql"
        assert s.enabled is True
        assert s.params == {}

    def test_full_step_parses(self):
        s = Step.from_dict(
            {
                "name": "load_data",
                "type": "psql",
                "file": "scripts/load.sql",
                "params": {"region": "BR"},
                "transaction_group": "tg1",
                "joined_group": "jg1",
                "joined_glue": "raw",
                "cleanup_target": "staging.x",
                "cleanup_mode": "truncate",
                "profile": True,
                "notify": True,
                "ping_on_end": "U1",
                "ping_on_error": "U2",
                "description": "Load staging data",
                "output_file": "out.csv",
                "enabled": False,
            }
        )
        assert s.name == "load_data"
        assert s.params == {"region": "BR"}
        assert s.cleanup_mode == "truncate"
        assert s.joined_glue == "raw"
        assert s.profile is True
        assert s.enabled is False

    def test_unknown_key_raises(self):
        # The whole point of Phase B: misspellings die at load, not at
        # the step that needed the missing field.
        with pytest.raises(ValueError, match="unknown keys"):
            Step.from_dict({"name": "x", "type": "sql", "fielo": "typo"})

    def test_missing_name_raises(self):
        with pytest.raises(ValueError, match="missing required 'name'"):
            Step.from_dict({"type": "sql"})

    def test_missing_type_raises(self):
        with pytest.raises(ValueError, match="missing required 'type'"):
            Step.from_dict({"name": "x"})

    def test_invalid_type_raises(self):
        with pytest.raises(ValueError, match="not one of"):
            Step.from_dict({"name": "x", "type": "selectish"})

    def test_invalid_joined_glue_raises(self):
        with pytest.raises(ValueError, match="joined_glue"):
            Step.from_dict(
                {
                    "name": "x",
                    "type": "psql",
                    "joined_glue": "weird",
                }
            )

    def test_invalid_cleanup_mode_raises(self):
        with pytest.raises(ValueError, match="cleanup_mode"):
            Step.from_dict(
                {
                    "name": "x",
                    "type": "sql",
                    "cleanup_mode": "explode",
                }
            )

    def test_step_is_frozen(self):
        s = Step.from_dict({"name": "x", "type": "sql"})
        with pytest.raises((AttributeError, TypeError)):
            s.name = "y"  # type: ignore[misc]


class TestManifestConfigFromDict:
    def test_empty_steps_ok(self):
        m = ManifestConfig.from_dict({"steps": []})
        assert m.steps == []

    def test_no_steps_key_treated_as_empty(self):
        m = ManifestConfig.from_dict({})
        assert m.steps == []

    def test_validates_each_step(self):
        with pytest.raises(ValueError, match="unknown keys"):
            ManifestConfig.from_dict(
                {
                    "steps": [
                        {"name": "good", "type": "sql"},
                        {"name": "bad", "type": "sql", "unknown": True},
                    ],
                }
            )

    def test_steps_must_be_list(self):
        with pytest.raises(ValueError, match="'steps' must be a list"):
            ManifestConfig.from_dict({"steps": "oops"})

    def test_foreach_expands_at_manifest_load(self):
        m = ManifestConfig.from_dict(
            {
                "steps": [
                    {"name": "before", "type": "sql"},
                    {"name": "fan", "type": "sql", "foreach": {"bkt": [0, 1, 2]}},
                    {"name": "after", "type": "sql"},
                ],
            }
        )
        assert [s.name for s in m.steps] == [
            "before",
            "fan_bkt0",
            "fan_bkt1",
            "fan_bkt2",
            "after",
        ]
        assert all(s.foreach == {} for s in m.steps)


class TestForeach:
    """The declared-repetition construct: single axis, cross product,
    and the validation that keeps `foreach` from loosening the schema."""

    def test_single_axis_list(self):
        steps = expand_foreach(
            Step.from_dict(
                {
                    "name": "load",
                    "type": "sql",
                    "params": {"out": "/data"},
                    "foreach": {"bkt": [0, 1, 15]},
                }
            )
        )
        assert [s.name for s in steps] == ["load_bkt00", "load_bkt01", "load_bkt15"]
        assert [s.params["bkt"] for s in steps] == [0, 1, 15]
        # Base params survive alongside the axis value.
        assert all(s.params["out"] == "/data" for s in steps)

    def test_zero_padding_follows_widest_value(self):
        one_digit = expand_foreach(
            Step.from_dict({"name": "s", "type": "sql", "foreach": {"n": [1, 2]}})
        )
        assert [s.name for s in one_digit] == ["s_n1", "s_n2"]
        four_digit = expand_foreach(
            Step.from_dict(
                {"name": "s", "type": "sql", "foreach": {"year": [2017, 2018]}}
            )
        )
        assert [s.name for s in four_digit] == ["s_year2017", "s_year2018"]

    def test_cross_product_last_axis_fastest(self):
        steps = expand_foreach(
            Step.from_dict(
                {
                    "name": "qx",
                    "type": "sql",
                    "foreach": {"bkt": [0, 1], "year": [2017, 2018, 2019]},
                }
            )
        )
        assert len(steps) == 6
        assert [s.name for s in steps] == [
            "qx_bkt0_year2017",
            "qx_bkt0_year2018",
            "qx_bkt0_year2019",
            "qx_bkt1_year2017",
            "qx_bkt1_year2018",
            "qx_bkt1_year2019",
        ]
        assert steps[4].params == {"bkt": 1, "year": 2018}

    def test_reference_pipeline_shape(self):
        """16 buckets x 9 years — the tábua pipeline's actual fan-out."""
        steps = expand_foreach(
            Step.from_dict(
                {
                    "name": "p",
                    "type": "sql",
                    "foreach": {
                        "bkt": list(range(16)),
                        "year": list(range(2017, 2026)),
                    },
                }
            )
        )
        assert len(steps) == 144
        assert len({s.name for s in steps}) == 144
        assert steps[0].name == "p_bkt00_year2017"
        assert steps[-1].name == "p_bkt15_year2025"

    def test_no_foreach_passes_through(self):
        step = Step.from_dict({"name": "solo", "type": "sql"})
        assert expand_foreach(step) == [step]

    def test_non_integer_values_are_not_padded(self):
        steps = expand_foreach(
            Step.from_dict(
                {"name": "s", "type": "sql", "foreach": {"sexo": ["M", "F"]}}
            )
        )
        assert [s.name for s in steps] == ["s_sexoM", "s_sexoF"]

    def test_unknown_key_still_rejected(self):
        # foreach widens the schema by exactly one key, not by any key.
        with pytest.raises(ValueError, match="unknown keys"):
            Step.from_dict({"name": "x", "type": "sql", "for_each": {"bkt": [0]}})
        with pytest.raises(ValueError, match="unknown keys"):
            Step.from_dict(
                {"name": "x", "type": "sql", "foreach": {"bkt": [0]}, "matrix": {}}
            )

    def test_empty_foreach_rejected(self):
        with pytest.raises(ValueError, match="non-empty mapping"):
            Step.from_dict({"name": "x", "type": "sql", "foreach": {}})

    def test_foreach_must_be_a_mapping(self):
        with pytest.raises(ValueError, match="non-empty mapping"):
            Step.from_dict({"name": "x", "type": "sql", "foreach": [0, 1]})

    def test_empty_axis_rejected(self):
        with pytest.raises(ValueError, match="non-empty list"):
            Step.from_dict({"name": "x", "type": "sql", "foreach": {"bkt": []}})

    def test_scalar_axis_rejected(self):
        with pytest.raises(ValueError, match="non-empty list"):
            Step.from_dict({"name": "x", "type": "sql", "foreach": {"bkt": 16}})

    def test_axis_colliding_with_params_rejected(self):
        with pytest.raises(ValueError, match="also set in params"):
            Step.from_dict(
                {
                    "name": "x",
                    "type": "sql",
                    "params": {"bkt": 3},
                    "foreach": {"bkt": [0, 1]},
                }
            )

    def test_axis_name_must_be_an_identifier(self):
        with pytest.raises(ValueError, match="not a valid param name"):
            Step.from_dict({"name": "x", "type": "sql", "foreach": {"a b": [1]}})


class TestProduces:
    """`produces:` is the resume witness — a path template, per expansion."""

    def test_defaults_to_none_meaning_nothing_to_check(self):
        assert Step.from_dict({"name": "x", "type": "sql"}).produces is None

    def test_survives_foreach_expansion_unrendered(self):
        # Rendering happens at the point of use, with the merged params,
        # so each expansion resolves to its own path.
        step = Step.from_dict(
            {
                "name": "qx",
                "type": "sql",
                "produces": "{{ out }}/qx_{{ bkt }}.parquet",
                "params": {"out": "/lake"},
                "foreach": {"bkt": [0, 1]},
            }
        )
        expanded = expand_foreach(step)
        assert [e.produces for e in expanded] == ["{{ out }}/qx_{{ bkt }}.parquet"] * 2
        assert [e.params["bkt"] for e in expanded] == [0, 1]


# ── ${VAR} nos params vem do ambiente (2026-08-01) ────────────────────────────
# As raizes (`out`, `bronze`, `silver`, `oracle`) moravam como literal em cada
# manifesto; agora vem do `.env`. O caso que importa e o NEGATIVO: variavel
# ausente tem de ERRAR, porque cair para string vazia faria
# `${LABMA_OUT}/eventos/x` virar `/eventos/x` e o passo gravaria na raiz do
# sistema de arquivos — erro que so apareceria depois de ja ter escrito.
def _passo(params):
    return {
        "name": "p",
        "type": "sql",
        "file": "x.sql",
        "transaction_group": 1,
        "params": params,
    }


def test_params_expandem_variavel_de_ambiente(monkeypatch):
    monkeypatch.setenv("LABMA_OUT_TESTE", "/mnt/disco/saida")
    s = Step.from_dict(_passo({"out": "${LABMA_OUT_TESTE}"}))
    assert s.params["out"] == "/mnt/disco/saida"


def test_params_expandem_no_meio_da_string(monkeypatch):
    monkeypatch.setenv("R", "/raiz")
    s = Step.from_dict(_passo({"p": "${R}/sub/${R}"}))
    assert s.params["p"] == "/raiz/sub//raiz"


def test_variavel_ausente_erra_em_vez_de_virar_vazio(monkeypatch):
    monkeypatch.delenv("NAO_EXISTE_MESMO", raising=False)
    with pytest.raises(ValueError, match="NAO_EXISTE_MESMO"):
        Step.from_dict(_passo({"out": "${NAO_EXISTE_MESMO}/eventos"}))


def test_variavel_vazia_tambem_erra(monkeypatch):
    monkeypatch.setenv("VAZIA", "")
    with pytest.raises(ValueError, match="VAZIA"):
        Step.from_dict(_passo({"out": "${VAZIA}/eventos"}))


def test_valor_nao_string_passa_intacto(monkeypatch):
    s = Step.from_dict(_passo({"n": 16, "flag": True}))
    assert s.params["n"] == 16 and s.params["flag"] is True


class TestForeachDeAmbiente:
    """Eixo `foreach` vindo de `${VAR}` — o que o manifest_refresh usa para
    receber os anos defasados do mr3.sh sem deixar de ser arquivo versionado."""

    def test_string_expande_e_vira_lista_de_ints(self, monkeypatch):
        monkeypatch.setenv("ANOS_TESTE", "2024 2025")
        steps = expand_foreach(
            Step.from_dict(
                {
                    "name": "silver_at",
                    "type": "sql",
                    "foreach": {"year": "${ANOS_TESTE}"},
                }
            )
        )
        assert [s.name for s in steps] == ["silver_at_year2024", "silver_at_year2025"]
        # ints de verdade: o zero-padding do _suffix depende disso.
        assert [s.params["year"] for s in steps] == [2024, 2025]

    def test_virgulas_tambem_separam(self, monkeypatch):
        monkeypatch.setenv("ANOS_TESTE", "2023,2024, 2025")
        steps = expand_foreach(
            Step.from_dict(
                {"name": "s", "type": "sql", "foreach": {"year": "${ANOS_TESTE}"}}
            )
        )
        assert [s.params["year"] for s in steps] == [2023, 2024, 2025]

    def test_variavel_ausente_erra_alto(self, monkeypatch):
        monkeypatch.delenv("NAO_EXISTE_MESMO", raising=False)
        with pytest.raises(ValueError, match="NAO_EXISTE_MESMO"):
            Step.from_dict(
                {"name": "s", "type": "sql", "foreach": {"year": "${NAO_EXISTE_MESMO}"}}
            )

    def test_variavel_que_expande_para_vazio_erra(self, monkeypatch):
        # `${V}` com V="  " passa pelo _expande_env (não é vazia) mas viraria
        # eixo de zero itens — zero passos rodando em silêncio.
        monkeypatch.setenv("SO_ESPACO", "  ")
        with pytest.raises(ValueError, match="empty"):
            Step.from_dict(
                {"name": "s", "type": "sql", "foreach": {"year": "${SO_ESPACO}"}}
            )

    def test_item_nao_numerico_fica_string(self, monkeypatch):
        monkeypatch.setenv("MISTO", "2024 extra")
        steps = expand_foreach(
            Step.from_dict(
                {"name": "s", "type": "sql", "foreach": {"year": "${MISTO}"}}
            )
        )
        assert [s.params["year"] for s in steps] == [2024, "extra"]
