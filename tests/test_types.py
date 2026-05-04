"""Tests for the typed step contract — validation behaviour at parse time."""
import pytest

from src.types import Step, ManifestConfig


class TestStepFromDict:
    def test_minimal_step_parses(self):
        s = Step.from_dict({"name": "x", "type": "sql"})
        assert s.name == "x"
        assert s.type == "sql"
        assert s.enabled is True
        assert s.params == {}

    def test_full_step_parses(self):
        s = Step.from_dict({
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
        })
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
            Step.from_dict({
                "name": "x", "type": "psql", "joined_glue": "weird",
            })

    def test_invalid_cleanup_mode_raises(self):
        with pytest.raises(ValueError, match="cleanup_mode"):
            Step.from_dict({
                "name": "x", "type": "sql", "cleanup_mode": "explode",
            })

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
            ManifestConfig.from_dict({
                "steps": [
                    {"name": "good", "type": "sql"},
                    {"name": "bad",  "type": "sql", "unknown": True},
                ],
            })

    def test_steps_must_be_list(self):
        with pytest.raises(ValueError, match="'steps' must be a list"):
            ManifestConfig.from_dict({"steps": "oops"})
