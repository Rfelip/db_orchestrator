"""Tests for the SQL catalog — parsing, resolution, manifest integration."""
from pathlib import Path

import pytest
from ruamel.yaml import YAML

from src.sql_catalog import CatalogError, SqlCatalog
from src.types import ManifestConfig, Step
from src.yaml_manager import YamlManager


def _write_yaml(path: Path, data: dict) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        YAML().dump(data, fh)


class TestParse:
    def test_minimal_entry(self):
        cat = SqlCatalog.from_dict({
            "sql": [{"id": "x", "file": "scripts/x.sql"}],
        })
        e = cat.resolve("x")
        assert e.sql_id == "x"
        assert e.file == "scripts/x.sql"
        assert e.intent is None
        assert e.read_only is False
        assert e.expected_duration_s is None

    def test_full_entry(self):
        cat = SqlCatalog.from_dict({
            "sql": [{
                "id": "load_staging",
                "file": "scripts/load.sql",
                "intent": "Load raw rows",
                "read_only": False,
                "expected_duration_s": 60,
            }],
        })
        e = cat.resolve("load_staging")
        assert e.intent == "Load raw rows"
        assert e.read_only is False
        assert e.expected_duration_s == 60.0

    def test_missing_id_raises(self):
        with pytest.raises(CatalogError, match="missing 'id'"):
            SqlCatalog.from_dict({"sql": [{"file": "x.sql"}]})

    def test_missing_file_raises(self):
        with pytest.raises(CatalogError, match="missing 'file'"):
            SqlCatalog.from_dict({"sql": [{"id": "x"}]})

    def test_duplicate_id_raises(self):
        with pytest.raises(CatalogError, match="duplicate sql_id"):
            SqlCatalog.from_dict({
                "sql": [
                    {"id": "x", "file": "a.sql"},
                    {"id": "x", "file": "b.sql"},
                ],
            })

    def test_empty_catalog_raises_on_resolve(self):
        cat = SqlCatalog.empty()
        with pytest.raises(CatalogError, match="not found"):
            cat.resolve("anything")


class TestLoadIfExists:
    def test_missing_returns_empty(self):
        cat = SqlCatalog.load_if_exists(Path("/nonexistent/sql-catalog.yaml"))
        assert isinstance(cat, SqlCatalog)
        assert cat.entries == {}

    def test_present_loads(self, tmp_path):
        catalog_path = tmp_path / "sql-catalog.yaml"
        _write_yaml(catalog_path, {
            "sql": [{"id": "demo", "file": "scripts/demo.sql"}],
        })
        cat = SqlCatalog.load_if_exists(catalog_path)
        assert cat.resolve("demo").file == "scripts/demo.sql"


class TestStepFileXorSqlId:
    def test_both_set_raises(self):
        with pytest.raises(ValueError, match="not both"):
            Step.from_dict({
                "name": "x", "type": "sql",
                "file": "a.sql", "sql_id": "registered",
            })

    def test_sql_id_only_ok(self):
        s = Step.from_dict({"name": "x", "type": "sql", "sql_id": "load"})
        assert s.sql_id == "load"
        assert s.file is None

    def test_file_only_ok(self):
        s = Step.from_dict({"name": "x", "type": "sql", "file": "a.sql"})
        assert s.sql_id is None
        assert s.file == "a.sql"


class TestManifestConfigWithCatalog:
    def test_resolves_sql_id_to_file(self):
        cat = SqlCatalog.from_dict({
            "sql": [{"id": "load", "file": "scripts/load.sql"}],
        })
        m = ManifestConfig.from_dict({
            "steps": [{"name": "step1", "type": "sql", "sql_id": "load"}],
        }, catalog=cat)
        assert m.steps[0].file == "scripts/load.sql"
        assert m.steps[0].sql_id is None  # cleared after resolution

    def test_unresolved_sql_id_raises(self):
        cat = SqlCatalog.empty()
        with pytest.raises(CatalogError):
            ManifestConfig.from_dict({
                "steps": [{"name": "x", "type": "sql", "sql_id": "missing"}],
            }, catalog=cat)

    def test_file_steps_pass_through(self):
        cat = SqlCatalog.from_dict({"sql": []})
        m = ManifestConfig.from_dict({
            "steps": [{"name": "x", "type": "sql", "file": "direct.sql"}],
        }, catalog=cat)
        assert m.steps[0].file == "direct.sql"
        assert m.steps[0].sql_id is None


class TestYamlManagerCatalogIntegration:
    def test_loads_catalog_alongside_manifest(self, tmp_path):
        manifest_path = tmp_path / "manifest.yaml"
        catalog_path = tmp_path / "sql-catalog.yaml"
        _write_yaml(catalog_path, {
            "sql": [{"id": "demo", "file": "scripts/demo.sql"}],
        })
        _write_yaml(manifest_path, {
            "steps": [{"name": "s1", "type": "sql", "sql_id": "demo"}],
        })
        manifest = YamlManager(manifest_path).load_manifest()
        assert manifest.steps[0].file == "scripts/demo.sql"

    def test_no_catalog_present_works_for_file_only_manifests(self, tmp_path):
        manifest_path = tmp_path / "manifest.yaml"
        _write_yaml(manifest_path, {
            "steps": [{"name": "s1", "type": "sql", "file": "x.sql"}],
        })
        manifest = YamlManager(manifest_path).load_manifest()
        assert manifest.steps[0].file == "x.sql"

    def test_no_catalog_with_sql_id_step_raises(self, tmp_path, monkeypatch):
        # Make cwd a fresh tmp dir so the cwd-fallback catalog also misses.
        manifest_path = tmp_path / "manifest.yaml"
        _write_yaml(manifest_path, {
            "steps": [{"name": "s1", "type": "sql", "sql_id": "demo"}],
        })
        monkeypatch.chdir(tmp_path)
        # The sql-catalog.yaml the YamlManager will look for doesn't exist.
        with pytest.raises(CatalogError):
            YamlManager(manifest_path).load_manifest()
