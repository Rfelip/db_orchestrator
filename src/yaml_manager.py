import logging
from datetime import datetime
from pathlib import Path

from ruamel.yaml import YAML

from src.sql_catalog import SqlCatalog
from src.types import ManifestConfig

log = logging.getLogger(__name__)

CATALOG_FILENAME = "sql-catalog.yaml"
"""Filename the manifest loader looks for, in priority order:
the manifest's own directory, then the repo root (cwd)."""

class YamlManager:
    """
    Manages reading and updating the YAML manifest file while preserving
    comments and structure using ruamel.yaml.
    """

    def __init__(self, manifest_path):
        """
        Initialize the manager with the path to the manifest file.
        
        Args:
            manifest_path (str): Path to the manifest.yaml file.
        """
        self.manifest_path = Path(manifest_path)
        self.yaml = YAML()
        self.yaml.preserve_quotes = True

    def load_manifest(self) -> ManifestConfig:
        """Load and validate the YAML manifest into a typed `ManifestConfig`.

        Validation runs at parse time: unknown step keys, missing
        required fields, and invalid `type` / `joined_glue` /
        `cleanup_mode` values all raise ValueError before execution
        begins. Existing manifests with only recognised keys remain
        valid without modification.

        Steps using `sql_id:` (instead of `file:`) are resolved against
        a `sql-catalog.yaml` found alongside the manifest or, failing
        that, in the current working directory. If no catalog is found
        and a step uses sql_id, the lookup raises CatalogError. Steps
        that use `file:` directly are unaffected.

        `disable_step` still reads/writes the raw YAML directly to
        preserve comments — this typed view is for the executor and any
        downstream tool, not for round-tripping back to disk.
        """
        if not self.manifest_path.exists():
            log.error(f"Manifest file not found at {self.manifest_path}")
            raise FileNotFoundError(f"Manifest file not found at {self.manifest_path}")

        with open(self.manifest_path, 'r', encoding='utf-8') as f:
            raw = self.yaml.load(f)
        plain = _to_plain(raw)
        catalog = self._load_catalog()
        return ManifestConfig.from_dict(plain or {}, catalog=catalog)

    def _load_catalog(self) -> SqlCatalog:
        """Look for `sql-catalog.yaml` next to the manifest, then in
        cwd. Returns an empty catalog if neither exists — sql_id
        resolution will then fail loudly, but manifests using `file:`
        directly continue to work."""
        candidates = [
            self.manifest_path.parent / CATALOG_FILENAME,
            Path.cwd() / CATALOG_FILENAME,
        ]
        for path in candidates:
            if path.exists():
                log.info(f"Loaded SQL catalog from {path}")
                return SqlCatalog.from_yaml(path)
        return SqlCatalog.empty()

    def disable_step(self, step_name):
        """
        Finds a step by name, sets enabled: false, adds a completion comment,
        and saves the file in place.

        Args:
            step_name (str): The name of the step to disable.
        """
        try:
            # Read, Modify, Write pattern to ensure we rely on the latest file state
            with open(self.manifest_path, 'r', encoding='utf-8') as f:
                data = self.yaml.load(f)

            steps = data.get('steps', [])
            step_found = False

            for step in steps:
                if step.get('name') == step_name:
                    if step.get('enabled') is False:
                        log.info(f"Step '{step_name}' is already disabled.")
                        return

                    step['enabled'] = False
                    
                    # Add a comment indicating completion time
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    # ruamel.yaml allows attaching comments to keys. 
                    # We attach it to the 'enabled' key of this specific step.
                    # 1 means comment on the same line (inline) or preceding. 
                    # We usually want a EOL comment or pre-comment. 
                    # Let's try adding a comment object.
                    
                    # Note: accessing the comment object on the CommentedMap
                    # format: step.yaml_add_eol_comment(comment, key)
                    step.yaml_add_eol_comment(f"# Done: {timestamp}", 'enabled')
                    
                    step_found = True
                    break

            if not step_found:
                log.warning(f"Step '{step_name}' not found in manifest.")
                return

            with open(self.manifest_path, 'w', encoding='utf-8') as f:
                self.yaml.dump(data, f)
            
            log.info(f"Step '{step_name}' successfully disabled in manifest.")

        except (OSError, KeyError, AttributeError) as e:
            log.error(f"Failed to update manifest for step '{step_name}': {e}")
            raise


def _to_plain(node):
    """Recursively coerce ruamel CommentedMap/CommentedSeq into dict/list.

    The validation layer (Step.from_dict, ManifestConfig.from_dict)
    accepts any Mapping, but downstream code that pickles, json-dumps,
    or copies the data structure is happier with plain Python types.
    """
    if isinstance(node, dict):
        return {k: _to_plain(v) for k, v in node.items()}
    if isinstance(node, list):
        return [_to_plain(v) for v in node]
    return node