import re
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
        # Every write here rewrites the WHOLE file, so the dump has to land
        # on the formatting the file already uses. With ruamel's defaults it
        # does not: the manifest came back re-indented and re-wrapped, 1082
        # lines becoming 1212, and a diff that should show one changed step
        # showed two thousand lines. That diff is not cosmetic — it leaves
        # the rig's tree permanently dirty, and `pull_main` refuses to pull
        # onto a dirty tree, so a bad round-trip blocks the NEXT run.
        self.yaml.indent(mapping=2, sequence=4, offset=2)
        # Long `description:` values are one line in the manifest and must
        # stay one line; the default 80 columns folds them.
        self.yaml.width = 4096

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

        with open(self.manifest_path, "r", encoding="utf-8") as f:
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

    def enable_all_steps(self) -> int:
        """Re-arm the manifest: drop every `enabled: false` line and the
        `# Done:` stamp on it. Returns how many steps were re-armed.

        `disable_step` makes the manifest a record of what the last run
        finished. That record is the right plan for a *resumed* run and
        the wrong one for a fresh run: without this, yesterday's success
        is read as today's list of work to skip, and a full run silently
        does nothing. The verb that clears it stays explicit, so the
        caller decides which runs start from scratch.

        The line is removed rather than set to `true`: `true` is already
        the default in `Step.from_dict`, and a manifest carrying a
        hundred redundant `enabled: true` lines is noise a reader has to
        discount.
        """
        try:
            linhas = self._linhas()
            mantidas = [l for l in linhas if not _LINHA_DESARMADA.match(l)]
            rearmados = len(linhas) - len(mantidas)
            if rearmados:
                self._escreve(mantidas)
            log.info(f"Manifest re-armed: {rearmados} steps re-enabled.")
            return rearmados

        except OSError as e:
            log.error(f"Failed to re-arm manifest: {e}")
            raise

    def disable_step(self, step_name):
        """Mark one step as finished: `enabled: false`, stamped with the
        time, written into the step's own block.

        Line-based, not a re-dump of the parsed document. This file is
        both source and run tracker: it is read by a person and by git
        between runs, and it is written once per completed step, 116
        times in a full run. Round-tripping it through ruamel reformats
        what it did not touch -- 1082 lines came back as 1212, flow
        mappings lost their inner spaces, long `description:` values
        refolded -- so a diff that should show one step showed two
        thousand lines. That is not cosmetic: it leaves the rig's tree
        dirty in a way nobody can read, and the deploy chain refuses to
        pull onto a dirty tree, so a noisy write blocks the NEXT run.

        Args:
            step_name (str): The name of the step to disable.
        """
        try:
            linhas = self._linhas()
            bloco = _acha_bloco(linhas, step_name)
            if bloco is None:
                log.warning(f"Step '{step_name}' not found in manifest.")
                return
            inicio, fim, recuo = bloco

            for i in range(inicio, fim):
                if not _LINHA_ENABLED.match(linhas[i]):
                    continue
                if _LINHA_DESARMADA.match(linhas[i]):
                    log.info(f"Step '{step_name}' is already disabled.")
                    return
                # An `enabled: true` written by hand: replace it in place
                # rather than leaving the step with two `enabled` keys.
                linhas[i] = self._linha_desarmada(recuo)
                break
            else:
                linhas.insert(inicio + 1, self._linha_desarmada(recuo))

            self._escreve(linhas)
            log.info(f"Step '{step_name}' successfully disabled in manifest.")

        except OSError as e:
            log.error(f"Failed to update manifest for step '{step_name}': {e}")
            raise

    def _linhas(self) -> list[str]:
        return self.manifest_path.read_text(encoding="utf-8").splitlines(keepends=True)

    def _escreve(self, linhas) -> None:
        self.manifest_path.write_text("".join(linhas), encoding="utf-8")

    @staticmethod
    def _linha_desarmada(recuo: str) -> str:
        carimbo = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return f"{recuo}enabled: false  # Done: {carimbo}\n"


_LINHA_NOME = re.compile(r"^(\s*)-(\s+)name:\s*(.+?)\s*$")
_LINHA_ENABLED = re.compile(r"^\s*enabled:\s*\S")
_LINHA_DESARMADA = re.compile(r"^\s*enabled:\s*false\b", re.IGNORECASE)


def _acha_bloco(linhas, step_name):
    """Where one step's block starts and ends, and the indent its keys sit at.

    Returns `(inicio, fim, recuo)` with `inicio` the `- name:` line and
    `fim` exclusive, or None when the name is not in the file. A block
    ends at the next line indented no deeper than the dash that opened
    it, which is the next step or the end of the `steps:` list.
    """
    for i, linha in enumerate(linhas):
        m = _LINHA_NOME.match(linha)
        if m is None or m.group(3).strip("\"'") != step_name:
            continue
        recuo_traco = m.group(1)
        # The keys of this item align with `name`, past the dash and its
        # following spaces.
        recuo = " " * (len(recuo_traco) + 1 + len(m.group(2)))
        for j in range(i + 1, len(linhas)):
            corpo = linhas[j]
            if corpo.strip() and len(corpo) - len(corpo.lstrip()) <= len(recuo_traco):
                return i, j, recuo
        return i, len(linhas), recuo
    return None


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
