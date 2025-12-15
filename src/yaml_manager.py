import logging
from datetime import datetime
from ruamel.yaml import YAML
from pathlib import Path

log = logging.getLogger(__name__)

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
        # Set indentation: mapping=2, sequence=2, offset=2
        self.yaml.indent(mapping=2, sequence=2, offset=2)

    def load_manifest(self):
        """
        Loads the YAML manifest.

        Returns:
            dict: The parsed YAML content.
        """
        if not self.manifest_path.exists():
            log.error(f"Manifest file not found at {self.manifest_path}")
            raise FileNotFoundError(f"Manifest file not found at {self.manifest_path}")

        try:
            with open(self.manifest_path, 'r', encoding='utf-8') as f:
                data = self.yaml.load(f)
            return data
        except Exception as e:
            log.error(f"Failed to load manifest: {e}")
            raise

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

        except Exception as e:
            log.error(f"Failed to update manifest for step '{step_name}': {e}")
            raise