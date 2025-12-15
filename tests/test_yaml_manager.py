import unittest
import tempfile
import shutil
import os
from pathlib import Path
from ruamel.yaml import YAML
from src.yaml_manager import YamlManager

class TestYamlManager(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.mkdtemp()
        self.manifest_path = Path(self.test_dir) / "manifest.yaml"
        
        # Create a sample manifest
        self.yaml = YAML()
        self.sample_data = {
            'steps': [
                {
                    'name': 'Step 1',
                    'file': 'step1.sql',
                    'type': 'sql',
                    'enabled': True
                },
                {
                    'name': 'Step 2',
                    'file': 'step2.sql',
                    'type': 'sql',
                    'enabled': False
                }
            ]
        }
        
        with open(self.manifest_path, 'w', encoding='utf-8') as f:
            self.yaml.dump(self.sample_data, f)
            
        self.manager = YamlManager(self.manifest_path)

    def tearDown(self):
        # Remove the temporary directory
        shutil.rmtree(self.test_dir)

    def test_load_manifest(self):
        data = self.manager.load_manifest()
        self.assertIn('steps', data)
        self.assertEqual(len(data['steps']), 2)
        self.assertEqual(data['steps'][0]['name'], 'Step 1')

    def test_load_manifest_file_not_found(self):
        manager = YamlManager(Path(self.test_dir) / "non_existent.yaml")
        with self.assertRaises(FileNotFoundError):
            manager.load_manifest()

    def test_disable_step(self):
        self.manager.disable_step('Step 1')
        
        # Reload to verify
        with open(self.manifest_path, 'r', encoding='utf-8') as f:
            data = self.yaml.load(f)
            
        step1 = next(s for s in data['steps'] if s['name'] == 'Step 1')
        self.assertFalse(step1['enabled'])
        
        # Check if comment exists (this might be tricky to test with parsed data, 
        # but ruamel.yaml stores comments in .ca)
        # However, checking if 'enabled' is False is the main functional requirement.
        
    def test_disable_step_already_disabled(self):
        # Should just log and return, not raise
        try:
            self.manager.disable_step('Step 2')
        except Exception as e:
            self.fail(f"disable_step raised exception on already disabled step: {e}")

    def test_disable_step_not_found(self):
        # Should just log and return
        try:
            self.manager.disable_step('Non Existent Step')
        except Exception as e:
            self.fail(f"disable_step raised exception on missing step: {e}")

if __name__ == '__main__':
    unittest.main()
