import unittest
from unittest.mock import MagicMock, patch, mock_open
import sys
from pathlib import Path
from src.executor import Executor

class TestExecutor(unittest.TestCase):
    def setUp(self):
        self.manifest_path = "dummy_manifest.yaml"
        self.db_config = {
            'dialect': 'sqlite',
            'user': 'user',
            'password': 'password',
            'host': 'localhost',
            'port': '5432',
            'service': 'db'
        }
        self.notifier_config = {
            'token': '123',
            'chat_id': '456'
        }

    @patch('src.executor.YamlManager')
    @patch('src.executor.Notifier')
    @patch('src.executor.DatabaseManager')
    def test_init(self, MockDB, MockNotifier, MockYaml):
        executor = Executor(self.manifest_path, self.db_config, self.notifier_config)
        self.assertIsNotNone(executor)
        MockYaml.assert_called_with(self.manifest_path)
        MockNotifier.assert_called()

    @patch('src.executor.YamlManager')
    @patch('src.executor.Notifier')
    @patch('src.executor.DatabaseManager')
    def test_run_no_steps(self, MockDB, MockNotifier, MockYaml):
        # Setup mock manifest
        mock_yaml_instance = MockYaml.return_value
        mock_yaml_instance.load_manifest.return_value = {'steps': []}

        executor = Executor(self.manifest_path, self.db_config, self.notifier_config)
        
        # We need to capture logs or just ensure it returns early.
        # Since it returns None, we just check that DB wasn't initialized
        executor.run()
        
        MockDB.assert_not_called()

    @patch('src.executor.YamlManager')
    @patch('src.executor.Notifier')
    @patch('src.executor.DatabaseManager')
    @patch('builtins.input', return_value='y')
    @patch('src.executor.SQLParser')
    @patch('src.executor.render_template')
    def test_run_sql_step_success(self, mock_render, mock_parser, mock_input, MockDB, MockNotifier, MockYaml):
        # Setup steps
        steps = [{
            'name': 'Test SQL',
            'file': 'test.sql',
            'type': 'sql',
            'enabled': True,
            'transaction_group': 1,
            'cleanup_target': None
        }]
        mock_yaml_instance = MockYaml.return_value
        mock_yaml_instance.load_manifest.return_value = {'steps': steps}
        
        # Setup DB
        mock_db_instance = MockDB.return_value
        mock_session = MagicMock()
        mock_db_instance.get_session.return_value = mock_session
        
        # Setup Parser
        mock_parser.read_sql_file.return_value = "SELECT 1"
        mock_render.return_value = "SELECT 1"

        executor = Executor(self.manifest_path, self.db_config, self.notifier_config)
        executor.run()
        
        # Verify DB calls
        MockDB.assert_called()
        mock_db_instance.execute_query.assert_called()
        mock_yaml_instance.disable_step.assert_called_with('Test SQL')
        
    @patch('src.executor.YamlManager')
    @patch('src.executor.Notifier')
    @patch('src.executor.DatabaseManager')
    @patch('builtins.input', return_value='n')
    def test_run_user_abort(self, mock_input, MockDB, MockNotifier, MockYaml):
        steps = [{'name': 'S1', 'enabled': True}]
        MockYaml.return_value.load_manifest.return_value = {'steps': steps}
        
        executor = Executor(self.manifest_path, self.db_config, self.notifier_config)
        
        with self.assertRaises(SystemExit) as cm:
            executor.run()
        
        self.assertEqual(cm.exception.code, 0)
        MockDB.assert_not_called()

    @patch('src.executor.YamlManager')
    @patch('src.executor.Notifier')
    @patch('src.executor.DatabaseManager')
    @patch('builtins.input', return_value='y')
    @patch('src.executor.subprocess.run')
    def test_run_python_step(self, mock_subprocess, mock_input, MockDB, MockNotifier, MockYaml):
         steps = [{
            'name': 'Test Py',
            'file': 'script.py',
            'type': 'python',
            'enabled': True
        }]
         MockYaml.return_value.load_manifest.return_value = {'steps': steps}
         
         # Mock file existence check
         with patch('src.executor.Path.exists', return_value=True):
             executor = Executor(self.manifest_path, self.db_config, self.notifier_config)
             executor.run()
             
             mock_subprocess.assert_called()
             MockYaml.return_value.disable_step.assert_called_with('Test Py')

if __name__ == '__main__':
    unittest.main()
