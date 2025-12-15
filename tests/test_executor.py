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
    @patch('src.executor.Reporter')
    def test_init(self, MockReporter, MockDB, MockNotifier, MockYaml):
        executor = Executor(self.manifest_path, self.db_config, self.notifier_config)
        self.assertIsNotNone(executor)
        MockYaml.assert_called_with(self.manifest_path)
        MockNotifier.assert_called()
        MockReporter.assert_called()

    @patch('src.executor.YamlManager')
    @patch('src.executor.Notifier')
    @patch('src.executor.DatabaseManager')
    @patch('src.executor.Reporter')
    def test_run_no_steps(self, MockReporter, MockDB, MockNotifier, MockYaml):
        # Setup mock manifest
        mock_yaml_instance = MockYaml.return_value
        mock_yaml_instance.load_manifest.return_value = {'steps': []}

        executor = Executor(self.manifest_path, self.db_config, self.notifier_config)
        
        executor.run()
        
        MockDB.assert_not_called()

    @patch('src.executor.YamlManager')
    @patch('src.executor.Notifier')
    @patch('src.executor.DatabaseManager')
    @patch('builtins.input', return_value='y')
    @patch('src.executor.SQLParser')
    @patch('src.executor.render_template')
    @patch('src.executor.Reporter')
    @patch('src.executor.OracleMonitorProfiler')
    def test_run_sql_step_success_oracle(self, MockOracleProfiler, MockReporter, mock_render, mock_parser, mock_input, MockDB, MockNotifier, MockYaml):
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

        # Mock Profiler
        mock_profiler_instance = MockOracleProfiler.return_value
        mock_profiler_instance.prepare_query.return_value = "SELECT /*+ MONITOR */ 1"
        mock_profiler_instance.get_metrics.return_value = {}

        # Set dialect to Oracle
        db_config_oracle = self.db_config.copy()
        db_config_oracle['dialect'] = 'oracle+cx_oracle'

        executor = Executor(self.manifest_path, db_config_oracle, self.notifier_config)
        executor.run()
        
        # Verify DB calls
        MockDB.assert_called()
        # Verify profiler usage
        MockOracleProfiler.assert_called()
        mock_profiler_instance.prepare_query.assert_called()
        mock_profiler_instance.post_execution_capture.assert_called()
        # Verify reporter usage
        MockReporter.return_value.add_task_result.assert_called()
        MockReporter.return_value.generate_report.assert_called()
        
        mock_yaml_instance.disable_step.assert_called_with('Test SQL')
        
    @patch('src.executor.YamlManager')
    @patch('src.executor.Notifier')
    @patch('src.executor.DatabaseManager')
    @patch('builtins.input', return_value='n')
    @patch('src.executor.Reporter')
    def test_run_user_abort(self, MockReporter, mock_input, MockDB, MockNotifier, MockYaml):
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
    @patch('src.executor.Reporter')
    def test_run_python_step(self, MockReporter, mock_subprocess, mock_input, MockDB, MockNotifier, MockYaml):
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
             # Reporter should generate report even if only python steps run? 
             # Current implementation calls generate_report at the end of run()
             MockReporter.return_value.generate_report.assert_called()

if __name__ == '__main__':
    unittest.main()