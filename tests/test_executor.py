import unittest
from unittest.mock import MagicMock, patch, mock_open
import sys
from pathlib import Path
from src.executor import Executor, _is_ddl


class TestIsDdl(unittest.TestCase):
    """Cobertura do helper _is_ddl, usado para decidir se cabe wrap em EXPLAIN."""

    def test_create_table_is_ddl(self):
        self.assertTrue(_is_ddl("CREATE TABLE foo (a INT)"))

    def test_drop_table_is_ddl(self):
        self.assertTrue(_is_ddl("DROP TABLE foo"))

    def test_create_with_leading_comment_is_ddl(self):
        self.assertTrue(_is_ddl("-- comentário\nCREATE TABLE foo (a INT)"))

    def test_select_is_not_ddl(self):
        self.assertFalse(_is_ddl("SELECT * FROM foo"))

    def test_insert_is_not_ddl(self):
        self.assertFalse(_is_ddl("INSERT INTO foo VALUES (1)"))

    def test_with_clause_is_not_ddl(self):
        self.assertFalse(_is_ddl("WITH cte AS (SELECT 1) SELECT * FROM cte"))

    def test_truncate_is_ddl(self):
        self.assertTrue(_is_ddl("TRUNCATE TABLE foo"))

    def test_copy_is_ddl(self):
        self.assertTrue(_is_ddl("COPY foo TO '/tmp/x.csv'"))


class TestJoinedGroup(unittest.TestCase):
    """Coverage do joined_group: coalescing + execução fundida via psql."""

    def setUp(self):
        # Não queremos rodar o construtor real — Executor.__init__ tenta carregar
        # YAML/notifier/reporter. Crie uma instância "vazia" via __new__.
        self.executor = Executor.__new__(Executor)
        self.executor.db_config = {
            'dialect': 'postgresql+psycopg2',
            'container_name': 'pgduckdb',
            'user': 'postgres',
            'password': 'x',
            'host': 'localhost',
            'port': '5434',
            'database': 'labma',
            'docker_sudo': True,
        }
        self.executor.reporter = MagicMock()

    def test_joined_group_rejects_mixed_types(self):
        from src.executor import Executor as Exec
        from unittest.mock import patch as _patch
        # _execute_joined_psql_group itself doesn't validate types — that's done
        # by the caller (_run_steps). Test via _run_steps with a stub.
        steps = [
            {'name': 'a', 'type': 'psql',   'file': '/tmp/x.sql', 'enabled': True, 'joined_group': 'g1'},
            {'name': 'b', 'type': 'python', 'file': '/tmp/x.py',  'enabled': True, 'joined_group': 'g1'},
        ]
        ym = MagicMock()
        ym.disable_step = MagicMock()
        notifier = MagicMock()
        self.executor.notifier = notifier
        self.executor.enable_all = True
        with self.assertRaises(RuntimeError) as ctx:
            self.executor._run_steps(steps, MagicMock(), ym, notify=False)
        self.assertIn("must contain only", str(ctx.exception))

    @patch('src.executor.subprocess.run')
    @patch('src.executor.SQLParser.read_sql_file')
    def test_joined_group_concatenates_and_calls_once(self, mock_read, mock_run):
        # Two psql files → one subprocess call.
        mock_read.side_effect = ["SELECT 1;", "SELECT 2;"]
        mock_run.return_value = MagicMock(returncode=0, stdout=b'', stderr=b'')

        # File existence check uses Path(...).exists(), patch it via tmp files.
        with patch('src.executor.Path') as mock_path:
            instance = MagicMock()
            instance.exists.return_value = True
            mock_path.return_value = instance

            group = [
                {'name': 'phase1', 'type': 'psql', 'file': '/tmp/p1.sql', 'joined_group': 'mq'},
                {'name': 'phase2', 'type': 'psql', 'file': '/tmp/p2.sql', 'joined_group': 'mq'},
            ]
            records = self.executor._execute_joined_psql_group(group)

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]['name'], 'phase1')
        self.assertEqual(records[1]['name'], 'phase2')
        # Exactly one psql call regardless of number of fragments.
        self.assertEqual(mock_run.call_count, 1)
        # The stdin payload contains both fragments separated by ';'.
        call = mock_run.call_args
        stdin_payload = call.kwargs['input'].decode('utf-8')
        self.assertIn("SELECT 1", stdin_payload)
        self.assertIn("SELECT 2", stdin_payload)
        self.assertIn(";", stdin_payload)

    @patch('src.executor.subprocess.run')
    @patch('src.executor.SQLParser.read_sql_file')
    def test_joined_group_propagates_psql_failure(self, mock_read, mock_run):
        mock_read.return_value = "SELECT 1;"
        mock_run.return_value = MagicMock(returncode=1, stdout=b'', stderr=b'oh no')

        with patch('src.executor.Path') as mock_path:
            mock_path.return_value.exists.return_value = True

            group = [{'name': 'a', 'type': 'psql', 'file': '/tmp/x.sql', 'joined_group': 'g'}]
            with self.assertRaises(Exception):
                self.executor._execute_joined_psql_group(group)

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