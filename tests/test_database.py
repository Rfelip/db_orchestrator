import unittest
from unittest.mock import MagicMock, patch
from src.database import DatabaseManager

class TestDatabaseManager(unittest.TestCase):
    @patch('src.database.create_engine')
    @patch('src.database.sessionmaker')
    def setUp(self, mock_sessionmaker, mock_create_engine):
        self.mock_engine = MagicMock()
        mock_create_engine.return_value = self.mock_engine
        
        self.mock_session = MagicMock()
        self.mock_scoped_session = MagicMock(return_value=self.mock_session)
        mock_sessionmaker.return_value = MagicMock(return_value=self.mock_session) # This is getting complicated due to scoped_session
        
        # Simpler approach: mock the resulting session factory
        
        self.db_url = "sqlite:///:memory:"
        self.db_manager = DatabaseManager(self.db_url)
        # Manually override Session factory for easier testing
        self.db_manager.Session = MagicMock(return_value=self.mock_session)
        self.db_manager.engine = self.mock_engine
        # Default to a non-Oracle dialect so drop_table takes the IF EXISTS path.
        self.db_manager.engine.dialect.name = 'postgresql'

    def test_init(self):
        self.assertEqual(self.db_manager.db_url, self.db_url)
        self.assertIsNotNone(self.db_manager.engine)

    def test_get_session(self):
        session = self.db_manager.get_session()
        self.assertEqual(session, self.mock_session)

    def test_execute_query(self):
        sql = "SELECT 1"
        self.db_manager.execute_query(sql, session=self.mock_session)
        # Check if session.execute was called
        # The call arguments depend on how text() is used in src/database.py
        self.assertTrue(self.mock_session.execute.called)

    def test_execute_query_no_session(self):
        with self.assertRaises(ValueError):
            self.db_manager.execute_query("SELECT 1", session=None)

    def test_drop_table_postgres_uses_if_exists(self):
        """On non-Oracle dialects, drop_table emits a single DROP TABLE IF EXISTS
        and lets the database decide whether to fire."""
        self.db_manager.drop_table("test_table", self.mock_session)
        self.assertTrue(self.mock_session.execute.called)
        args, _ = self.mock_session.execute.call_args
        self.assertIn("DROP TABLE IF EXISTS test_table", str(args[0]))

    def test_drop_table_oracle_no_if_exists(self):
        """Oracle pre-23c has no IF EXISTS; the manager swallows ORA-00942."""
        self.db_manager.engine.dialect.name = 'oracle'
        self.db_manager.drop_table("test_table", self.mock_session)
        self.assertTrue(self.mock_session.execute.called)
        args, _ = self.mock_session.execute.call_args
        rendered = str(args[0])
        self.assertIn("DROP TABLE test_table", rendered)
        self.assertNotIn("IF EXISTS", rendered)

if __name__ == '__main__':
    unittest.main()
