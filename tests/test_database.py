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

    @patch('src.database.sqlalchemy.inspect')
    def test_drop_table_exists(self, mock_inspect):
        mock_inspector = MagicMock()
        mock_inspect.return_value = mock_inspector
        mock_inspector.has_table.return_value = True
        
        self.db_manager.drop_table("test_table", self.mock_session)
        
        # Verify drop was called
        # execute_query calls session.execute
        self.assertTrue(self.mock_session.execute.called)
        # We can check the string in the call args if we want to be specific
        args, _ = self.mock_session.execute.call_args
        self.assertIn("DROP TABLE test_table", str(args[0]))

    @patch('src.database.sqlalchemy.inspect')
    def test_drop_table_not_exists(self, mock_inspect):
        mock_inspector = MagicMock()
        mock_inspect.return_value = mock_inspector
        mock_inspector.has_table.return_value = False
        
        self.db_manager.drop_table("test_table", self.mock_session)
        
        # Verify execute was NOT called (for drop)
        self.assertFalse(self.mock_session.execute.called)

if __name__ == '__main__':
    unittest.main()
