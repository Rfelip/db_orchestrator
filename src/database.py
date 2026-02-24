import os
import logging
import sqlalchemy
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session

log = logging.getLogger(__name__)

class DatabaseManager:
    """
    Manages database connections, sessions, and raw SQL execution.
    Supports transaction management and safe object cleanup.
    """

    def __init__(self, db_url):
        """
        Initialize the DatabaseManager.

        Args:
            db_url (str): SQLAlchemy connection string (e.g., 'oracle+cx_oracle://user:pass@host:port/service')
        """
        self.db_url = db_url
        self.engine = None
        self.Session = None
        self._setup_engine()

    def _setup_engine(self):
        """Creates the SQLAlchemy engine."""
        try:
            # Oracle Thick Mode Support
            if 'oracle' in self.db_url:
                try:
                    import oracledb
                    lib_dir = os.environ.get('ORACLE_CLIENT_DIR')
                    try:
                        if lib_dir:
                            oracledb.init_oracle_client(lib_dir=lib_dir)
                            log.info(f"Initialized Oracle Instant Client from {lib_dir}")
                        else:
                            oracledb.init_oracle_client()
                            log.info("Initialized Oracle Instant Client from default location")
                    except oracledb.DatabaseError as e:
                        # Error if already initialized or path invalid
                        log.warning(f"Oracle client init warning (might be already init): {e}")
                except ImportError:
                    log.warning("oracledb not installed, but oracle dialect used.")

            # pool_pre_ping=True helps recover from stale connections
            self.engine = create_engine(self.db_url, pool_pre_ping=True)
            self.Session = scoped_session(sessionmaker(bind=self.engine))
            log.info("Database engine initialized.")
        except Exception as e:
            log.error(f"Failed to initialize database engine: {e}")
            raise

    def get_session(self):
        """Returns a new thread-local session."""
        return self.Session()

    def execute_query(self, sql, params=None, session=None):
        """
        Executes a raw SQL query.

        Args:
            sql (str): The SQL query string.
            params (dict, optional): Parameters for binding.
            session (Session, optional): The active DB session.

        Returns:
            ResultProxy: The result of the execution.
        """
        if session is None:
            raise ValueError("A valid session object is required.")

        try:
            # SQLAlchemy 1.4+ uses text() construct for raw SQL
            stmt = text(sql)
            if params:
                result = session.execute(stmt, params)
            else:
                result = session.execute(stmt)
            return result
        except Exception as e:
            log.error(f"Query execution failed: {e}")
            raise

    def drop_table(self, table_name, session):
        """
        Safely drops a table if it exists. 
        Checks metadata first to avoid blindly running DROP.

        Args:
            table_name (str): Name of the table to drop.
            session (Session): Active DB session.
        """
        try:
            # We use the engine's dialect to check for table existence
            # This is safer and more portable than querying system views manually
            inspector = sqlalchemy.inspect(self.engine)
            
            # Note: For Oracle, table names are usually uppercase in metadata
            # For Postgres, they are lowercase unless quoted.
            # We will check assuming the user provided the correct case, 
            # but for Oracle we might want to upper() it if not found.
            
            exists = inspector.has_table(table_name)
            
            # Simple fallback for Oracle case insensitivity if needed
            if not exists and self.engine.dialect.name == 'oracle':
                 exists = inspector.has_table(table_name.upper())
                 if exists:
                     table_name = table_name.upper()

            if exists:
                log.info(f"Dropping table: {table_name}")
                self.execute_query(f"DROP TABLE {table_name}", session=session)
            else:
                log.info(f"Table {table_name} not found. Skipping cleanup.")

        except Exception as e:
            log.error(f"Failed to drop table {table_name}: {e}")
            raise

    def truncate_table(self, table_name, session):
        """
        Trunca uma tabela, preservando o schema (DDL).

        Args:
            table_name (str): Nome da tabela a truncar.
            session (Session): Sessão ativa do banco.
        """
        try:
            inspector = sqlalchemy.inspect(self.engine)
            exists = inspector.has_table(table_name)

            if not exists and self.engine.dialect.name == 'oracle':
                exists = inspector.has_table(table_name.upper())
                if exists:
                    table_name = table_name.upper()

            if exists:
                log.info(f"Truncating table: {table_name}")
                self.execute_query(f"TRUNCATE TABLE {table_name}", session=session)
            else:
                log.info(f"Table {table_name} not found. Skipping truncate.")

        except Exception as e:
            log.error(f"Failed to truncate table {table_name}: {e}")
            raise

    def close(self):
        """Dispose of the engine and close connections."""
        if self.engine:
            self.engine.dispose()