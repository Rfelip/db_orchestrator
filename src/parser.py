import logging
from pathlib import Path

log = logging.getLogger(__name__)

class SQLParser:
    """
    Handles reading of SQL files.
    """

    _dialect = None  # Set by executor before first use

    @classmethod
    def set_dialect(cls, dialect):
        """Set the SQL dialect for path resolution."""
        cls._dialect = dialect
        log.info(f"SQL parser dialect set to: {dialect}")

    @staticmethod
    def read_sql_file(file_path):
        """
        Reads the content of an SQL file.
        For non-Oracle dialects, looks for translated files in {dialect}-pipeline/ first.

        Args:
            file_path (str or Path): Path to the SQL file.

        Returns:
            str: The content of the file.
        """
        path = Path(file_path)

        # Try dialect-specific path first for non-Oracle
        if SQLParser._dialect and 'oracle' not in SQLParser._dialect:
            # Map dialect to directory prefix
            dialect_map = {
                'postgresql': 'postgres-pipeline',
                'psycopg2': 'postgres-pipeline',
                'clickhouse': 'clickhouse-pipeline',
                'duckdb': 'duckdb-pipeline',
            }
            for key, prefix in dialect_map.items():
                if key in (SQLParser._dialect or ''):
                    # Convert "02 - Contagem de exposição/01_1 - create.sql"
                    # to "postgres-pipeline/schemas/create.sql" or similar
                    dialect_path = Path(prefix) / path.name
                    if dialect_path.exists():
                        log.info(f"Using dialect-specific SQL: {dialect_path}")
                        path = dialect_path
                        break
                    # Also try with underscored filename
                    underscored = path.name.replace(' - ', '_').replace(' ', '_')
                    dialect_path2 = Path(prefix) / underscored
                    if dialect_path2.exists():
                        log.info(f"Using dialect-specific SQL: {dialect_path2}")
                        path = dialect_path2
                        break
                    # Try in schemas subdirectory
                    dialect_path3 = Path(prefix) / "schemas" / path.name
                    if dialect_path3.exists():
                        log.info(f"Using dialect-specific SQL: {dialect_path3}")
                        path = dialect_path3
                        break
                    dialect_path4 = Path(prefix) / "schemas" / underscored
                    if dialect_path4.exists():
                        log.info(f"Using dialect-specific SQL: {dialect_path4}")
                        path = dialect_path4
                        break
                    log.debug(f"No dialect-specific file found for {path.name}, using original")

        if not path.exists():
            log.error(f"SQL file not found: {path}")
            raise FileNotFoundError(f"SQL file not found: {path}")

        try:
            with open(path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            log.error(f"Failed to read SQL file {path}: {e}")
            raise
