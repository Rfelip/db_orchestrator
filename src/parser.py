import logging
from pathlib import Path

log = logging.getLogger(__name__)

class SQLParser:
    """
    Handles reading of SQL files.
    """
    
    @staticmethod
    def read_sql_file(file_path):
        """
        Reads the content of an SQL file.
        
        Args:
            file_path (str or Path): Path to the SQL file.
            
        Returns:
            str: The content of the file.
        """
        path = Path(file_path)
        if not path.exists():
            log.error(f"SQL file not found: {path}")
            raise FileNotFoundError(f"SQL file not found: {path}")
            
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            log.error(f"Failed to read SQL file {path}: {e}")
            raise
