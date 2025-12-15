import logging
import logging.config
import os
from datetime import datetime

def setup_logging(log_dir="logs", level=logging.INFO):
    """
    Sets up logging configuration for the application.
    Logs to console and a rotating file in the 'logs' directory.
    
    Args:
        log_dir (str): Directory where log files will be stored.
        level (int): The minimum level for messages to be logged.
    """
    # Ensure log directory exists
    os.makedirs(log_dir, exist_ok=True)
    
    # Generate a timestamped log file name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file_path = os.path.join(log_dir, f"orchestrator_{timestamp}.log")

    logging_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            },
            'file_formatter': {
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            }
        },
        'handlers': {
            'console': {
                'level': 'INFO',
                'formatter': 'standard',
                'class': 'logging.StreamHandler',
                'stream': 'ext://sys.stdout'
            },
            'file': {
                'level': 'DEBUG', # Log all debug messages to file
                'formatter': 'file_formatter',
                'class': 'logging.handlers.RotatingFileHandler',
                'filename': log_file_path,
                'maxBytes': 10485760, # 10 MB
                'backupCount': 5,
                'encoding': 'utf8'
            }
        },
        'loggers': {
            '': {  # Root logger
                'handlers': ['console', 'file'],
                'level': level,
                'propagate': False
            },
            'sqlalchemy': { # For SQLAlchemy logs
                'handlers': ['file'], # Only log SQLAlchemy to file
                'level': 'WARNING',
                'propagate': False
            },
            'requests': { # For requests library logs
                'handlers': ['file'], # Only log requests to file
                'level': 'WARNING',
                'propagate': False
            }
        }
    }
    
    logging.config.dictConfig(logging_config)
    logging.info(f"Logging setup complete. Log file: {log_file_path}")

# Example usage (for testing/debugging)
if __name__ == "__main__":
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.debug("This is a debug message.")
    logger.info("This is an info message.")
    logger.warning("This is a warning message.")
    logger.error("This is an error message.")
    logger.critical("This is a critical message.")