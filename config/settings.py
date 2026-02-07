import os
from dotenv import load_dotenv
import logging

log = logging.getLogger(__name__)

def load_settings():
    """
    Loads configuration settings from environment variables,
    prioritizing .env file if present.
    """
    load_dotenv()  # This loads variables from .env file into the environment

    db_config = {
        'dialect': os.getenv('DB_DIALECT'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASS'),
        'service': os.getenv('DB_SERVICE'), # For Oracle
        'database': os.getenv('DB_DATABASE'), # For PostgreSQL
        'use_diagnostics_pack': os.getenv('USE_DIAGNOSTICS_PACK', 'true').lower() == 'true'
    }

    # Validate essential DB settings
    if not all([db_config['dialect'], db_config['host'], db_config['user'], db_config['password']]):
        log.error("Missing essential database configuration in .env. Please check DB_DIALECT, DB_HOST, DB_USER, DB_PASS.")
        raise ValueError("Missing essential database configuration.")
    
    # Port is often optional or default for some dialects, handle gracefully
    if db_config['port'] is None:
        log.warning("DB_PORT not set. Using default port based on dialect if applicable.")
    
    # Service vs Database for different DB types
    if 'oracle' in db_config['dialect'] and db_config['service'] is None:
        log.error("Oracle dialect chosen but DB_SERVICE is not set.")
        raise ValueError("DB_SERVICE is required for Oracle dialect.")
    elif 'postgresql' in db_config['dialect'] and db_config['database'] is None:
        log.error("PostgreSQL dialect chosen but DB_DATABASE is not set.")
        raise ValueError("DB_DATABASE is required for PostgreSQL dialect.")

    notifier_config = {
        'webhook_url': os.getenv('DISCORD_WEBHOOK_URL')
    }

    if not notifier_config['webhook_url']:
        log.warning("DISCORD_WEBHOOK_URL not set. Discord notifications will be disabled.")

    return {
        'db': db_config,
        'notifier': notifier_config
    }

# Example usage (for testing/debugging, not normally called directly in production)
if __name__ == "__main__":
    try:
        settings = load_settings()
        print("Loaded Settings:")
        print(f"DB Dialect: {settings['db']['dialect']}")
        print(f"Discord Webhook: {'Configured' if settings['notifier']['webhook_url'] else 'N/A'}")
    except ValueError as e:
        print(f"Error loading settings: {e}")