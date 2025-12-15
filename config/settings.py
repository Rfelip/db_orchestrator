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
        'database': os.getenv('DB_DATABASE') # For PostgreSQL
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

    telegram_config = {
        'token': os.getenv('TELEGRAM_BOT_TOKEN'),
        'chat_id': os.getenv('TELEGRAM_CHAT_ID')
    }

    # Validate Telegram settings if token is present (chat_id is also needed)
    if telegram_config['token'] and not telegram_config['chat_id']:
        log.warning("TELEGRAM_BOT_TOKEN is set, but TELEGRAM_CHAT_ID is missing. Telegram notifications may not work.")
    elif telegram_config['chat_id'] and not telegram_config['token']:
         log.warning("TELEGRAM_CHAT_ID is set, but TELEGRAM_BOT_TOKEN is missing. Telegram notifications may not work.")
    
    return {
        'db': db_config,
        'telegram': telegram_config
    }

# Example usage (for testing/debugging, not normally called directly in production)
if __name__ == "__main__":
    try:
        settings = load_settings()
        print("Loaded Settings:")
        print(f"DB Dialect: {settings['db']['dialect']}")
        print(f"Telegram Token (first 5 chars): {settings['telegram']['token'][:5] if settings['telegram']['token'] else 'N/A'}")
    except ValueError as e:
        print(f"Error loading settings: {e}")