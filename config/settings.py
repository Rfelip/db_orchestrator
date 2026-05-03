import os
from dotenv import load_dotenv, find_dotenv
import logging

log = logging.getLogger(__name__)

def load_settings():
    """
    Loads configuration settings from environment variables,
    prioritizing .env file if present.

    Looks for .env starting from the current working directory and walking up.
    This lets pipelines living in a sibling repo (e.g. scripts_tabua) keep their
    own `.env` next to the manifests, rather than inside the orchestrator.

    `override=True` so that values in .env beat any stale environment variables
    that may be left over from a previous shell session.
    """
    load_dotenv(find_dotenv(usecwd=True), override=True)

    db_config = {
        'dialect': os.getenv('DB_DIALECT'),
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASS'),
        'service': os.getenv('DB_SERVICE'),       # For Oracle
        'database': os.getenv('DB_DATABASE'),     # For PostgreSQL / pgduckdb
        'use_diagnostics_pack': os.getenv('USE_DIAGNOSTICS_PACK', 'true').lower() == 'true',
        # pgduckdb / containerized Postgres support — used by the `psql` step type.
        # `container_name` is the name of the docker container running Postgres;
        # `docker_sudo` controls whether we prefix `sudo` (rootful docker installs).
        'container_name': os.getenv('DB_CONTAINER_NAME'),
        'docker_sudo': os.getenv('DB_DOCKER_SUDO', 'true').lower() == 'true',
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
        'discord_webhook_url': os.getenv('DISCORD_WEBHOOK_URL'),
        'telegram_bot_token': os.getenv('TELEGRAM_BOT_TOKEN'),
        'telegram_chat_id': os.getenv('TELEGRAM_CHAT_ID'),
        'user_name': os.getenv('USER_NAME', 'Unknown'),
    }
    # Notifier fan-out is decided in src/notifier.py:build_notifier — this
    # struct just carries whatever env supplied. Channels with empty config
    # are skipped silently; the orchestrator never crashes for missing
    # notification credentials.

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
        n = settings['notifier']
        print(f"Discord Webhook: {'Configured' if n.get('discord_webhook_url') else 'N/A'}")
        tg_ok = bool(n.get('telegram_bot_token') and n.get('telegram_chat_id'))
        print(f"Telegram: {'Configured' if tg_ok else 'N/A'}")
    except ValueError as e:
        print(f"Error loading settings: {e}")