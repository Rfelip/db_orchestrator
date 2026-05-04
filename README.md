# Database Task Orchestrator

A single API for running SQL on databases — Oracle, PostgreSQL, and
pgduckdb-in-docker. Exposes a YAML-manifest workflow plus a Python
library (`src.api`) so other code can run queries through the same
plumbing instead of reinventing it. Handles transactions, retries,
execution-plan capture, and notifications to Discord and Telegram.

## Quick Start

### As a CLI

```bash
# Install dependencies
uv pip install -r requirements.txt

# Configure environment (see below)
cp config/.env.example config/.env

# Dry run (shows plan, executes nothing)
python main.py --dry-run

# Execute
python main.py

# Execute without confirmation prompt
python main.py --force

# Ad-hoc query mode (DQL only — no DDL/DML)
python main.py --query "SELECT count(*) FROM users"
```

### As a library

```python
from config.settings import load_settings
from src.api import run_sql, run_manifest

settings = load_settings()

# Single-statement query — returns a QueryResult with columns + rows.
result = run_sql(
    "SELECT id, name FROM users WHERE active = :active",
    db_config=settings['db'],
    params={'active': True},
    dql_only=True,
)
print(result.columns, result.row_count)

# Full manifest — same flow as the CLI, just callable from Python.
run_manifest(
    "queue/manifest.yaml",
    db_config=settings['db'],
    notifier_config=settings['notifier'],
    force=True,
)
```

`run_sql` / `run_manifest` are the canonical entry points. `main.py` is
a thin CLI wrapper around them; downstream code that needs to drive
the orchestrator from Python should import directly from `src.api`.

## Configuration

All config lives in `config/.env`. Required variables:

### Database

```ini
DB_DIALECT=oracle+oracledb        # or postgresql+psycopg2
DB_HOST=your_host
DB_PORT=1521
DB_USER=your_db_user
DB_PASS=your_db_password
DB_SERVICE=your_service_name       # Oracle only
DB_DATABASE=your_database_name     # PostgreSQL only
USE_DIAGNOSTICS_PACK=true          # Oracle AWR/ASH profiling (optional)
ORACLE_CLIENT_DIR=                 # Path to Oracle Instant Client (optional)
```

### Notifications (Discord and Telegram)

Both channels are optional and independent. Configure either, both, or
neither — alerts fan out to whichever are populated.

```ini
# Discord (channel webhook)
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/YOUR_ID/YOUR_TOKEN

# Telegram (bot token + chat id)
TELEGRAM_BOT_TOKEN=123456:abcdef...
TELEGRAM_CHAT_ID=123456789
```

- **Discord webhook URL:** Discord channel settings → Integrations →
  Webhooks → New Webhook → Copy URL.
- **Telegram bot token:** create a bot via @BotFather; the token looks
  like `123456:abcdef`.
- **Telegram chat ID:** message the bot once, then visit
  `https://api.telegram.org/bot<TOKEN>/getUpdates` to find your chat's
  `id`. For a personal DM the chat ID equals your Telegram user ID.

Alerts fire on job start/end, step failures, and any step that
exceeds 5s wall-clock or sets `notify: true`. If neither channel is
configured the orchestrator runs silently with one warning at startup.

Failure alerts include the step name, source SQL file, transaction or
joined-group label, and a SHA-256 prefix of the source SQL — that
prefix lets a recipient grep `reports/{ts}/rendered/` directly to find
the SQL that ran.

## Project Structure

```
db_orchestrator/
├── config/
│   ├── .env                 # Secrets (DB creds, webhook URL)
│   ├── settings.py          # Loads config from .env
│   └── logging_config.py    # Log formatting
├── queue/
│   └── manifest.yaml        # Execution manifest (task definitions)
├── scripts/
│   ├── sql/                 # SQL source files
│   └── python/              # Standalone Python scripts
├── src/
│   ├── database.py          # SQLAlchemy connection & transaction management
│   ├── executor.py          # Main orchestration logic
│   ├── parser.py            # SQL file reading
│   ├── notifier.py          # Discord webhook notifications
│   ├── reporter.py          # Execution report generation
│   ├── yaml_manager.py      # Manifest state management (ruamel.yaml)
│   ├── utils.py             # Jinja2 templating
│   └── profiler/            # Query profiling (Oracle AWR, Postgres EXPLAIN)
├── reports/                 # Generated execution reports
├── logs/                    # Timestamped log files
├── main.py                  # CLI entry point
└── requirements.txt
```

## The Manifest

The manifest (`queue/manifest.yaml`) defines what runs and in what order. Steps are executed sequentially; completed steps are auto-disabled.

```yaml
steps:
  - name: "Create staging table"
    file: "scripts/sql/01_create_staging.sql"
    type: sql
    enabled: true
    transaction_group: 1          # Steps in the same group share a transaction
    cleanup_target: "ST_SALES"    # DROP this table before running (optional)

  - name: "Import data"
    file: "scripts/sql/02_import.sql"
    type: sql
    enabled: true
    transaction_group: 1
    params:                       # Jinja2 template variables (optional)
      region: "US-EAST"
    notify: true                  # Force notification on completion (optional)
    output_file: "results/out.csv"  # Save query results to file (optional)

  - name: "Run cleanup script"
    file: "scripts/python/cleanup.py"
    type: python
    enabled: true
```

**Step types:** `sql`, `plsql`, `python`. Python scripts break any open transaction and run standalone via subprocess.

## Key Behaviors

- **Transaction groups:** Consecutive SQL steps with the same `transaction_group` ID share a single DB transaction. Commit happens when the group changes or a Python step runs.
- **Auto-disable:** Completed steps get `enabled: false` written back to the manifest (preserves YAML comments).
- **Retries:** SQL steps retry up to 3 times with exponential backoff on failure.
- **Profiling:** Oracle (AWR/ASH) and PostgreSQL (EXPLAIN) profiling is automatic when available. Results go to `reports/`.
