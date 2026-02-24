# Database Task Orchestrator

CLI tool that executes sequential database tasks (SQL, PL/SQL, Python scripts) defined in a YAML manifest. Handles transactions, retries, profiling, and notifications.

## Quick Start

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
```

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

### Notifications (Discord)

```ini
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/YOUR_ID/YOUR_TOKEN
```

To get a webhook URL: Discord channel settings > Integrations > Webhooks > New Webhook > Copy URL.

Notifications fire on job start/end, step failures, and long-running steps (>5s). If the URL is not set, notifications are silently disabled.

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
