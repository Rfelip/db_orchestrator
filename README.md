# Database Task Orchestrator

A single API for running SQL on databases — Oracle, PostgreSQL, and
pgduckdb-in-docker. Exposes a YAML-manifest workflow plus a Python
library (`src.api`) so other code can run queries through the same
plumbing instead of reinventing it. Handles transactions, retries,
execution-plan capture, and notifications to Discord and Telegram.

## Quick Start

### As a CLI

```bash
# Install dependencies (add -r requirements-dev.txt to run the tests —
# duckdb is test-only, the orchestrator never imports it)
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

The simplest path is **named targets**. Declare each database the
caller might reach in `.env` with `DB_TARGET_<NAME>_*` keys (see
`config/.env.example` for the full template), then:

```python
from src.api import run_sql

# Caller doesn't see secrets, transport details, or SSH plumbing.
result = run_sql(
    "SELECT count(*) AS n FROM tabua_pura_subpops WHERE emp = 'MON'",
    target="MR3",
    dql_only=True,
)
print(result.columns, result.rows, result.elapsed_ms)
```

Transports that ship today:

  - **direct** — SQLAlchemy connection to a host:port. Use for local
    Postgres, Oracle, or any DB whose port the caller can reach.
  - **ssh+wsl** — `ssh adm@host wsl docker exec -i <container> psql
    --csv -f -` with SQL on stdin. Use for containerised DBs reachable
    only via SSH (e.g. MR3's pgduckdb, where Tailscale terminates at
    the Windows host and the container's port is not visible).
  - **ssh+duckdb** — native DuckDB on the remote host, the full dialect
    (`read_parquet`, `glob`, `filename=true`) that pgduckdb does not
    expose. See *Native DuckDB* below.
  - **duckdb** — the same native DuckDB, in a child process on **this**
    machine. No ssh, no key, no hop. See *Native DuckDB* below.
  - **ssh+clickhouse** — `clickhouse-client` in a container over ssh.

The transport is set by `DB_TARGET_<NAME>_TRANSPORT` in `.env`. The
caller never has to know which one fires.

Every `run_sql` call appends a JSONL line to
`output/_ad_hoc/_provenance.jsonl` with the timestamp, fetch name, SQL
hash, transport, row count, elapsed time, status (ok / error), and
error stderr if any. Disable with `log_provenance=False`.

For full-manifest runs:

```python
from config.settings import load_settings
from src.api import run_manifest

settings = load_settings()
run_manifest(
    "queue/manifest.yaml",
    db_config=settings['db'],
    notifier_config=settings['notifier'],
    force=True,
)
```

`run_sql` / `run_manifest` are the canonical entry points. `main.py`
is a thin CLI wrapper around them.

## Native DuckDB

A `ssh+duckdb` or `duckdb` target runs the manifest on **one persistent
DuckDB** — a single process for the whole run, so a `TEMP TABLE` created
in one step is there for the next. That is a semantic requirement, not a
speed optimisation: a pipeline that stages per-bucket temp tables cannot
work on a connection-per-statement transport.

The two differ only in where that process starts, and the choice is
forced by where `main.py` runs relative to the data:

  - `ssh+duckdb` when the orchestrator drives a DuckDB on another host.
  - `duckdb` when the orchestrator already runs **on** the machine
    holding the data. `Executor` makes output directories and `--resume`
    stats `produces:` paths through the local filesystem, so an
    orchestrator split from its data by ssh makes directories on the
    wrong machine and resumes against files that were never there.

```bash
python main.py --manifest manifests/pipeline.yaml --target MR3DUCK --force
```

```ini
DB_TARGET_MR3DUCK_TRANSPORT=ssh+duckdb
DB_TARGET_MR3DUCK_SSH=mr3-lan
DB_TARGET_MR3DUCK_WSL=false
DB_TARGET_MR3DUCK_PYTHON=/home/user/.venv/bin/python

# Run knobs. Defaults shown; they belong to the machine, not the SQL.
DB_TARGET_MR3DUCK_MEMORY_LIMIT=16GB
DB_TARGET_MR3DUCK_THREADS=8
DB_TARGET_MR3DUCK_TEMP_DIRECTORY=~/duckdb_spill
DB_TARGET_MR3DUCK_MAX_TEMP_DIRECTORY_SIZE=512GB
DB_TARGET_MR3DUCK_PRESERVE_INSERTION_ORDER=false
DB_TARGET_MR3DUCK_PROFILE=false
```

A local target drops `SSH`, `WSL` and `PYTHON` and keeps every knob:

```ini
DB_TARGET_LOCALDUCK_TRANSPORT=duckdb
DB_TARGET_LOCALDUCK_MEMORY_LIMIT=16GB
DB_TARGET_LOCALDUCK_THREADS=8
DB_TARGET_LOCALDUCK_TEMP_DIRECTORY=~/duckdb_spill
DB_TARGET_LOCALDUCK_MAX_TEMP_DIRECTORY_SIZE=512GB
```

`PYTHON` defaults to the interpreter running the orchestrator, which is
the one whose environment already resolved `duckdb`; name it only to
point at a different venv.

`temp_directory` must sit on fast storage — `~` on MR3 is NVMe;
`/mnt/BANCOS` is a rotational RAID1 and spill is write-heavy.
`max_temp_directory_size` is validated as a bounded size because the
host is shared and an unbounded spill fills the root filesystem.

As a library:

```python
from src.duckdb_session import open_transport_session
from src.transport import DuckDbSettings, build_transport

transport = build_transport(transport="ssh+duckdb", ssh="mr3-lan", wsl=False,
                            settings=DuckDbSettings(memory_limit="16GB"))
# ...or, on the machine that holds the data:
#   build_transport(transport="duckdb",
#                   settings=DuckDbSettings(memory_limit="16GB"))
with open_transport_session(transport) as session:
    session.run("CREATE TEMP TABLE t AS SELECT 1")
    result = session.run("SELECT * FROM t")
```

`session.run()` returns either a `RawResult` (there was a result set) or
a `Completed` (there was not — DDL, DML, `COPY`, `SET`). It does not
report a `COPY` as an empty SELECT.

`psql` and `plsql` steps are refused on a DuckDB target rather than
approximated.

## Declared fan-out: `foreach`

A step repeats by declaring the axes it ranges over. Several axes are
their cross product, in declaration order, last axis varying fastest.

```yaml
steps:
  - name: at_ingressos
    type: sql
    file: "02 - Eventos/02 - at_ingressos.sql"
    params: { out: /mnt/lake }
    foreach:
      bkt: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]

  - name: qx
    type: sql
    file: "05 - Tabuas/03 - qx.sql"
    foreach:
      bkt: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]
      year: [2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
```

The first becomes 16 steps named `at_ingressos_bkt00 … _bkt15`; the
second becomes 144, `qx_bkt00_year2017 … qx_bkt15_year2025`. Integer
values are zero-padded to the widest value on their axis, so names sort
the way the values do. Each expansion gets its axis values merged into
`params`, so the SQL uses `{{ bkt }}` / `{{ year }}` unchanged.

An axis that is also set in `params` is an error, not an override. The
unknown-key check is unchanged: `foreach` widens the schema by exactly
one key.

Expanded step names exist nowhere in the source YAML, so `disable_step`
cannot match them and a `foreach` manifest is never rewritten — good, it
is hand-written source. Resume for expanded steps is the run ledger
below, which works off the expanded plan instead of the file.

## Resume

A 345-step pipeline that dies at step 300 must not cost you steps 1–299.
Two mechanisms, answering different questions.

### `--resume` — where it broke

Every completed step appends a line to
`<plan-dir>/<run_id>/ledger.jsonl` as the run goes (flushed per line, so
it survives the crash that made it necessary). A later run reads it back
and skips what it proves is done.

```bash
# The run that failed at step 300 of 345.
python main.py --manifest manifests/gerados/manifest_pipeline.yaml \
               --target MR3DUCK --force

# What can be resumed from.
python main.py --resume list

# Pick up where it stopped. 'last' is the most recent run.
python main.py --manifest manifests/gerados/manifest_pipeline.yaml \
               --target MR3DUCK --force --enable-all --resume last
```

It prints what it is doing, and what it could not check, before running
anything:

```
--- Resume ---
ledger 20260730T101500: 299 of 345 planned steps proven complete.
  verified on disk : 144 (declared `produces:` still present)
  NOT verified     : 155
                     ^ these declared no `produces:`, so resume is TRUSTING that
                       their outputs are still on disk and still valid. Nothing checked.
  Resuming at      : qx_bkt03_year2019
  Because          : it is not recorded as completed in the ledger
```

A step is skipped only if the ledger has it **and** its SQL and params
still hash the same **and** its declared output is still there. The skip
list is a *prefix*: the first step that fails any check is the resume
point, and everything after it runs too.

`--enable-all` is the recommended pairing. Auto-disable (`enabled: false`
written back to the YAML) and the ledger both record "done", and only the
ledger sees expanded names; `--enable-all` makes the ledger the single
authority instead of two half-authorities.

Resuming a resumed run works: the new ledger inherits the old entries, so
a second failure does not cost the first run's work.

### `produces:` — what resume is allowed to check

The ledger knows a step *finished*. It cannot know its output survived.
`produces:` closes that gap where you declare it — a path template
rendered with the step's params, so each `foreach` expansion gets its own:

```yaml
  - name: qx
    type: sql
    file: "05 - Tabuas/03 - qx.sql"
    params: { out: /mnt/lake }
    produces: "{{ out }}/tabuas/qx_bkt{{ bkt }}_{{ year }}.parquet"
    foreach:
      bkt: [0, 1, 2, 3]
      year: [2019, 2020]
```

Delete `qx_bkt02_2019.parquet` and resume: that step and everything after
it re-runs. Without `produces:`, resume skips the step and says so in the
banner rather than implying it checked. `produces:` checks existence, not
contents — a truncated file from a killed writer still passes.

`produces:` also does one thing before the step runs: **its directory is
created** (`mkdir -p` on the parent), so a plan can build its output tree
from nothing. `COPY … TO '<path>'` creates no parent directory, and DuckDB's
partitioned write creates a single level — without this, every manifest that
writes files needs a wrapper script to lay out the folders first, and that
hand-written list is what drifts. Declaring the path is enough; nothing else
declares the folders. Both assume the executor sees the same filesystem the
step writes to, which is the same assumption resume makes.

### `--from` / `--until` — just this part

Substring match over the **expanded** plan, the ergonomics the old
`scripts/run_pipeline.py` had:

```bash
python main.py --target MR3DUCK --force --from qx_bkt03_year2019
python main.py --target MR3DUCK --force --from qx_ --until qx_bkt15_year2025
```

`--from` takes the first matching step, `--until` the last, inclusive —
so `--until qx_bkt03` runs the whole `qx_bkt03_*` group. A substring that
matches nothing is an error, not a full run.

The two compose: the window is what you asked to run, and the ledger only
removes work from inside it.

`--no-ledger` opts out of recording (the run is then not resumable).
Ledgers live beside captured plans under `--plan-dir` — one run id, one
directory, what ran and what it cost.

## Execution plans

With `PROFILE=true` on a DuckDB target, every statement's plan is
captured — via `SET enable_profiling='json'` rather than
`EXPLAIN ANALYZE`, because that captures DDL and `COPY` too and needs no
query rewriting.

```
reports/plans/<run_id>/index.jsonl        one line per statement
reports/plans/<run_id>/001_<step>.json    the raw DuckDB profile
```

```bash
python main.py --plans          # summarise the latest run
python main.py --plans list     # list captured run ids
python main.py --plans 20260730T101500
```

The summary reports the slowest steps with their top operators, and the
dominant operators across the whole run. `--plan-dir` (or
`$ORCH_PLAN_DIR`) moves the store.

Measured cost of leaving profiling on: **~0.2 ms per statement**, fixed.
On four statements over 4M rows that is +1.7%; on 345 sub-millisecond
statements it is +61%. Against anything doing real work it disappears —
a 345-step pipeline pays about 0.07s.

## SQL catalog (optional)

A `sql-catalog.yaml` next to your manifest gives every SQL file a
stable identifier. Manifests then reference SQL by `sql_id:` instead
of `file:`, and the orchestrator resolves the path at load time.

```yaml
# sql-catalog.yaml
sql:
  - id: load_staging
    file: scripts/sql/load_staging.sql
    intent: "Load raw rows from source into staging table."
    read_only: false
    expected_duration_s: 60
```

```yaml
# manifest.yaml
steps:
  - name: ingest
    type: sql
    sql_id: load_staging        # ← looked up in the catalog
```

The catalog is optional — manifests using `file:` directly keep
working unchanged. See `sql-catalog.yaml.example` for the full
template.

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
│   ├── transport.py         # How SQL reaches a DB + DuckDbSettings
│   ├── duckdb_session.py    # Persistent remote DuckDB session
│   ├── plans.py             # Plan capture, storage, and summary
│   ├── ledger.py            # Run ledger + resume decision + --from/--until
│   ├── types.py             # Step / ManifestConfig, foreach expansion
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

**Step types:** `sql`, `plsql`, `psql`, `bulk_insert`, `python`, `manifest`. Python scripts break any open transaction and run standalone via subprocess.

## Key Behaviors

- **Transaction groups:** Consecutive SQL steps with the same `transaction_group` ID share a single DB transaction. Commit happens when the group changes or a Python step runs.
- **Auto-disable:** Completed steps get `enabled: false` written back to the manifest (preserves YAML comments). Only steps whose names appear in the YAML — `foreach` expansions are covered by the run ledger instead, see **Resume**.
- **Retries:** SQL steps retry up to 3 times with exponential backoff on failure.
- **Profiling:** Oracle (AWR/ASH) and PostgreSQL (EXPLAIN) profiling is automatic when available. Results go to `reports/`.
