# System Design Document: Database Task Orchestrator

## 1. Overview
The **Database Task Orchestrator** is a CLI-based Python application designed to automate the execution of sequential database tasks. It manages a queue of SQL queries and Python scripts, handling dependencies, transactions, logging, and notifications.

**Key Objectives:**
*   **Sequential Execution:** Run tasks in a strict order defined by a YAML manifest.
*   **State Persistence:** Automatically disable tasks in the YAML file upon successful completion.
*   **Reliability:** Implements transaction grouping, rollback capabilities, and retry logic with exponential backoff.
*   **Observability:** Provides real-time granular Telegram notifications and detailed local logging.

---

## 2. Project Architecture

### 2.1 File Structure
```text
db_orchestrator/
│
├── config/
│   ├── .env                 # Secrets (DB Creds, Telegram Token)
│   ├── settings.py          # Configuration loader
│   └── logging_config.py    # Log format definitions
│
├── queue/
│   └── manifest.yaml        # The "State" and "Instruction" file
│
├── scripts/
│   ├── sql/                 # SQL source files (.sql, .plsql)
│   └── python/              # External Python automation scripts (.py)
│
├── src/
│   ├── __init__.py
│   ├── database.py          # Connection & Transaction Manager
│   ├── executor.py          # Main Orchestration Logic
│   ├── parser.py            # SQL Parsing (splitting by delimiter)
│   ├── notifier.py          # Telegram integration
│   ├── yaml_manager.py      # State updating (ruamel.yaml)
│   └── utils.py             # Templating (Jinja2) & Helpers
│
├── logs/                    # Timestamped log files
├── main.py                  # CLI Entry point
└── requirements.txt         # Dependencies
```

### 2.2 Tech Stack
*   **Language:** Python 3.9+
*   **Database Interface:** `SQLAlchemy` (for connection abstraction) + Driver (e.g., `cx_Oracle`, `psycopg2`).
*   **Configuration:** `python-dotenv` (Secrets), `ruamel.yaml` (Manifest manipulation).
*   **Templating:** `Jinja2` (Parameter injection into SQL).
*   **CLI:** `argparse`.

---

## 3. Configuration Specifications

### 3.1 The `.env` File
Stores sensitive connection details.
```ini
DB_DIALECT=oracle+cx_oracle  # or postgresql+psycopg2
DB_HOST=localhost
DB_PORT=1521
DB_USER=admin
DB_PASS=secret
DB_SERVICE=ORCL

TELEGRAM_BOT_TOKEN=123:ABC...
TELEGRAM_CHAT_ID=987654...
```

### 3.2 The Manifest (`manifest.yaml`)
The source of truth for execution order.
*   **Schema Rules:**
    *   **name:** Display name for logs/alerts.
    *   **file:** Filename relative to `scripts/sql` or `scripts/python`.
    *   **type:** `sql`, `plsql`, or `python`.
    *   **enabled:** `true` (run) or `false` (skip).
    *   **transaction_group:** (Optional) Integer. Consecutive SQL steps with the same ID share a transaction.
    *   **cleanup_target:** (Optional) Table name to `DROP` before execution.
    *   **output_file:** (Optional) Relative path to save query output (e.g., `results/report.csv`).
    *   **notify:** (Optional) Boolean. Force Telegram notification for this step.
    *   **params:** (Optional) Dictionary of values to inject into Jinja2 templates.

*   **Example:**
```yaml
steps:
  - name: "Initialize Staging"
    file: "01_create_staging.sql"
    type: "sql"
    enabled: true
    transaction_group: 1
    cleanup_target: "ST_SALES_IMPORT"
    notify: false

  - name: "Import Data"
    file: "02_import_sales.sql"
    type: "sql"
    enabled: true
    transaction_group: 1
    params:
      region: "US-EAST"
```

---

## 4. Component Design

### 4.1 Database Manager (`src/database.py`)
*   **Responsibilities:**
    *   Create engine using `SQLAlchemy`.
    *   Manage Sessions/Connections.
    *   Handle `DROP TABLE IF EXISTS` logic safely.
    *   Execute raw SQL.
*   **Key Method:** `execute_query(sql, params, session)`
*   **Key Method:** `drop_table(table_name, session)` (Checks `information_schema` or `all_tables` before attempting drop).

### 4.2 YAML Manager (`src/yaml_manager.py`)
*   **Library:** `ruamel.yaml` (Must be used to preserve comments).
*   **Functionality:**
    *   `load_manifest()`: Reads the YAML.
    *   `disable_step(step_name)`: Finds the step by name, sets `enabled: false`, adds a comment (e.g., `# Done: YYYY-MM-DD`), and saves the file in place.

### 4.3 Script Templater (`src/utils.py`)
*   **Single Statement Enforcement:** Reads `.sql` files as a single atomic command. No splitting by `;` is performed.
*   **PL/SQL Support:** `plsql` tasks are executed as blocks (e.g., `BEGIN ... END;` in Oracle) and must be self-contained.
*   **Templating:** Uses `jinja2.Template` to replace `{{ param }}` placeholders with values from the YAML `params` block.

### 4.4 External Script Interface
*   **Constraint:** Python scripts are **non-transactional**.
*   **Contract:** External scripts must be standalone processes (via `subprocess` or dynamic import) that handle their own logic. They do not share the Orchestrator's DB session.

### 4.5 Notifier (`src/notifier.py`)
*   **Logic:**
    *   `send_message(msg)`: Sends to Telegram API.
    *   **Granularity:**
        *   **Info:** Sent on script start ("🚀 Job Started").
        *   **Step Success:** Sent if `notify: true` OR execution time > threshold (e.g., 5s). Includes step name and duration.
        *   **Error:** Sent immediately on failure with exception details.
        *   **Summary:** Sent at the end of execution ("✅ 4 Tasks Completed").

---

## 5. Execution Logic (The Orchestrator)

Here is the updated section of the System Design Document. I have updated **Section 5.1 (Main Execution Flow)** to include the mandatory user confirmation step.

### Updated Section: 5. Execution Logic (The Orchestrator)

#### 5.1 Main Execution Flow (`src/executor.py`)

1.  **Initialization:**
    *   Load `.env`.
    *   Setup Logging (File + Console).
    *   Parse CLI args (check for `--dry-run` or `--force`).

2.  **Plan Generation (Pre-Check):**
    *   Load `manifest.yaml`.
    *   Filter for tasks where `enabled: true`.
    *   **Print Execution Plan** to Console in a readable format:
        *   `[1] SQL: Create Table (Group 1) - Cleanup: ST_SALES`
        *   `[2] SQL: Insert Data (Group 1) - Params: region=US`
        *   `[3] PY:  Clean Data`
    *   *Logic Branch:*
        *   If `--dry-run` CLI flag is set: Log "Dry Run Complete" and **EXIT**.

3.  **User Confirmation (Safety Gate):**
    *   **Prompt:** Pause the application and wait for standard input.
        *   `"Plan loaded with [X] tasks. Are you sure you want to execute? [y/N]: "`
    *   **Logic:**
        *   **Input `y` or `yes`:** Proceed to Step 4 (Active Execution).
        *   **Input `n`, `no`, or Enter:** Log "Execution aborted by user" and **EXIT**.

4.  **Active Execution Loop:**
    *   Initialize `current_transaction_group = None`.
    *   Open DB Session.
    
    *   **Iterate through Steps:**
        1.  **Skip Check:** If `enabled: false`, continue (double-check).
        2.  **Transaction Management:**
            *   If `step.group != current_transaction_group`:
                *   Commit previous session (if open).
                *   Start new transaction.
                *   Update `current_transaction_group`.
        3.  **Pre-Flight (Cleanup):**
            *   If `cleanup_target` exists: Execute `DROP TABLE`.
        4.  **Process (SQL & PL/SQL):**
            *   Read file.
            *   Apply Jinja2 params.
            *   **Execution:**
                *   Execute as a single statement.
            *   **Output Handling:**
                *   If `output_file` is defined: Fetch results (e.g., `fetchall()`) and write to specified path (CSV/JSON).
            *   **Retry Loop:**
                *   Try Execute.
                *   On Error: Wait `2^retries` seconds. Retry (Max 3 times).
                *   On Fatal Error: **ROLLBACK** transaction, Log, Alert Telegram, Exit.
        5.  **Process (Python):**
            *   Commit any pending SQL transaction (Python scripts break transactions).
            *   Run Script via `subprocess`.
            *   On Error: Alert, Exit.
        6.  **Post-Process:**
            *   Call `yaml_manager.disable_step(step.name)`.
            *   Send Telegram Notification (if eligible).

5.  **Finalization:**
    *   Commit remaining open transactions.
    *   Close Connection.
    *   Send "Job Finished" Telegram summary.