# Design Document Addendum: Observability & Reporting (Revision 2)

**Parent Document:** System Design Document: Database Task Orchestrator
**Version:** 1.2
**Date:** 2025-12-15
**Focus:** Execution Plans, Post-Task Reporting, and Parallel-Aware Profiling.

## 1. Overview

This addendum details the subsystems for **Task Profiling** and **Automated Reporting**.
The primary goal is to capture the "true cost" of a query—distinguishing between CPU-bound and I/O-bound operations—even when the database distributes the work across multiple parallel execution servers (Oracle Parallel Query or Postgres Parallel Workers).

## 2. Architecture Updates

### 2.1 File Structure
New modules are introduced to handle metrics collection and report generation.

```text
db_orchestrator/
│
├── reports/                 # New: Destination for run reports
│   └── YYYYMMDD_HHMMSS/     # One folder per execution run
│       ├── summary.json     # Aggregated metrics for machine parsing
│       ├── report.html      # Human-readable dashboard
│       └── plans/           # Raw Execution Plans (txt/json)
│
├── src/
│   ├── profiler/            # New Package
│   │   ├── __init__.py
│   │   ├── abstract.py      # Interface
│   │   ├── oracle_monitor.py # Oracle implementation (V$SQL_MONITOR)
│   │   └── postgres_explain.py # Postgres implementation (EXPLAIN ANALYZE)
│   │
│   ├── reporter.py          # JSON/HTML Report Generator
│   └── executor.py          # Updated to invoke Profilers
```

---

## 3. Profiling Strategy (`src/profiler/`)

To solve the "Parallel Execution" visibility problem, we must move away from simple session snapshots (`V$MYSTAT`) and utilize the database's built-in monitoring aggregation tools.

### 3.1 Abstract Interface (`ProfilerStrategy`)
*   `prepare_query(sql)`: Modifies SQL if necessary (e.g., injecting hints or wrapping in `EXPLAIN`).
*   `post_execution_capture(cursor, execution_result)`: Retrieves metrics and execution plans after the task finishes.
*   `get_metrics()`: Returns standardized dictionary:
    *   `duration_ms`: Wall clock time.
    *   `db_cpu_ms`: Total CPU time (summed across parallel workers).
    *   `db_io_ms`: Total I/O wait time (summed across parallel workers).
    *   `io_requests`: Physical read requests.
    *   `parallel_degree`: Number of workers/slaves used.
*   `save_plan(path)`: Writes the captured plan to disk.

### 3.2 Oracle Implementation: `OracleMonitorProfiler`
Oracle Parallel Queries splits work into a Coordinator (QC) and Slaves (PX). `V$MYSTAT` only sees the QC.
**Solution:** Use **`V$SQL_MONITOR`**. This view automatically aggregates statistics from the QC and all PX servers for a specific execution.

*   **Preparation:**
    *   Inject the `/*+ MONITOR */` hint into the SQL using regex. This forces Oracle to track the query in `V$SQL_MONITOR` even if it executes quickly, ensuring we get data.
*   **Execution:**
    *   Run the query.
    *   Retrieve the `SQL_ID` from the cursor (e.g., `cursor.statement.sql_id` in `oracledb` driver) or via `SELECT PREV_SQL_ID ...`.
*   **Capture Logic (SQL):**
    ```sql
    SELECT 
       MAX(status) as status,
       SUM(elapsed_time) as total_time, -- QC + Slaves wall time (approx)
       SUM(cpu_time) as cpu_time,       -- Aggregated CPU
       SUM(user_io_wait_time) as io_time, -- Aggregated I/O
       SUM(physical_read_bytes) as phy_read_bytes,
       MAX(px_servers_allocated) as parallel_count
    FROM v$sql_monitor
    WHERE sql_id = :sql_id
      AND sid = SYS_CONTEXT('USERENV', 'SID') -- Ensure we get our specific run
    GROUP BY sql_id, sql_exec_id
    ```
*   **Execution Plan Capture:**
    *   Use `DBMS_XPLAN.DISPLAY_CURSOR(:sql_id, NULL, 'ALLSTATS LAST')`.
    *   *Benefit:* This format explicitly shows the "Table Queue" (TQ) interactions, proving how data moved between parallel layers.

### 3.3 Postgres Implementation: `PostgresExplainProfiler`
Postgres handles parallel workers (Gather Node) differently. We cannot query a view like `V$SQL_MONITOR` easily after the fact for a specific query without extensions like `pg_stat_monitor`. The most reliable native method is `EXPLAIN (ANALYZE, FORMAT JSON)`.

*   **Preparation:**
    *   Wrap the query: `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, FORMAT JSON) <original_query>`.
*   **Execution:**
    *   Run the wrapped query. The result *is* the report.
*   **Metric Extraction (JSON Parsing):**
    *   **Total Duration:** Root node `Execution Time`.
    *   **Parallelism:** Recursive search for nodes of type `Gather`. The `Workers Launched` field indicates the parallel degree.
    *   **I/O vs CPU:**
        *   Postgres `EXPLAIN` does not give a clean "Time spent on I/O" vs "Time spent on CPU" in milliseconds unless `track_io_timing` is enabled in `postgresql.conf`.
        *   **Strategy:**
            1.  Sum `Shared Read Blocks` + `Local Read Blocks` + `Temp Read Blocks` -> **I/O Metric**.
            2.  Sum `Shared Hit Blocks` -> **CPU/Memory Metric**.
            3.  **Bottleneck Logic:** If `Read Blocks` > 0 and `Read Blocks` / (`Hit` + `Read`) > 0.5, classify as I/O intensive.
*   **Plan Capture:** Save the raw JSON output.

---

## 4. Reporting Module (`src/reporter.py`)

The reporter aggregates the data collected by the profilers into a folder `reports/YYYYMMDD_HHMMSS/`.

### 4.1 Folder Content
1.  **`plans/`**: Contains files named `{step_order}_{step_name}.txt`.
    *   *Oracle:* Contains the output of `DBMS_XPLAN`.
    *   *Postgres:* Contains the formatted JSON from `EXPLAIN`.
2.  **`summary.json`**:
    ```json
    {
      "meta": { "timestamp": "2023-10-27T10:00:00", "db": "Oracle 19c" },
      "tasks": [
        {
          "name": "Heavy Aggregation",
          "db_type": "ORACLE",
          "execution_time_sec": 45.2,
          "parallel_degree": 4,
          "profile": {
             "cpu_time_sec": 120.5,  // Note: Can be higher than execution time due to parallelism
             "io_time_sec": 15.0,
             "bottleneck": "CPU"
          },
          "plan_file": "plans/02_Heavy_Aggregation.txt"
        }
      ]
    }
    ```

### 4.2 Bottleneck Analysis Logic
The report generation includes a logic layer to categorize tasks:

*   **For Oracle:**
    *   Calculate `Total_Resource_Time = cpu_time + io_time`.
    *   If `io_time / Total_Resource_Time > 50%` → **I/O Bound**.
    *   Else → **CPU Bound**.
    *   *Note:* If `elapsed_time` is significantly higher than `cpu + io`, flag as **Concurrency/Locking Issue** (waiting on latches/locks, not raw resources).

*   **For Postgres:**
    *   If `track_io_timing` is OFF: Use Buffer Hit Ratio. (Low Hit Ratio = I/O Bound).
    *   If `track_io_timing` is ON: Use time-based comparison similar to Oracle.

---

## 5. Execution Flow Modifications

1.  **Orchestrator Start:**
    *   Create Report Directory.

2.  **Task Execution Loop:**
    *   **Pre-Execute:**
        *   Identify DB Type.
        *   Initialize specific Profiler (OracleMonitor vs PostgresExplain).
        *   Profiler calls `prepare_query(sql)` (Inject hints or wrap EXPLAIN).
    *   **Execute:**
        *   Run the Modified SQL.
    *   **Post-Execute:**
        *   **Capture:** Profiler runs `post_execution_capture`.
            *   *Oracle:* Queries `V$SQL_MONITOR` for the specific `SQL_ID` to get sum of QC + Slaves. Queries `DBMS_XPLAN`.
            *   *Postgres:* Parses the returned JSON.
        *   **Save:** Write Plan to `reports/.../plans/`. Add metrics to memory.

3.  **Finalization:**
    *   Generate `summary.json` and `index.html`.
    *   Log location of reports.

---

## 6. Risks and Mitigations

### 6.1 Oracle `V$SQL_MONITOR` Retention
*   **Risk:** If the system is extremely busy, the query stats might age out of the `V$SQL_MONITOR` buffer before Python can query it (though unlikely for immediate capture).
*   **Mitigation:** The code performs the capture immediately after the `execute()` call returns.

### 6.2 Oracle Licensing
*   **Risk:** `V$SQL_MONITOR` officially requires the **Diagnostics and Tuning Pack** license.
*   **Fallback (License-Free):** If the user config disables "use_diagnostics_pack", the system falls back to `V$SESSMETRIC`.
    *   *Limitation:* `V$SESSMETRIC` will only show the Coordinator's wait for "Parallel Dequeue". It will accurately report *Duration*, but will fail to report specific PX CPU/IO split. The report will flag these as "Parallel-Obscured" metrics.

### 6.3 Postgres Write Operations
*   **Risk:** `EXPLAIN ANALYZE` executes the query. If the query is an `INSERT/UPDATE/DELETE`, it **will change data**.
*   **Mitigation:**
    *   The entire task is wrapped in a **Transaction**.
    *   Postgres Profiler runs the `EXPLAIN ANALYZE ...`.
    *   Currently, the Orchestrator is designed to run the task once. Since `EXPLAIN ANALYZE` runs it, we treat that as the actual execution.
    *   *Crucial:* We must ensure we don't run `EXPLAIN ANALYZE` *and then* run the query again, or data will be duplicated. The `executor.py` logic must replace the standard run with the profiled run.
