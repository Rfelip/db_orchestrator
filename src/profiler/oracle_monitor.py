import logging
import re
from src.profiler.abstract import ProfilerStrategy

log = logging.getLogger(__name__)

class OracleMonitorProfiler(ProfilerStrategy):
    """
    Oracle implementation of ProfilerStrategy using V$SQL_MONITOR.
    Captures aggregated statistics from QC and PX servers.
    """

    def __init__(self, session):
        self.session = session
        self.sql_id = None
        self.metrics = {}
        self.execution_plan = ""

    def prepare_query(self, sql: str) -> str:
        """
        Injects the /*+ MONITOR */ hint into the SQL to ensure tracking.
        """
        # Simple regex to find the first SELECT/INSERT/UPDATE/DELETE/MERGE and inject hint
        # Logic: Replace "SELECT" with "SELECT /*+ MONITOR */"
        # Case insensitive flag needed.
        # This is a basic implementation. Complex SQL might need robust parsing.
        
        pattern = re.compile(r"^\s*(SELECT|INSERT|UPDATE|DELETE|MERGE)", re.IGNORECASE)
        match = pattern.match(sql)
        
        if match:
            # Check if hints already exist? For simplicity, we just append ours.
            # If /*+ exists, we might ideally merge, but appending a new comment block works in Oracle usually
            # or just inserting after the verb.
            # "SELECT /*+ MONITOR */ ..."
            verb = match.group(1)
            modified_sql = pattern.sub(f"{verb} /*+ MONITOR */", sql, count=1)
            log.debug("Injected MONITOR hint into SQL.")
            return modified_sql
        
        return sql

    def post_execution_capture(self, cursor, execution_result) -> None:
        """
        Retrieves SQL_ID, queries V$SQL_MONITOR, and fetches execution plan.
        """
        try:
            # 1. Retrieve SQL_ID
            # cx_Oracle / python-oracledb specific
            # cursor.statement might be None if no statement executed or error
            if hasattr(cursor, 'statement') and cursor.statement:
                # Assuming python-oracledb or cx_Oracle
                # In some versions it might be accessible differently. 
                # cursor.statement is an object, cursor.statement.sql_id might exist if using python-oracledb
                # For cx_Oracle it might be different, but we moved to oracledb.
                try:
                    self.sql_id = cursor.statement.sql_id
                except AttributeError:
                    # Fallback or older driver logic if needed
                    pass
            
            # Fallback: If we can't get it from cursor, try getting the last executed SQL_ID from session
            if not self.sql_id:
                # This is risky in high concurrency if shared session, but usually fine here.
                # Query v$session or similar? 
                # Better: SELECT PREV_SQL_ID FROM v$session WHERE SID = SYS_CONTEXT('USERENV','SID')
                row = self.session.execute(
                    "SELECT prev_sql_id FROM v$session WHERE sid = SYS_CONTEXT('USERENV', 'SID')"
                ).fetchone()
                if row:
                    self.sql_id = row[0]

            if not self.sql_id:
                log.warning("Could not determine SQL_ID. Metrics capture skipped.")
                return

            log.info(f"Capturing metrics for SQL_ID: {self.sql_id}")

            # 2. Query V$SQL_MONITOR
            # We filter by current SID to avoid picking up other sessions running same SQL
            monitor_sql = """
                SELECT 
                   MAX(status) as status,
                   SUM(elapsed_time) as total_time,
                   SUM(cpu_time) as cpu_time,
                   SUM(user_io_wait_time) as io_time,
                   SUM(physical_read_bytes) as phy_read_bytes,
                   MAX(px_servers_allocated) as parallel_count
                FROM v$sql_monitor
                WHERE sql_id = :sql_id
                  AND sid = SYS_CONTEXT('USERENV', 'SID')
                GROUP BY sql_id, sql_exec_id
            """
            
            # We might have multiple executions if looped (unlikely in this orchestrator). 
            # We take the latest (implicitly or via ordering if needed). 
            # The GROUP BY suggests we might get multiple rows if multiple executions occurred.
            # We'll fetch one.
            
            result = self.session.execute(monitor_sql, {'sql_id': self.sql_id}).fetchone()
            
            if result:
                # Unpack
                status, elapsed, cpu, io_wait, phy_bytes, px_count = result
                
                self.metrics = {
                    'duration_ms': (elapsed or 0) / 1000.0, # Oracle times are microseconds
                    'db_cpu_ms': (cpu or 0) / 1000.0,
                    'db_io_ms': (io_wait or 0) / 1000.0,
                    'io_requests': 0, # Not easily available as count without detailed stats
                    'io_bytes': phy_bytes or 0,
                    'parallel_degree': px_count or 0,
                    'status': status
                }
            else:
                log.warning(f"No V$SQL_MONITOR data found for SQL_ID {self.sql_id}")

            # 3. Capture Plan
            # ALLSTATS LAST gives runtime stats
            plan_sql = "SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR(:sql_id, NULL, 'ALLSTATS LAST'))"
            plan_rows = self.session.execute(plan_sql, {'sql_id': self.sql_id}).fetchall()
            
            self.execution_plan = "\n".join([row[0] for row in plan_rows])

        except Exception as e:
            log.error(f"Failed to capture Oracle metrics: {e}")

    def get_metrics(self) -> dict:
        return self.metrics

    def save_plan(self, path: str) -> None:
        if not self.execution_plan:
            log.warning("No execution plan to save.")
            return
            
        try:
            with open(path, 'w', encoding='utf-8') as f:
                f.write(self.execution_plan)
        except Exception as e:
            log.error(f"Failed to save execution plan to {path}: {e}")
