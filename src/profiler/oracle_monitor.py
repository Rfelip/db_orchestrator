import logging
import re
from sqlalchemy import text
from src.profiler.abstract import ProfilerStrategy

log = logging.getLogger(__name__)

class OracleMonitorProfiler(ProfilerStrategy):
    """
    Oracle implementation of ProfilerStrategy.
    If use_diagnostics_pack=True, uses V$SQL_MONITOR (Requires License).
    If False, falls back to V$MYSTAT (Session Stats), which misses Parallel Execution details.
    """

    def __init__(self, session, use_diagnostics_pack=True):
        self.session = session
        self.use_diagnostics_pack = use_diagnostics_pack
        self.sql_id = None
        self.metrics = {}
        self.execution_plan = ""
        self._pre_stats = {}

    def prepare_query(self, sql: str) -> str:
        """
        Prepares query based on licensing configuration.
        """
        if self.use_diagnostics_pack:
            # Inject MONITOR hint
            pattern = re.compile(r"^\s*(SELECT|INSERT|UPDATE|DELETE|MERGE)", re.IGNORECASE)
            match = pattern.match(sql)
            if match:
                verb = match.group(1)
                modified_sql = pattern.sub(f"{verb} /*+ MONITOR */", sql, count=1)
                log.debug("Injected MONITOR hint into SQL.")
                return modified_sql
            return sql
        else:
            # Fallback: Capture pre-execution session stats
            self._pre_stats = self._get_session_stats()
            return sql

    def post_execution_capture(self, cursor, execution_result) -> None:
        """
        Retrieves metrics and execution plan.
        """
        try:
            # 1. Retrieve SQL_ID
            if hasattr(cursor, 'statement') and cursor.statement:
                try:
                    self.sql_id = cursor.statement.sql_id
                except AttributeError:
                    pass
            
            if not self.sql_id:
                row = self.session.execute(
                    text("SELECT prev_sql_id FROM v$session WHERE sid = SYS_CONTEXT('USERENV', 'SID')")
                ).fetchone()
                if row:
                    self.sql_id = row[0]

            if not self.sql_id:
                log.warning("Could not determine SQL_ID. Metrics capture skipped.")
                return

            log.info(f"Capturing metrics for SQL_ID: {self.sql_id}")

            if self.use_diagnostics_pack:
                self._capture_via_monitor()
            else:
                self._capture_via_fallback()

        except Exception as e:
            log.error(f"Failed to capture Oracle metrics: {e}")

    def _capture_via_monitor(self):
        monitor_sql = text("""
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
        """)
        result = self.session.execute(monitor_sql, {'sql_id': self.sql_id}).fetchone()
        
        if result:
            status, elapsed, cpu, io_wait, phy_bytes, px_count = result
            self.metrics = {
                'duration_ms': (elapsed or 0) / 1000.0,
                'db_cpu_ms': (cpu or 0) / 1000.0,
                'db_io_ms': (io_wait or 0) / 1000.0,
                'io_requests': 0, 
                'io_bytes': phy_bytes or 0,
                'parallel_degree': px_count or 0,
                'status': status
            }
        
        # Plan with Stats
        plan_sql = text("SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR(:sql_id, NULL, 'ALLSTATS LAST'))")
        plan_rows = self.session.execute(plan_sql, {'sql_id': self.sql_id}).fetchall()
        self.execution_plan = "\n".join([row[0] for row in plan_rows])

    def _capture_via_fallback(self):
        # Capture post stats
        post_stats = self._get_session_stats()
        
        # Calculate Delta (centiseconds to ms)
        cpu = (post_stats.get('CPU used by this session', 0) - self._pre_stats.get('CPU used by this session', 0)) * 10 
        io_time = (post_stats.get('user I/O wait time', 0) - self._pre_stats.get('user I/O wait time', 0)) * 10
        
        self.metrics = {
            'duration_ms': 0, # Placeholder; Executor should fill with wall clock
            'db_cpu_ms': cpu,
            'db_io_ms': io_time,
            'parallel_degree': 1, # Unknown in fallback
            'status': 'DONE (Fallback)',
            'note': 'Parallel-Obscured Metrics'
        }
        
        # Plan without Stats
        plan_sql = text("SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR(:sql_id, NULL, 'TYPICAL'))")
        plan_rows = self.session.execute(plan_sql, {'sql_id': self.sql_id}).fetchall()
        self.execution_plan = "\n".join([row[0] for row in plan_rows])

    def _get_session_stats(self):
        sql = text("""
            SELECT n.name, s.value 
            FROM v$mystat s 
            JOIN v$statname n ON s.statistic# = n.statistic#
            WHERE n.name IN ('CPU used by this session', 'user I/O wait time')
        """)
        return dict(self.session.execute(sql).fetchall())

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

    def get_plan_content(self) -> str:
        return self.execution_plan