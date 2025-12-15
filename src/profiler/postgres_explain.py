import logging
import json
from src.profiler.abstract import ProfilerStrategy

log = logging.getLogger(__name__)

class PostgresExplainProfiler(ProfilerStrategy):
    """
    Postgres implementation of ProfilerStrategy using EXPLAIN (ANALYZE, FORMAT JSON).
    Parses the resulting JSON to extract CPU/IO proxies and parallelism info.
    """

    def __init__(self):
        self.metrics = {}
        self.raw_plan_json = None

    def prepare_query(self, sql: str) -> str:
        """
        Wraps the original query in an EXPLAIN ANALYZE block.
        """
        # Remove trailing semicolon if present to avoid syntax error inside EXPLAIN
        sql = sql.strip().rstrip(';')
        return f"EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, FORMAT JSON) {sql}"

    def post_execution_capture(self, cursor, execution_result) -> None:
        """
        Parses the JSON output from the EXPLAIN command.
        Args:
            cursor: The cursor (used to fetch if execution_result isn't the data).
            execution_result: Expected to be the ResultProxy.
        """
        try:
            # fetchall() usually returns a list of tuples. 
            # For EXPLAIN JSON, it's usually [[ [ { ... } ] ]] (a single row, single col containing the JSON array)
            rows = execution_result.fetchall()
            if not rows:
                log.warning("No EXPLAIN output returned.")
                return

            # Depending on driver, it might be a string or a dict object.
            # psycopg2 usually returns the JSON structure if using Json adapter, or list of list.
            # Let's assume it returns the JSON data structure directly or a string.
            
            plan_data = rows[0][0] # First row, first column
            
            if isinstance(plan_data, str):
                self.raw_plan_json = json.loads(plan_data)
            else:
                self.raw_plan_json = plan_data

            # The top level is a list of plans (usually just one)
            if isinstance(self.raw_plan_json, list) and len(self.raw_plan_json) > 0:
                root_node = self.raw_plan_json[0]
                plan_node = root_node.get('Plan', {})
                
                # 1. Duration
                # 'Execution Time' is at the root level (in ms)
                exec_time = root_node.get('Execution Time', 0)
                
                # 2. Parallelism & I/O
                self._analyze_plan_node(plan_node)
                
                # Calculate aggregated metrics
                total_read = self.metrics.get('read_blocks', 0)
                total_hit = self.metrics.get('hit_blocks', 0)
                
                # Simple heuristic for I/O vs CPU time if track_io_timing is off
                # We don't have direct ms for I/O unless configured.
                # We store the block counts.
                
                self.metrics['duration_ms'] = exec_time
                self.metrics['io_requests'] = total_read
                self.metrics['cache_hits'] = total_hit
                
                # If we had track_io_timing, we would sum 'I/O Read Time' from nodes.
                # Let's try to sum it if present.
                
            else:
                log.warning("Unexpected JSON format for Explain Plan.")

        except Exception as e:
            log.error(f"Failed to parse Postgres EXPLAIN output: {e}")

    def _analyze_plan_node(self, node):
        """
        Recursively traverse the plan tree to sum metrics.
        """
        # Sum Buffers
        # Shared + Local + Temp
        read_blocks = (
            node.get('Shared Read Blocks', 0) + 
            node.get('Local Read Blocks', 0) + 
            node.get('Temp Read Blocks', 0)
        )
        hit_blocks = (
            node.get('Shared Hit Blocks', 0) + 
            node.get('Local Hit Blocks', 0)
        )
        
        # Accumulate
        self.metrics['read_blocks'] = self.metrics.get('read_blocks', 0) + read_blocks
        self.metrics['hit_blocks'] = self.metrics.get('hit_blocks', 0) + hit_blocks
        
        # Check I/O timing (if enabled in DB)
        io_time = (
            node.get('I/O Read Time', 0) + 
            node.get('I/O Write Time', 0)
        )
        self.metrics['db_io_ms'] = self.metrics.get('db_io_ms', 0.0) + io_time
        
        # Parallelism
        if node.get('Node Type') == 'Gather':
            self.metrics['parallel_degree'] = max(
                self.metrics.get('parallel_degree', 0), 
                node.get('Workers Launched', 0)
            )

        # Recurse
        if 'Plans' in node:
            for child in node['Plans']:
                self._analyze_plan_node(child)

    def get_metrics(self) -> dict:
        return self.metrics

    def save_plan(self, path: str) -> None:
        if not self.raw_plan_json:
            log.warning("No JSON plan to save.")
            return

        try:
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(self.raw_plan_json, f, indent=2)
        except Exception as e:
            log.error(f"Failed to save Postgres plan to {path}: {e}")

    def get_plan_content(self) -> str:
        if not self.raw_plan_json:
            return "No Plan Captured"
        return json.dumps(self.raw_plan_json, indent=2)
