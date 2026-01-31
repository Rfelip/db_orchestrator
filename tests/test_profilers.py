import unittest
from unittest.mock import MagicMock, patch
from src.profiler.oracle_monitor import OracleMonitorProfiler
from src.profiler.postgres_explain import PostgresExplainProfiler

class TestProfilers(unittest.TestCase):
    
    def test_oracle_monitor_prepare_query_with_hint(self):
        mock_session = MagicMock()
        profiler = OracleMonitorProfiler(mock_session, use_diagnostics_pack=True)
        sql = "SELECT * FROM my_table"
        prepared_sql = profiler.prepare_query(sql)
        self.assertIn("/*+ MONITOR */", prepared_sql)

    def test_oracle_monitor_prepare_query_fallback(self):
        mock_session = MagicMock()
        mock_session.execute.return_value.fetchall.return_value = [('CPU used by this session', 100)]
        profiler = OracleMonitorProfiler(mock_session, use_diagnostics_pack=False)
        sql = "SELECT * FROM my_table"
        prepared_sql = profiler.prepare_query(sql)
        self.assertEqual(sql, prepared_sql)
        self.assertEqual(profiler._pre_stats['CPU used by this session'], 100)

    def test_postgres_explain_prepare_query(self):
        profiler = PostgresExplainProfiler()
        sql = "SELECT * FROM my_table;"
        prepared_sql = profiler.prepare_query(sql)
        self.assertTrue(prepared_sql.startswith("EXPLAIN (ANALYZE"))
        self.assertFalse(prepared_sql.endswith(";"))

    def test_postgres_explain_parsing(self):
        profiler = PostgresExplainProfiler()
        mock_result = MagicMock()
        # Mocking the JSON structure Postgres returns for EXPLAIN (ANALYZE, FORMAT JSON)
        plan_json = [{
            "Plan": {
                "Node Type": "Gather",
                "Workers Launched": 2,
                "Shared Read Blocks": 10,
                "Shared Hit Blocks": 50,
                "Plans": [
                    {
                        "Node Type": "Seq Scan",
                        "Shared Read Blocks": 5
                    }
                ]
            },
            "Execution Time": 123.45
        }]
        mock_result.fetchall.return_value = [[plan_json]]
        
        profiler.post_execution_capture(None, mock_result)
        metrics = profiler.get_metrics()
        
        self.assertEqual(metrics['duration_ms'], 123.45)
        self.assertEqual(metrics['parallel_degree'], 2)
        self.assertEqual(metrics['io_requests'], 15) # 10 + 5
        self.assertEqual(metrics['cache_hits'], 50)

if __name__ == '__main__':
    unittest.main()
