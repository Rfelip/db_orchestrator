from pathlib import Path
from src.reporter import Reporter

def generate_dummy():
    report_dir = Path('reports/example_run')
    if report_dir.exists():
        import shutil
        shutil.rmtree(report_dir)
        
    reporter = Reporter(report_dir)

    # Task 1: Oracle CPU Bound
    reporter.add_task_result(
        task_name="Calculate Sales Aggregates",
        db_type="ORACLE",
        metrics={
            'duration_ms': 45000,
            'db_cpu_ms': 120000, # High CPU (parallel)
            'db_io_ms': 5000,
            'parallel_degree': 4
        },
        plan_content="""
-------------------------------------------------------------------------------------------------------
| Id  | Operation                  | Name             | Rows  | Bytes | Cost (%CPU)| Time     | Pstart|
-------------------------------------------------------------------------------------------------------
|   0 | SELECT STATEMENT           |                  |       |       |   452 (100)|          |       |
|   1 |  PX COORDINATOR            |                  |       |       |            |          |       |
|   2 |   PX SEND QC (RANDOM)      | :TQ10001         |     1 |    13 |   452   (2)| 00:00:01 |       |
|   3 |    HASH GROUP BY           |                  |     1 |    13 |   452   (2)| 00:00:01 |       |
|   4 |     PX RECEIVE             |                  |   100K|  1269K|   451   (2)| 00:00:01 |       |
|   5 |      PX SEND HASH          | :TQ10000         |   100K|  1269K|   451   (2)| 00:00:01 |       |
|   6 |       PX BLOCK ITERATOR    |                  |   100K|  1269K|   451   (2)| 00:00:01 |     1 |
|*  7 |        TABLE ACCESS FULL   | SALES_FACT       |   100K|  1269K|   451   (2)| 00:00:01 |     1 |
-------------------------------------------------------------------------------------------------------
"""
    )

    # Task 2: Postgres I/O Bound
    reporter.add_task_result(
        task_name="Archive Old Logs",
        db_type="POSTGRES",
        metrics={
            'duration_ms': 12000,
            'db_cpu_ms': 0,
            'db_io_ms': 0,
            'io_requests': 15000,
            'cache_hits': 200,
            'parallel_degree': 1
        },
        plan_content="""
[
  {
    "Plan": {
      "Node Type": "Seq Scan",
      "Relation Name": "audit_logs",
      "Filter": "(created_at < '2023-01-01 00:00:00'::timestamp without time zone)",
      "Rows Removed by Filter": 500,
      "Shared Hit Blocks": 200,
      "Shared Read Blocks": 15000,
      "Execution Time": 12000.5
    }
  }
]
"""
    )
    
    # Task 3: Mixed Workload
    reporter.add_task_result(
        task_name="Update Customer Scores",
        db_type="ORACLE",
        metrics={
            'duration_ms': 25000,
            'db_cpu_ms': 20000,
            'db_io_ms': 18000, 
            'parallel_degree': 2
        },
        plan_content="PLAN_TABLE_OUTPUT\n-----------------\nUPDATE STATEMENT..."
    )

    reporter.generate_report(db_info="Oracle 19c & Postgres 14")
    print(f"Dummy report generated at {report_dir / 'report.html'}")

if __name__ == "__main__":
    generate_dummy()
