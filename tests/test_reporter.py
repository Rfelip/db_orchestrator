import unittest
import json
import shutil
from pathlib import Path
from src.reporter import Reporter

class TestReporter(unittest.TestCase):
    def setUp(self):
        self.test_report_dir = Path("tests/temp_report")
        self.reporter = Reporter(self.test_report_dir)

    def tearDown(self):
        if self.test_report_dir.exists():
            shutil.rmtree(self.test_report_dir)

    def test_add_task_result_cpu_bottleneck(self):
        metrics = {
            'db_cpu_ms': 1000,
            'db_io_ms': 200,
            'duration_ms': 1500,
            'parallel_degree': 2
        }
        self.reporter.add_task_result("Test CPU Task", "ORACLE", metrics, "PLAN CONTENT")
        
        self.assertEqual(len(self.reporter.tasks_data), 1)
        self.assertEqual(self.reporter.tasks_data[0]['profile']['bottleneck'], "CPU")
        self.assertTrue((self.test_report_dir / "plans/01_Test CPU Task.txt").exists())

    def test_add_task_result_io_bottleneck(self):
        metrics = {
            'db_cpu_ms': 200,
            'db_io_ms': 1000,
            'duration_ms': 1500,
            'parallel_degree': 1
        }
        self.reporter.add_task_result("Test IO Task", "POSTGRES", metrics, "PLAN CONTENT")
        
        self.assertEqual(self.reporter.tasks_data[0]['profile']['bottleneck'], "I/O")

    def test_generate_report(self):
        self.reporter.add_task_result("Task 1", "ORACLE", {'duration_ms': 100}, "PLAN 1")
        self.reporter.generate_report("Oracle 19c")
        
        self.assertTrue((self.test_report_dir / "summary.json").exists())
        self.assertTrue((self.test_report_dir / "report.html").exists())
        
        with open(self.test_report_dir / "summary.json", 'r') as f:
            data = json.load(f)
            self.assertEqual(data['meta']['db'], "Oracle 19c")
            self.assertEqual(len(data['tasks']), 1)

if __name__ == '__main__':
    unittest.main()
