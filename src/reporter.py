import json
import logging
import shutil
from pathlib import Path
from datetime import datetime
from jinja2 import Environment, FileSystemLoader

log = logging.getLogger(__name__)

class Reporter:
    """
    Generates execution reports (JSON summary and HTML dashboard).
    """

    def __init__(self, report_dir: Path):
        self.report_dir = report_dir
        self.report_dir.mkdir(parents=True, exist_ok=True)
        self.plans_dir = self.report_dir / 'plans'
        self.plans_dir.mkdir(exist_ok=True)
        self.tasks_data = []

    def add_task_result(self, task_name, db_type, metrics, plan_content):
        """
        Adds a task's execution results to the report data.
        """
        # Save plan to file
        safe_name = "".join(x for x in task_name if x.isalnum() or x in "._- ")
        plan_filename = f"{len(self.tasks_data) + 1:02d}_{safe_name}.txt"
        plan_path = self.plans_dir / plan_filename
        
        try:
            with open(plan_path, 'w', encoding='utf-8') as f:
                f.write(plan_content)
        except Exception as e:
            log.error(f"Failed to save plan for {task_name}: {e}")
            plan_filename = None

        # Determine Bottleneck
        bottleneck = "Unknown"
        cpu = metrics.get('db_cpu_ms', 0) / 1000.0
        io = metrics.get('db_io_ms', 0) / 1000.0
        total_resource = cpu + io
        
        if total_resource > 0:
            if io / total_resource > 0.5:
                bottleneck = "I/O"
            else:
                bottleneck = "CPU"
        elif metrics.get('io_requests', 0) > 0 and metrics.get('cache_hits', 0) >= 0:
             # Postgres fallback (block based)
             reads = metrics.get('io_requests', 0)
             hits = metrics.get('cache_hits', 0)
             total_ops = reads + hits
             if total_ops > 0 and (reads / total_ops) > 0.5:
                 bottleneck = "I/O"
             else:
                 bottleneck = "CPU"

        # Structure for summary.json
        task_entry = {
            "name": task_name,
            "db_type": db_type,
            "execution_time_sec": metrics.get('duration_ms', 0) / 1000.0,
            "parallel_degree": metrics.get('parallel_degree', 1),
            "profile": {
                "cpu_time_sec": cpu,
                "io_time_sec": io,
                "bottleneck": bottleneck
            },
            "plan_file": f"plans/{plan_filename}" if plan_filename else None,
            "plan_content": plan_content  # For HTML embedding
        }
        
        self.tasks_data.append(task_entry)

    def generate_report(self, db_info="Unknown DB"):
        """
        Writes summary.json and renders report.html.
        """
        timestamp = datetime.now().isoformat()
        
        summary = {
            "meta": {
                "timestamp": timestamp,
                "db": db_info
            },
            "tasks": self.tasks_data
        }

        # Write summary.json
        try:
            with open(self.report_dir / 'summary.json', 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2)
        except Exception as e:
            log.error(f"Failed to write summary.json: {e}")

        # Render HTML
        try:
            # Load template from src/templates
            template_dir = Path(__file__).parent / 'templates'
            if not template_dir.exists():
                 # Fallback if running from root
                 template_dir = Path('src/templates')

            env = Environment(loader=FileSystemLoader(template_dir))
            template = env.get_template('report.html')
            
            html_output = template.render(
                meta=summary['meta'],
                tasks=summary['tasks']
            )
            
            with open(self.report_dir / 'report.html', 'w', encoding='utf-8') as f:
                f.write(html_output)
                
            log.info(f"Report generated at {self.report_dir / 'report.html'}")
            
        except Exception as e:
            log.error(f"Failed to generate HTML report: {e}")
            raise
