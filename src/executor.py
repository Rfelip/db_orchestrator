import sys
import time
import logging
import argparse
import subprocess
from pathlib import Path
from datetime import datetime

from src.database import DatabaseManager
from src.yaml_manager import YamlManager
from src.parser import SQLParser
from src.notifier import Notifier
from src.utils import render_template
from src.reporter import Reporter
from src.profiler import OracleMonitorProfiler, PostgresExplainProfiler
from config.settings import load_settings  # Assuming we implement this or load env here

# Setup local logger
log = logging.getLogger(__name__)

class Executor:
    """
    The main orchestrator class responsible for executing the workflow defined in the manifest.
    """

    def __init__(self, manifest_path, db_config, notifier_config, dry_run=False, force=False):
        """
        Initialize the Executor.

        Args:
            manifest_path (str): Path to manifest.yaml.
            db_config (dict): Database connection details.
            notifier_config (dict): Notification settings.
            dry_run (bool): If True, only print the plan and exit.
            force (bool): If True, skip user confirmation.
        """
        self.manifest_path = manifest_path
        self.db_config = db_config
        self.dry_run = dry_run
        self.force = force
        
        self.yaml_manager = YamlManager(manifest_path)
        self.notifier = Notifier(
            telegram_token=notifier_config.get('token'),
            telegram_chat_id=notifier_config.get('chat_id')
        )
        
        # Initialize Reporter with a timestamped directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.reporter = Reporter(Path(f"reports/{timestamp}"))
        
        # We delay DB initialization until we actually need it to avoid connection setup in dry-run if desired,
        # but for simplicity we can init it or just the url.
        # Construct DB URL.
        # Format: dialect://user:pass@host:port/service
        # Note: This is a basic construction. 
        self.db_url = (
            f"{db_config['dialect']}://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['service']}"
        )

    def run(self):
        """
        Main execution flow.
        """
        log.info("Starting Database Task Orchestrator")
        
        # 1. Load Manifest
        try:
            manifest = self.yaml_manager.load_manifest()
            all_steps = manifest.get('steps', [])
            
            # Filter enabled steps
            execution_queue = [s for s in all_steps if s.get('enabled', True)]
            
            log.info(f"Loaded {len(all_steps)} steps. {len(execution_queue)} enabled.")
            
            if not execution_queue:
                log.info("No enabled steps found. Exiting.")
                return

        except Exception as e:
            log.critical(f"Failed to load manifest: {e}")
            self.notifier.send_alert("Orchestrator Failure", f"Failed to load manifest: {e}")
            sys.exit(1)

        # 2. Print Plan
        print("\n--- Execution Plan ---")
        for idx, step in enumerate(execution_queue, 1):
            s_type = step.get('type', 'UNK').upper()
            s_name = step.get('name', 'Unnamed')
            s_group = step.get('transaction_group', 'None')
            s_cleanup = f" - Cleanup: {step['cleanup_target']}" if step.get('cleanup_target') else ""
            print(f"[{idx}] {s_type}: {s_name} (Group {s_group}){s_cleanup}")
        print("----------------------\n")

        if self.dry_run:
            log.info("Dry run complete. Exiting.")
            return

        # 3. User Confirmation
        if not self.force:
            if not self._get_user_confirmation(len(execution_queue)):
                log.info("Execution aborted by user.")
                sys.exit(0)
        else:
            log.info("Force flag detected. Skipping user confirmation.")

        # 4. Active Execution Loop
        self.notifier.send_alert("Job Started", f"Starting execution of {len(execution_queue)} tasks.")
        
        db_manager = None
        current_session = None
        current_group = None
        
        try:
            db_manager = DatabaseManager(self.db_url)
            
            for step in execution_queue:
                step_name = step.get('name')
                step_type = step.get('type')
                step_group = step.get('transaction_group')
                
                log.info(f"Processing step: {step_name}")

                # Transaction Management
                if step_group != current_group:
                    if current_session:
                        log.info(f"Committing transaction group {current_group}")
                        current_session.commit()
                        current_session.close()
                        current_session = None
                    
                    if step_group is not None:
                        current_group = step_group
                        current_session = db_manager.get_session()
                        log.info(f"Started transaction group {current_group}")
                    else:
                        current_group = None
                        current_session = db_manager.get_session()

                if not current_session:
                    current_session = db_manager.get_session()

                try:
                    # Pre-flight: Cleanup
                    if step.get('cleanup_target'):
                        db_manager.drop_table(step['cleanup_target'], current_session)

                    # Execution
                    start_time = time.time()
                    
                    if step_type in ['sql', 'plsql']:
                        self._execute_sql_step(step, db_manager, current_session)
                    elif step_type == 'python':
                        # Python scripts break transactions. Commit current.
                        if current_session:
                            current_session.commit()
                            current_session.close()
                            current_session = None
                            current_group = None 
                        
                        self._execute_python_step(step)
                    else:
                        log.warning(f"Unknown step type: {step_type}")
                        continue

                    duration = time.time() - start_time
                    
                    # Post-Process
                    self.yaml_manager.disable_step(step_name)
                    
                    if step.get('notify') or duration > 5:
                        self.notifier.send_alert(
                            "Step Completed", 
                            f"Step '{step_name}' completed in {duration:.2f}s."
                        )

                except Exception as e:
                    log.error(f"Error in step '{step_name}': {e}")
                    if current_session:
                        current_session.rollback()
                    self.notifier.send_alert("Step Failed", f"Step '{step_name}' failed: {e}")
                    raise 

            # Final Commit for any open session
            if current_session:
                current_session.commit()
                current_session.close()

            # Generate Report
            self.reporter.generate_report(db_info=self.db_config.get('dialect', 'Unknown'))
            
            log.info("All tasks completed successfully.")
            self.notifier.send_alert("Job Finished", "All tasks completed successfully.")

        except Exception as e:
            log.critical(f"Execution failed: {e}")
            self.notifier.send_alert("Job Failed", f"Execution stopped due to error: {e}")
            sys.exit(1)
        finally:
            if db_manager:
                db_manager.close()

    def _execute_sql_step(self, step, db_manager, session):
        """Handles SQL/PLSQL execution with retries and profiling."""
        file_path = Path('scripts/sql') / step['file']
        raw_sql = SQLParser.read_sql_file(file_path)
        
        # Apply Templates
        params = step.get('params', {})
        final_sql = render_template(raw_sql, params)
        
        # Initialize Profiler
        profiler = None
        dialect = self.db_config.get('dialect', '').lower()
        if 'oracle' in dialect:
            use_diagnostics = self.db_config.get('use_diagnostics_pack', True)
            profiler = OracleMonitorProfiler(session, use_diagnostics_pack=use_diagnostics)
        elif 'postgres' in dialect:
            profiler = PostgresExplainProfiler()
        
        if profiler:
            final_sql = profiler.prepare_query(final_sql)

        retries = 3
        for attempt in range(retries + 1):
            try:
                start_exec = time.time()
                result = db_manager.execute_query(final_sql, session=session)
                duration_exec = time.time() - start_exec
                
                # Output handling
                if step.get('output_file'):
                    rows = result.fetchall()
                    if rows:
                        out_path = Path(step['output_file'])
                        out_path.parent.mkdir(parents=True, exist_ok=True)
                        with open(out_path, 'w', encoding='utf-8') as f:
                            if result.keys():
                                f.write(','.join(result.keys()) + '\n')
                            for row in rows:
                                f.write(','.join(map(str, row)) + '\n')
                        log.info(f"Output written to {out_path}")
                
                # Profiling Capture
                if profiler:
                    try:
                        profiler.post_execution_capture(result.cursor if hasattr(result, 'cursor') else result, result)
                        metrics = profiler.get_metrics()
                        
                        # Backfill duration if missing (e.g. Oracle Fallback)
                        if metrics.get('duration_ms', 0) == 0:
                            metrics['duration_ms'] = duration_exec * 1000.0
                        
                        plan_content = profiler.get_plan_content()
                            
                        self.reporter.add_task_result(
                            task_name=step['name'],
                            db_type=dialect.split('+')[0].upper(),
                            metrics=metrics,
                            plan_content=plan_content
                        )
                    except Exception as pe:
                        log.error(f"Profiling failed for step '{step['name']}': {pe}")
                
                break # Success
            except Exception as e:
                if attempt < retries:
                    wait = 2 ** attempt
                    log.warning(f"Step '{step['name']}' failed. Retrying in {wait}s... Error: {e}")
                    time.sleep(wait)
                else:
                    log.error(f"Step '{step['name']}' failed after {retries} retries.")
                    raise

    def _execute_python_step(self, step):
        """Handles external Python script execution."""
        file_path = Path('scripts/python') / step['file']
        if not file_path.exists():
            raise FileNotFoundError(f"Python script not found: {file_path}")

        log.info(f"Executing Python script: {file_path}")
        
        # We run the script. It needs to be standalone.
        # Check if we need to pass params? Design doc doesn't specify passing params to python scripts via CLI,
        # but usually they might need env vars or args. 
        # For now, run as is. 
        
        try:
            # Using current python executable
            cmd = [sys.executable, str(file_path)]
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            log.info(f"Script output: {result.stdout}")
        except subprocess.CalledProcessError as e:
            log.error(f"Python script failed: {e.stderr}")
            raise

    def _get_user_confirmation(self, count):
        """Prompts user for confirmation."""
        while True:
            response = input(f"Plan loaded with {count} tasks. Are you sure you want to execute? [y/N]: ").lower().strip()
            if response in ['y', 'yes']:
                return True
            if response in ['n', 'no', '']:
                return False