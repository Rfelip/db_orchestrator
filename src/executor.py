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

    def __init__(self, manifest_path, db_config, notifier_config, dry_run=False, force=False, enable_all=False):
        """
        Initialize the Executor.

        Args:
            manifest_path (str): Path to manifest.yaml.
            db_config (dict): Database connection details.
            notifier_config (dict): Notification settings.
            dry_run (bool): If True, only print the plan and exit.
            force (bool): If True, skip user confirmation.
            enable_all (bool): If True, run all tasks regardless of 'enabled' flag.
        """
        self.manifest_path = manifest_path
        self.db_config = db_config
        self.dry_run = dry_run
        self.force = force
        self.enable_all = enable_all

        self.yaml_manager = YamlManager(manifest_path)
        self.notifier = Notifier(
            webhook_url=notifier_config.get('webhook_url'),
            user_name=notifier_config.get('user_name', 'Unknown')
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
            if self.enable_all:
                execution_queue = all_steps
            else:
                execution_queue = [s for s in all_steps if s.get('enabled', True)]

            log.info(f"Loaded {len(all_steps)} steps. {len(execution_queue)} enabled.")

            if not execution_queue:
                log.info("No enabled steps found. Exiting.")
                return

        except Exception as e:
            log.critical(f"Failed to load manifest: {e}")
            self.notifier.send_alert("Orchestrator Failure", f"Failed to load manifest: {e}")
            sys.exit(1)

        # 2. Build grouped plan (consecutive steps with same transaction_group collapse into one entry)
        plan_items = []
        i = 0
        while i < len(execution_queue):
            step = execution_queue[i]
            group = step.get('transaction_group')

            if group is not None:
                group_steps = []
                while i < len(execution_queue) and execution_queue[i].get('transaction_group') == group:
                    group_steps.append(execution_queue[i])
                    i += 1
                plan_items.append(('group', group, group_steps))
            else:
                plan_items.append(('step', None, [step]))
                i += 1

        # 3. Print Plan
        print("\n--- Execution Plan ---")
        for idx, (item_type, group_id, steps) in enumerate(plan_items, 1):
            if item_type == 'group':
                descs = [s['description'] for s in steps if s.get('description')]
                desc = f"\n     {descs[0]}" if descs else ""
                print(f"[{idx}] GROUP {group_id} ({len(steps)} steps){desc}")
            else:
                step = steps[0]
                s_type = step.get('type', 'UNK').upper()
                s_name = step.get('name', 'Unnamed')
                s_mode = step.get('cleanup_mode', 'drop') if step.get('cleanup_target') else ''
                s_cleanup = f" - Cleanup({s_mode}): {step['cleanup_target']}" if step.get('cleanup_target') else ""
                s_desc = f"\n     {step['description']}" if step.get('description') else ""
                print(f"[{idx}] {s_type}: {s_name}{s_cleanup}{s_desc}")
        print(f"--- {len(plan_items)} tasks ({len(execution_queue)} steps) ---\n")

        if self.dry_run:
            log.info("Dry run complete. Exiting.")
            return

        # 4. User Confirmation
        if not self.force:
            if not self._get_user_confirmation(len(plan_items), len(execution_queue)):
                log.info("Execution aborted by user.")
                sys.exit(0)
        else:
            log.info("Force flag detected. Skipping user confirmation.")

        # 5. Active Execution Loop
        self.notifier.send_alert("Job Started", f"Starting execution of {len(plan_items)} tasks ({len(execution_queue)} steps).")

        job_start_time = time.time()
        executed_steps = []

        db_manager = None

        try:
            db_manager = DatabaseManager(self.db_url)
            executed_steps = self._run_steps(
                execution_queue, db_manager, self.yaml_manager, notify=True
            )

            # Generate Report
            self.reporter.generate_report(db_info=self.db_config.get('dialect', 'Unknown'))

            log.info("All tasks completed successfully.")

            total_duration = time.time() - job_start_time
            steps_summary = self._format_steps_summary(executed_steps)

            summary = (
                f"Job finished successfully.\n\n"
                f"**Total Duration:** {total_duration:.2f}s\n\n"
                f"**Executed tasks:**\n{steps_summary if steps_summary else 'None'}"
            )

            self.notifier.send_alert("Job Finished", summary)

        except Exception as e:
            log.critical(f"Execution failed: {e}")

            total_duration = time.time() - job_start_time
            steps_summary = self._format_steps_summary(executed_steps)
            failed_step = getattr(e, 'failed_step', 'Unknown')

            summary = (
                f"Job execution stopped.\n\n"
                f"**Failed Step:** {failed_step}\n"
                f"**Total Duration:** {total_duration:.2f}s\n\n"
                f"**Executed tasks:**\n{steps_summary if steps_summary else 'None'}"
            )

            self.notifier.send_alert("Job Failed", summary)
            sys.exit(1)
        finally:
            if db_manager:
                db_manager.close()

    def _run_steps(self, execution_queue, db_manager, yaml_manager, notify=True):
        """
        Execute a sequence of steps with transaction management.

        Args:
            execution_queue (list): Steps to execute.
            db_manager (DatabaseManager): Active DB connection manager.
            yaml_manager (YamlManager): Manifest manager for disabling completed steps.
            notify (bool): If True, send per-step Discord notifications.

        Returns:
            list: Executed step records [{"name", "duration", "group"}, ...].
        """
        executed_steps = []
        current_session = None
        current_group = None

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
                    mode = step.get('cleanup_mode', 'drop')
                    if mode == 'truncate':
                        db_manager.truncate_table(step['cleanup_target'], current_session)
                    else:
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
                elif step_type == 'manifest':
                    # Child manifests manage their own transactions.
                    if current_session:
                        current_session.commit()
                        current_session.close()
                        current_session = None
                        current_group = None

                    self._execute_manifest_step(step, db_manager)
                else:
                    log.warning(f"Unknown step type: {step_type}")
                    continue

                duration = time.time() - start_time
                executed_steps.append({"name": step_name, "duration": duration, "group": step_group})

                # Post-Process
                yaml_manager.disable_step(step_name)

                if notify and (step.get('notify') or duration > 5):
                    desc = f"\n{step['description']}" if step.get('description') else ""
                    self.notifier.send_alert(
                        "Step Completed",
                        f"Step '{step_name}' completed in {duration:.2f}s.{desc}",
                        ping=step.get('ping_on_end')
                    )

            except Exception as e:
                log.error(f"Error in step '{step_name}': {e}")
                if current_session:
                    current_session.rollback()
                    current_session.close()
                    current_session = None
                if notify:
                    self.notifier.send_alert(
                        "Step Failed",
                        f"Step '{step_name}' failed: {e}",
                        ping=step.get('ping_on_error')
                    )
                # Preserve the most granular failed step name
                if not hasattr(e, 'failed_step'):
                    e.failed_step = step_name
                raise

        # Final Commit for any open session
        if current_session:
            current_session.commit()
            current_session.close()

        return executed_steps

    def _execute_manifest_step(self, step, db_manager):
        """Handles manifest-type step: loads and executes a child manifest."""
        file_path = Path(step['file'])
        if not file_path.exists():
            raise FileNotFoundError(f"Child manifest not found: {file_path}")

        child_yaml = YamlManager(file_path)
        child_manifest = child_yaml.load_manifest()
        child_steps = child_manifest.get('steps', [])

        if self.enable_all:
            child_queue = child_steps
        else:
            child_queue = [s for s in child_steps if s.get('enabled', True)]

        log.info(f"Executing child manifest: {file_path} ({len(child_queue)}/{len(child_steps)} steps enabled)")

        # Child manifests run with notify=False — the parent step handles notifications.
        self._run_steps(child_queue, db_manager, child_yaml, notify=False)

    def _execute_sql_step(self, step, db_manager, session):
        """Handles SQL/PLSQL execution with retries and profiling."""
        file_path = Path(step['file'])
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
                    wait = 5 * 2 ** attempt
                    log.warning(f"Step '{step['name']}' failed. Retrying in {wait}s... Error: {e}")
                    time.sleep(wait)
                else:
                    log.error(f"Step '{step['name']}' failed after {retries} retries.")
                    raise

    def _execute_python_step(self, step):
        """Handles external Python script execution."""
        file_path = Path(step['file'])
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

    def _format_steps_summary(self, executed_steps):
        """Groups executed steps by transaction_group for summary display."""
        lines = []
        task_idx = 0
        i = 0
        while i < len(executed_steps):
            s = executed_steps[i]
            group = s.get('group')
            task_idx += 1

            if group is not None:
                group_steps = []
                while i < len(executed_steps) and executed_steps[i].get('group') == group:
                    group_steps.append(executed_steps[i])
                    i += 1
                total = sum(gs['duration'] for gs in group_steps)
                lines.append(f"Task {task_idx} - Group {group} ({len(group_steps)} steps) - time taken: {total:.2f}s")
            else:
                lines.append(f"Task {task_idx} - {s['name']} - time taken: {s['duration']:.2f}s")
                i += 1

        return "\n".join(lines)

    def _get_user_confirmation(self, task_count, step_count):
        """Prompts user for confirmation."""
        while True:
            response = input(f"Plan loaded with {task_count} tasks ({step_count} steps). Are you sure you want to execute? [y/N]: ").lower().strip()
            if response in ['y', 'yes']:
                return True
            if response in ['n', 'no', '']:
                return False