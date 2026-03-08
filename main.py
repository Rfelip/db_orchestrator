import argparse
import sys
import os
import json
import logging
import subprocess
import signal
from pathlib import Path
from datetime import datetime

from config.settings import load_settings
from config.logging_config import setup_logging

JOBS_DIR = Path(__file__).parent.parent.parent / "jobs"


def _spawn_background(args_list):
    """Spawn the orchestrator as a detached background process."""
    JOBS_DIR.mkdir(exist_ok=True)

    # Build the command to re-invoke ourselves without --bg
    cmd = [sys.executable, __file__] + args_list

    # Platform-specific detach
    kwargs = {}
    if sys.platform == "win32":
        DETACHED = 0x00000008  # DETACHED_PROCESS
        CREATE_NO_WINDOW = 0x08000000
        kwargs["creationflags"] = DETACHED | CREATE_NO_WINDOW
    else:
        kwargs["start_new_session"] = True

    # Redirect stdout/stderr to a log file
    log_file = JOBS_DIR / f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    with open(log_file, "w") as lf:
        proc = subprocess.Popen(
            cmd,
            stdout=lf,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            **kwargs
        )

    # Write job metadata
    job_file = JOBS_DIR / f"job_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    job_data = {
        "pid": proc.pid,
        "started": datetime.now().isoformat(),
        "manifest": next((a for i, a in enumerate(args_list) if args_list[i-1] == "--manifest"), "queue/manifest.yaml") if "--manifest" in args_list else "queue/manifest.yaml",
        "log_file": str(log_file),
        "args": args_list,
    }
    job_file.write_text(json.dumps(job_data, indent=2))

    print(f"Background job spawned (PID {proc.pid})")
    print(f"Job file: {job_file}")
    print(f"Log file: {log_file}")
    print(f"Check status: uv run python {__file__} --status")


def _show_status():
    """Show status of background jobs."""
    if not JOBS_DIR.exists():
        print("No jobs directory found. No background jobs have been run.")
        return

    job_files = sorted(JOBS_DIR.glob("*.json"), reverse=True)
    if not job_files:
        print("No background jobs found.")
        return

    for jf in job_files[:5]:  # Show last 5 jobs
        data = json.loads(jf.read_text())
        pid = data["pid"]
        started = data["started"]
        manifest = data["manifest"]

        # Check if process is alive
        alive = _is_pid_alive(pid)
        status = "RUNNING" if alive else "FINISHED"

        # Check log file for errors
        log_path = Path(data.get("log_file", ""))
        has_error = False
        if log_path.exists():
            content = log_path.read_text(errors="replace")
            if "CRITICAL" in content or "failed after" in content:
                has_error = True
                status = "RUNNING" if alive else "FAILED"

        print(f"[{status}] PID={pid}  started={started}  manifest={manifest}")

        # Show tail of log for latest job
        if jf == job_files[0] and log_path.exists():
            lines = content.strip().split("\n")
            tail = lines[-5:] if len(lines) > 5 else lines
            print("  Latest log lines:")
            for line in tail:
                print(f"    {line[:120]}")
        print()


def _kill_job():
    """Kill the latest running background job."""
    if not JOBS_DIR.exists():
        print("No jobs directory found.")
        return

    job_files = sorted(JOBS_DIR.glob("*.json"), reverse=True)
    for jf in job_files:
        data = json.loads(jf.read_text())
        pid = data["pid"]
        if _is_pid_alive(pid):
            try:
                if sys.platform == "win32":
                    subprocess.run(["taskkill", "/PID", str(pid), "/F"], check=True)
                else:
                    os.kill(pid, signal.SIGTERM)
                print(f"Killed job PID={pid} ({data['manifest']})")
            except Exception as e:
                print(f"Failed to kill PID={pid}: {e}")
            return

    print("No running background jobs found.")


def _is_pid_alive(pid):
    """Check if a process with given PID is running."""
    if sys.platform == "win32":
        try:
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}"],
                capture_output=True, text=True
            )
            return str(pid) in result.stdout
        except Exception:
            return False
    else:
        try:
            os.kill(pid, 0)
            return True
        except (ProcessLookupError, PermissionError):
            return False


def main():
    """
    Main entry point for the Database Task Orchestrator CLI.
    """
    # 1. Setup Logging
    setup_logging()
    log = logging.getLogger(__name__)

    # 2. Parse Arguments
    parser = argparse.ArgumentParser(
        description="Database Task Orchestrator: Automate sequential DB tasks."
    )
    parser.add_argument(
        "--manifest",
        type=str,
        default="queue/manifest.yaml",
        help="Path to the manifest YAML file (default: queue/manifest.yaml)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the execution plan without running any tasks."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Skip the user confirmation prompt and execute immediately."
    )
    parser.add_argument(
        "--enable-all",
        "--run-all",
        action="store_true",
        help="Run all tasks in the manifest, ignoring the 'enabled: false' flag."
    )
    parser.add_argument(
        "--bg", "--background",
        action="store_true",
        help="Spawn the orchestrator as a detached background process."
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show status of background jobs."
    )
    parser.add_argument(
        "--kill",
        action="store_true",
        help="Kill the latest running background job."
    )

    args = parser.parse_args()

    # Handle status/kill before anything else
    if args.status:
        _show_status()
        return

    if args.kill:
        _kill_job()
        return

    # Handle background spawn
    if args.bg:
        # Rebuild args without --bg
        spawn_args = []
        for arg in sys.argv[1:]:
            if arg not in ("--bg", "--background"):
                spawn_args.append(arg)
        # Always force in background (no interactive prompt)
        if "--force" not in spawn_args:
            spawn_args.append("--force")
        _spawn_background(spawn_args)
        return

    # 3. Load Configuration
    try:
        settings = load_settings()
        db_config = settings['db']
        notifier_config = settings['notifier']
    except Exception as e:
        log.critical(f"Failed to load configuration: {e}")
        sys.exit(1)

    # 4. Initialize and Run Executor
    manifest_path = Path(args.manifest)
    if not manifest_path.exists():
        log.critical(f"Manifest file not found: {manifest_path}")
        sys.exit(1)

    try:
        executor = Executor(
            manifest_path=manifest_path,
            db_config=db_config,
            notifier_config=notifier_config,
            dry_run=args.dry_run,
            force=args.force,
            enable_all=args.enable_all
        )
        executor.run()

    except Exception as e:
        log.critical(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
