import argparse
import sys
import logging
from pathlib import Path

from config.settings import load_settings
from config.logging_config import setup_logging
from src.executor import Executor

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
    
    args = parser.parse_args()

    # 3. Load Configuration
    try:
        settings = load_settings()
        db_config = settings['db']
        telegram_config = settings['telegram']
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
            notifier_config=telegram_config,
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