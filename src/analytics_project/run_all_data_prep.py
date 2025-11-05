"""
Master Data Preparation Pipeline

Orchestrates all data preparation scripts in the correct order.
Run this to process all raw data files at once.

Usage:
    python src/analytics_project/run_all_data_prep.py

This script will:
1. Execute prepare_customers.py
2. Execute prepare_products.py
3. Execute prepare_sales.py
4. Provide a summary of results
"""

import subprocess
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).resolve().parent))

from utils_logger import logger, init_logger


def run_data_prep_pipeline():
    """Execute all data preparation scripts in sequence."""
    init_logger()

    start_time = datetime.now()

    logger.info("=" * 80)
    logger.info("STARTING MASTER DATA PREPARATION PIPELINE")
    logger.info("=" * 80)
    logger.info(f"Pipeline started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("")

    # Define scripts in execution order
    scripts = [
        ("prepare_customers.py", "Customer Data Preparation"),
        ("prepare_products.py", "Product Data Preparation"),
        ("prepare_sales.py", "Sales Data Preparation"),
    ]

    scripts_dir = Path(__file__).parent / "data_preparation"
    results = {}

    # Execute each script
    for script_name, description in scripts:
        script_path = scripts_dir / script_name

        logger.info("=" * 80)
        logger.info(f"EXECUTING: {description}")
        logger.info("=" * 80)
        logger.info(f"Script: {script_name}")
        logger.info(f"Path: {script_path}")
        logger.info("")

        try:
            # Run the script as a subprocess
            result = subprocess.run(
                [sys.executable, str(script_path)],
                capture_output=True,
                text=True,
                check=True,
                cwd=Path(__file__).parent.parent.parent,  # Project root
            )

            results[script_name] = {
                "status": "SUCCESS",
                "description": description,
                "output": result.stdout,
            }

            logger.info(f"✅ {script_name} completed successfully")
            logger.info("")

            # Log key output lines (last 10 lines of output)
            output_lines = result.stdout.strip().split('\n')
            if len(output_lines) > 10:
                logger.info("Last 10 lines of output:")
                for line in output_lines[-10:]:
                    logger.info(f"  {line}")
            else:
                logger.info("Output:")
                for line in output_lines:
                    logger.info(f"  {line}")

            logger.info("")

        except subprocess.CalledProcessError as e:
            results[script_name] = {
                "status": "FAILED",
                "description": description,
                "error": e.stderr,
            }

            logger.error(f"❌ {script_name} FAILED")
            logger.error("Error output:")
            logger.error(e.stderr)
            logger.info("")

        except FileNotFoundError:
            results[script_name] = {
                "status": "NOT FOUND",
                "description": description,
                "error": f"Script not found at {script_path}",
            }

            logger.error(f"❌ {script_name} NOT FOUND")
            logger.error(f"Expected location: {script_path}")
            logger.info("")

    # Calculate execution time
    end_time = datetime.now()
    execution_time = end_time - start_time

    # Print summary
    logger.info("")
    logger.info("=" * 80)
    logger.info("PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Pipeline completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Total execution time: {execution_time}")
    logger.info("")

    # Detailed results
    success_count = 0
    failed_count = 0

    for script_name, result_info in results.items():
        status = result_info["status"]
        description = result_info["description"]

        if status == "SUCCESS":
            status_icon = "✅"
            success_count += 1
        else:
            status_icon = "❌"
            failed_count += 1

        logger.info(f"{status_icon} {description}")
        logger.info(f"   Script: {script_name}")
        logger.info(f"   Status: {status}")
        logger.info("")

    # Overall summary
    total_scripts = len(scripts)
    logger.info("-" * 80)
    logger.info(f"Total Scripts: {total_scripts}")
    logger.info(f"Successful: {success_count}")
    logger.info(f"Failed: {failed_count}")
    logger.info(f"Success Rate: {(success_count / total_scripts) * 100:.1f}%")
    logger.info("=" * 80)

    # Return exit code based on results
    if failed_count > 0:
        logger.warning("⚠️  Pipeline completed with errors!")
        return 1
    else:
        logger.info("🎉 Pipeline completed successfully!")
        return 0


def main():
    """Main entry point for the pipeline."""
    try:
        exit_code = run_data_prep_pipeline()
        sys.exit(exit_code)
    except Exception as e:
        logger.error("=" * 80)
        logger.error("CRITICAL ERROR IN PIPELINE EXECUTION")
        logger.error("=" * 80)
        logger.error(f"Error: {str(e)}")
        logger.error("Pipeline execution aborted.")
        sys.exit(1)


if __name__ == "__main__":
    main()
