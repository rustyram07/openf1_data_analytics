"""
Dagster Pipeline for F1 Data Analytics
Orchestrates the complete end-to-end pipeline: API → Bronze → Silver → Gold

This is a standalone orchestration script that calls existing code without modification.
Can be scheduled and monitored through Dagster UI.

Installation:
    pip install dagster dagster-webserver dagster-shell

Usage:
    # Start Dagster UI
    dagster dev -f dagster_pipeline.py

    # Access UI at http://localhost:3000
    # Click "Materialize All" to run the pipeline

Features:
    - Schedule daily/hourly runs
    - Monitor pipeline execution
    - View logs and metrics
    - Retry failed steps
    - Send alerts on failures
"""

import os
import subprocess
from pathlib import Path
from typing import Dict, Any

from dagster import (
    asset,
    AssetExecutionContext,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    AssetSelection,
    sensor,
    RunRequest,
    SensorEvaluationContext,
    DefaultScheduleStatus,
    Output,
    MetadataValue,
)


# Configuration
PROJECT_ROOT = Path.home() / "Documents" / "openf1_data_analytics"
VENV_PATH = PROJECT_ROOT / ".venv" / "bin" / "activate"


def run_shell_command(command: str, cwd: Path = PROJECT_ROOT) -> Dict[str, Any]:
    """
    Execute shell command and return results

    Returns:
        Dict with stdout, stderr, returncode, and success status
    """
    # Activate virtual environment in command
    full_command = f"source {VENV_PATH} && {command}"

    result = subprocess.run(
        full_command,
        shell=True,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        executable="/bin/bash"
    )

    return {
        "stdout": result.stdout,
        "stderr": result.stderr,
        "returncode": result.returncode,
        "success": result.returncode == 0
    }


# =============================================================================
# ASSETS - Represent data artifacts in the pipeline
# =============================================================================

@asset(
    group_name="extraction",
    description="Extract F1 session data from OpenF1 API to local JSON files",
    compute_kind="python"
)
def api_extracted_data(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Step 1: Extract data from OpenF1 API

    Extracts latest session(s) from OpenF1 API and saves to local JSON files.
    Can be configured to extract oldest/latest/year sessions.
    """
    context.log.info("Starting API extraction...")

    # Configuration - modify these as needed
    mode = "latest"  # Options: "latest", "oldest", "year"
    num_sessions = 1

    # Build command based on mode
    extraction_dir = PROJECT_ROOT / "openf1_api_data_extractor" / "src" / "ingestion"

    if mode == "year":
        command = f"python extract_sessions.py --mode year --year 2024"
    elif mode == "oldest":
        command = f"python extract_sessions.py --order oldest --num-sessions {num_sessions}"
    else:
        command = f"python extract_sessions.py --order latest --num-sessions {num_sessions}"

    context.log.info(f"Executing: {command}")
    result = run_shell_command(command, cwd=extraction_dir)

    if not result["success"]:
        context.log.error(f"API extraction failed: {result['stderr']}")
        raise Exception(f"API extraction failed: {result['stderr']}")

    context.log.info("API extraction completed successfully")
    context.log.info(result["stdout"][-500:])  # Log last 500 chars

    # Count extracted files
    local_data_dir = PROJECT_ROOT / "local_test_data"
    json_files = list(local_data_dir.glob("*.json"))

    return Output(
        value={
            "status": "success",
            "files_extracted": len(json_files),
            "mode": mode,
            "num_sessions": num_sessions
        },
        metadata={
            "files_count": MetadataValue.int(len(json_files)),
            "extraction_mode": MetadataValue.text(mode),
            "sessions_extracted": MetadataValue.int(num_sessions),
            "output_log": MetadataValue.text(result["stdout"][-1000:])
        }
    )


@asset(
    group_name="bronze",
    description="Load JSON files to Bronze layer Delta tables in Databricks",
    compute_kind="python",
    deps=[api_extracted_data]
)
def bronze_tables_loaded(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Step 2: Load to Bronze Layer

    Reads JSON files from local storage and loads them to Bronze Delta tables
    in Databricks (bronze_raw schema).
    """
    context.log.info("Starting Bronze layer load...")

    bronze_loader_dir = PROJECT_ROOT / "openf1_api_data_extractor" / "dlt_bronze_load"
    command = "python run_batch_load.py"

    context.log.info(f"Executing: {command}")
    result = run_shell_command(command, cwd=bronze_loader_dir)

    if not result["success"]:
        context.log.error(f"Bronze load failed: {result['stderr']}")
        raise Exception(f"Bronze load failed: {result['stderr']}")

    context.log.info("Bronze layer loaded successfully")
    context.log.info(result["stdout"][-500:])

    # Parse output for record counts (rough estimation)
    output = result["stdout"]
    tables_loaded = output.count("Loaded") + output.count("records to")

    return Output(
        value={
            "status": "success",
            "tables_loaded": 4,  # sessions, drivers, laps, locations
            "schema": "dev_f1_data_analytics.bronze_raw"
        },
        metadata={
            "tables_count": MetadataValue.int(4),
            "schema": MetadataValue.text("dev_f1_data_analytics.bronze_raw"),
            "output_log": MetadataValue.text(result["stdout"][-1000:])
        }
    )


@asset(
    group_name="silver",
    description="Build Silver layer tables (cleaned and conformed data)",
    compute_kind="dbt",
    deps=[bronze_tables_loaded]
)
def silver_tables_built(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Step 3: Build Silver Layer

    Runs dbt to build silver layer tables with:
    - Deduplication
    - Data cleaning
    - Business logic
    - Incremental processing
    """
    context.log.info("Starting Silver layer build...")

    dbt_dir = PROJECT_ROOT / "dbt_f1_data_analytics"
    command = "./run_dbt.sh build --select silver_clean --full-refresh"

    context.log.info(f"Executing: {command}")
    result = run_shell_command(command, cwd=dbt_dir)

    if not result["success"]:
        context.log.error(f"Silver build failed: {result['stderr']}")
        raise Exception(f"Silver build failed: {result['stderr']}")

    context.log.info("Silver layer built successfully")

    # Parse dbt output for metrics
    output = result["stdout"]
    pass_count = output.count("OK created") + output.count("PASS")

    return Output(
        value={
            "status": "success",
            "models_built": 4,
            "schema": "dev_f1_data_analytics.default_silver_clean",
            "tests_passed": pass_count
        },
        metadata={
            "models_count": MetadataValue.int(4),
            "schema": MetadataValue.text("dev_f1_data_analytics.default_silver_clean"),
            "tests_passed": MetadataValue.int(pass_count),
            "output_log": MetadataValue.text(result["stdout"][-1000:])
        }
    )


@asset(
    group_name="gold",
    description="Build Gold core tables (dimensions and facts)",
    compute_kind="dbt",
    deps=[silver_tables_built]
)
def gold_core_tables_built(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Step 4a: Build Gold Core (Dimensions & Facts)

    Runs dbt to build:
    - dim_sessions (session dimension)
    - dim_drivers (driver dimension with SCD Type 2)
    - fact_laps (lap-level facts)
    """
    context.log.info("Starting Gold core build...")

    dbt_dir = PROJECT_ROOT / "dbt_f1_data_analytics"
    command = "./run_dbt.sh build --select gold_analytics.core --full-refresh"

    context.log.info(f"Executing: {command}")
    result = run_shell_command(command, cwd=dbt_dir)

    if not result["success"]:
        context.log.error(f"Gold core build failed: {result['stderr']}")
        raise Exception(f"Gold core build failed: {result['stderr']}")

    context.log.info("Gold core built successfully")

    # Parse dbt output
    output = result["stdout"]
    pass_count = output.count("OK created") + output.count("PASS")

    return Output(
        value={
            "status": "success",
            "models_built": 3,  # dim_sessions, dim_drivers, fact_laps
            "schema": "dev_f1_data_analytics.default_gold_analytics"
        },
        metadata={
            "models_count": MetadataValue.int(3),
            "schema": MetadataValue.text("dev_f1_data_analytics.default_gold_analytics"),
            "tests_passed": MetadataValue.int(pass_count),
            "output_log": MetadataValue.text(result["stdout"][-1000:])
        }
    )


@asset(
    group_name="gold",
    description="Build Gold mart tables (pre-aggregated analytics)",
    compute_kind="dbt",
    deps=[gold_core_tables_built]
)
def gold_mart_tables_built(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
    """
    Step 4b: Build Gold Marts

    Runs dbt to build analytics-ready mart tables:
    - session_leaderboard (driver rankings)
    - driver_session_summary (performance metrics)
    - team_performance (team aggregations)
    """
    context.log.info("Starting Gold marts build...")

    dbt_dir = PROJECT_ROOT / "dbt_f1_data_analytics"
    command = "./run_dbt.sh run --select session_leaderboard driver_session_summary team_performance --full-refresh"

    context.log.info(f"Executing: {command}")
    result = run_shell_command(command, cwd=dbt_dir)

    if not result["success"]:
        context.log.error(f"Gold marts build failed: {result['stderr']}")
        raise Exception(f"Gold marts build failed: {result['stderr']}")

    context.log.info("Gold marts built successfully")

    # Parse dbt output
    output = result["stdout"]
    pass_count = output.count("OK created") + output.count("PASS")

    return Output(
        value={
            "status": "success",
            "models_built": 3,  # session_leaderboard, driver_session_summary, team_performance
            "schema": "dev_f1_data_analytics.default_gold_analytics",
            "pipeline_complete": True
        },
        metadata={
            "models_count": MetadataValue.int(3),
            "schema": MetadataValue.text("dev_f1_data_analytics.default_gold_analytics"),
            "tests_passed": MetadataValue.int(pass_count),
            "pipeline_status": MetadataValue.text("COMPLETE"),
            "output_log": MetadataValue.text(result["stdout"][-1000:])
        }
    )


# =============================================================================
# JOBS - Define how assets are executed together
# =============================================================================

# Full pipeline job - runs all assets in order
f1_pipeline_job = define_asset_job(
    name="f1_data_pipeline",
    description="Complete F1 data pipeline: API → Bronze → Silver → Gold",
    selection=AssetSelection.all()
)

# Individual layer jobs for targeted runs
extraction_job = define_asset_job(
    name="api_extraction",
    description="Extract data from OpenF1 API only",
    selection=AssetSelection.groups("extraction")
)

bronze_job = define_asset_job(
    name="bronze_load",
    description="Load to Bronze layer only",
    selection=AssetSelection.groups("bronze")
)

silver_job = define_asset_job(
    name="silver_build",
    description="Build Silver layer only",
    selection=AssetSelection.groups("silver")
)

gold_job = define_asset_job(
    name="gold_build",
    description="Build Gold layer only",
    selection=AssetSelection.groups("gold")
)


# =============================================================================
# SCHEDULES - Define when jobs run automatically
# =============================================================================

# Daily pipeline run at 2 AM
daily_pipeline_schedule = ScheduleDefinition(
    name="daily_f1_pipeline",
    job=f1_pipeline_job,
    cron_schedule="0 2 * * *",  # 2 AM daily
    description="Run full F1 pipeline daily at 2 AM",
    default_status=DefaultScheduleStatus.STOPPED  # Start manually via UI
)

# Hourly extraction (for real-time updates during race weekends)
hourly_extraction_schedule = ScheduleDefinition(
    name="hourly_api_extraction",
    job=extraction_job,
    cron_schedule="0 * * * *",  # Every hour
    description="Extract latest session data hourly",
    default_status=DefaultScheduleStatus.STOPPED
)

# Weekly full refresh (rebuild everything from scratch)
weekly_full_refresh_schedule = ScheduleDefinition(
    name="weekly_full_refresh",
    job=f1_pipeline_job,
    cron_schedule="0 3 * * 0",  # 3 AM every Sunday
    description="Full pipeline refresh every Sunday",
    default_status=DefaultScheduleStatus.STOPPED
)


# =============================================================================
# SENSORS - Trigger jobs based on events
# =============================================================================

@sensor(
    name="new_session_sensor",
    description="Trigger pipeline when new session data is detected",
    minimum_interval_seconds=300,  # Check every 5 minutes
    job=f1_pipeline_job
)
def new_session_file_sensor(context: SensorEvaluationContext):
    """
    Monitor local_test_data directory for new JSON files.
    Trigger pipeline when new files appear.
    """
    local_data_dir = PROJECT_ROOT / "local_test_data"

    # Get list of JSON files
    json_files = list(local_data_dir.glob("*.json"))

    if not json_files:
        return

    # Get most recent file modification time
    latest_file = max(json_files, key=lambda f: f.stat().st_mtime)
    latest_mtime = latest_file.stat().st_mtime

    # Check cursor (last processed time)
    last_mtime = context.cursor or "0"

    if str(latest_mtime) != last_mtime:
        context.log.info(f"New file detected: {latest_file.name}")
        yield RunRequest(
            run_key=str(latest_mtime),
            run_config={},
        )
        context.update_cursor(str(latest_mtime))


# =============================================================================
# DEFINITIONS - Bundle everything for Dagster
# =============================================================================

defs = Definitions(
    assets=[
        api_extracted_data,
        bronze_tables_loaded,
        silver_tables_built,
        gold_core_tables_built,
        gold_mart_tables_built,
    ],
    jobs=[
        f1_pipeline_job,
        extraction_job,
        bronze_job,
        silver_job,
        gold_job,
    ],
    schedules=[
        daily_pipeline_schedule,
        hourly_extraction_schedule,
        weekly_full_refresh_schedule,
    ],
    sensors=[
        new_session_file_sensor,
    ],
)


# =============================================================================
# CONFIGURATION EXAMPLES
# =============================================================================

"""
CONFIGURATION OPTIONS:

1. Modify extraction mode in api_extracted_data asset:
   - mode = "latest" / "oldest" / "year"
   - num_sessions = 1, 5, 10, etc.

2. Change schedule times in ScheduleDefinition:
   - cron_schedule="0 2 * * *" → "0 14 * * *" (2 PM)
   - See: https://crontab.guru/

3. Enable/disable full refresh:
   - Replace --full-refresh with incremental mode
   - Remove --full-refresh from dbt commands

4. Add alerting (requires dagster-slack or dagster-pagerduty):
   from dagster_slack import slack_on_failure

   @slack_on_failure(slack_token=..., channel=...)
   def alert_on_failure(...):
       pass

RUNNING THE PIPELINE:

1. Install Dagster:
   pip install dagster dagster-webserver dagster-shell

2. Start Dagster UI:
   dagster dev -f dagster_pipeline.py

3. Access UI:
   http://localhost:3000

4. Run pipeline:
   - Click "Assets" → "Materialize All"
   - Or click "Jobs" → "f1_data_pipeline" → "Launch Run"

5. Enable schedules:
   - Click "Schedules" → Select schedule → Toggle "On"

6. Monitor runs:
   - Click "Runs" to see execution history
   - Click individual runs for detailed logs
   - View lineage in "Asset Lineage" tab

PRODUCTION DEPLOYMENT:

1. Deploy to Dagster Cloud:
   dagster-cloud workspace sync

2. Or deploy with Docker:
   docker build -t f1-dagster .
   docker run -p 3000:3000 f1-dagster

3. Or deploy with Kubernetes:
   helm install dagster dagster/dagster
"""
