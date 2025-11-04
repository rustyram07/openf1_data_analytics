#!/bin/bash
# Helper script to run dbt with proper environment variables

# Activate virtual environment
source ../.venv/bin/activate

# Set Databricks environment variables for dbt
export DBT_DATABRICKS_HOST="XXXXXXXX.cloud.databricks.com"
export DBT_DATABRICKS_TOKEN="XXXXX"
export DBT_DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/XXXXXXXX"

# Run dbt command (pass all arguments to dbt)
dbt "$@"
