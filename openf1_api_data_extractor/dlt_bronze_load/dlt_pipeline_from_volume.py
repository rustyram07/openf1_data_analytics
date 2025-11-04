"""
Delta Live Tables (DLT) Pipeline - Load from Databricks Volume
Production-ready DLT pipeline that loads bronze tables from landing volume
Uses Auto Loader with schema evolution and proper JSON handling

This is the RECOMMENDED approach for production:
- Incremental processing
- Auto Loader handles new files automatically
- Schema evolution support
- Better performance than batch loading
"""

import dlt
from pyspark.sql.functions import current_timestamp, col
from pyspark.sql.types import *

# Configuration
LANDING_VOLUME_PATH = "/Volumes/dev_f1_data_analytics/raw_data/landing"
BRONZE_CATALOG = "dev_f1_data_analytics"
BRONZE_SCHEMA = "bronze_raw_v2"  # Use v2 to not interfere with existing tables


# ============================================================================
# Bronze Tables - Raw JSON data from landing volume
# ============================================================================

@dlt.table(
    name="bronze_sessions_dlt",
    comment="Raw F1 session data from OpenF1 API - loaded via DLT",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "session_key,date_start"
    }
)
def bronze_sessions_dlt():
    """
    Load sessions data from volume using Auto Loader
    Schema is automatically inferred from JSON
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", f"{LANDING_VOLUME_PATH}/_schemas/sessions")
        .load(f"{LANDING_VOLUME_PATH}/sessions*.json")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )


@dlt.table(
    name="bronze_drivers_dlt",
    comment="Raw F1 driver data from OpenF1 API - loaded via DLT",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "session_key,driver_number"
    }
)
def bronze_drivers_dlt():
    """
    Load drivers data from volume using Auto Loader
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", f"{LANDING_VOLUME_PATH}/_schemas/drivers")
        .load(f"{LANDING_VOLUME_PATH}/drivers*.json")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )


@dlt.table(
    name="bronze_laps_dlt",
    comment="Raw F1 lap timing data from OpenF1 API - loaded via DLT",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "session_key,driver_number,lap_number"
    }
)
def bronze_laps_dlt():
    """
    Load laps data from volume using Auto Loader
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", f"{LANDING_VOLUME_PATH}/_schemas/laps")
        .load(f"{LANDING_VOLUME_PATH}/laps*.json")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )


@dlt.table(
    name="bronze_locations_dlt",
    comment="Raw F1 GPS telemetry data from OpenF1 API - loaded via DLT",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "session_key,driver_number,date"
    }
)
def bronze_locations_dlt():
    """
    Load locations data from volume using Auto Loader
    Large dataset - Auto Loader processes incrementally
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.schemaLocation", f"{LANDING_VOLUME_PATH}/_schemas/locations")
        .option("cloudFiles.maxFilesPerTrigger", 10)  # Process 10 files at a time
        .load(f"{LANDING_VOLUME_PATH}/locations*.json")
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
    )


# ============================================================================
# How to Use This Pipeline
# ============================================================================
"""
## Setup in Databricks:

1. Create DLT Pipeline in Databricks:
   - Go to Workflows > Delta Live Tables
   - Click "Create Pipeline"
   - Name: "F1 Bronze Layer DLT"
   - Source Code: Point to this file in your workspace/repo
   - Target: dev_f1_data_analytics.bronze_raw_v2
   - Storage Location: /pipelines/f1_bronze_dlt

2. Configuration:
   - Mode: Triggered (for batch) or Continuous (for streaming)
   - Cluster: Serverless (recommended) or Classic
   - Advanced: Enable Auto Loader

3. Run Pipeline:
   - Click "Start" in DLT pipeline UI
   - Monitor progress in real-time
   - View data lineage graph

## Advantages over Batch Loading:

✅ Incremental Processing
   - Only processes new files
   - No need to reload everything

✅ Auto Loader
   - Automatically discovers new files
   - Handles schema evolution
   - Efficient at scale

✅ DLT Features
   - Data quality expectations
   - Automatic retries
   - Lineage tracking
   - Better monitoring

✅ Production Ready
   - Handles failures gracefully
   - Supports continuous updates
   - Integrates with orchestration

## Tables Created:

dev_f1_data_analytics.bronze_raw_v2.bronze_sessions_dlt
dev_f1_data_analytics.bronze_raw_v2.bronze_drivers_dlt
dev_f1_data_analytics.bronze_raw_v2.bronze_laps_dlt
dev_f1_data_analytics.bronze_raw_v2.bronze_locations_dlt

Note: Using _v2 schema to not interfere with existing bronze_raw tables

## Verification Queries:

SELECT * FROM dev_f1_data_analytics.bronze_raw_v2.bronze_sessions_dlt LIMIT 10;
SELECT * FROM dev_f1_data_analytics.bronze_raw_v2.bronze_drivers_dlt LIMIT 10;
SELECT * FROM dev_f1_data_analytics.bronze_raw_v2.bronze_laps_dlt LIMIT 10;
SELECT * FROM dev_f1_data_analytics.bronze_raw_v2.bronze_locations_dlt LIMIT 100;

## Migration from Existing Tables:

If you want to use these DLT tables in dbt, update your sources.yml:

sources:
  - name: bronze
    database: dev_f1_data_analytics
    schema: bronze_raw_v2  # Change from bronze_raw
    tables:
      - name: bronze_sessions_dlt  # Add _dlt suffix
      - name: bronze_drivers_dlt
      - name: bronze_laps_dlt
      - name: bronze_locations_dlt
"""
