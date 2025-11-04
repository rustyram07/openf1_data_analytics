"""
DLT Table Definitions for Bronze Layer
Defines Delta Live Tables with streaming ingestion from landing zone.
This file is used by the DLT pipeline for streaming/incremental loads.
"""

import dlt
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, to_timestamp
)

from config import config
from schemas import schemas


@dlt.table(
    name="bronze_sessions",
    comment=config.get_table_comment("sessions"),
    table_properties=config.get_table_properties("sessions")
)
def bronze_sessions():
    """
    Read all sessions JSON files from landing zone.
    Incremental load with append-only semantics using Auto Loader (CloudFiles).
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", config.get_schema_location("sessions"))
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", config.get_file_pattern("sessions"))
        .schema(schemas.get_sessions_schema())
        .load(config.LANDING_VOLUME_PATH)
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("date_start_ts", to_timestamp(col("date_start")))
        .withColumn("date_end_ts", to_timestamp(col("date_end")))
    )


@dlt.table(
    name="bronze_drivers",
    comment=config.get_table_comment("drivers"),
    table_properties=config.get_table_properties("drivers")
)
def bronze_drivers():
    """
    Read all drivers JSON files from landing zone.
    Incremental load with append-only semantics using Auto Loader (CloudFiles).
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", config.get_schema_location("drivers"))
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", config.get_file_pattern("drivers"))
        .schema(schemas.get_drivers_schema())
        .load(config.LANDING_VOLUME_PATH)
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingestion_timestamp", current_timestamp())
    )


@dlt.table(
    name="bronze_laps",
    comment=config.get_table_comment("laps"),
    table_properties=config.get_table_properties("laps")
)
def bronze_laps():
    """
    Read all laps JSON files from landing zone.
    Incremental load with append-only semantics using Auto Loader (CloudFiles).
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", config.get_schema_location("laps"))
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", config.get_file_pattern("laps"))
        .schema(schemas.get_laps_schema())
        .load(config.LANDING_VOLUME_PATH)
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("date_start_ts", to_timestamp(col("date_start")))
    )


@dlt.table(
    name="bronze_locations",
    comment=config.get_table_comment("locations"),
    table_properties=config.get_table_properties("locations")
)
def bronze_locations():
    """
    Read all locations JSON files from landing zone.
    Incremental load with append-only semantics using Auto Loader (CloudFiles).
    This is the largest dataset and benefits from z-ordering and partitioning.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", config.get_schema_location("locations"))
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", config.get_file_pattern("locations"))
        .schema(schemas.get_locations_schema())
        .load(config.LANDING_VOLUME_PATH)
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("date_ts", to_timestamp(col("date")))
    )


# Data Quality Expectations

@dlt.expect_or_drop("valid_session_key", "session_key IS NOT NULL")
@dlt.expect_or_drop("valid_driver_number", "driver_number IS NOT NULL")
@dlt.expect("valid_lap_duration", "lap_duration > 0 OR lap_duration IS NULL")
@dlt.table(
    name="bronze_laps_quality",
    comment="Laps data with quality checks applied"
)
def bronze_laps_quality():
    """
    Apply data quality expectations to laps data.
    Drops records with invalid session_key or driver_number.
    Tracks records with invalid lap_duration.
    """
    return dlt.read_stream("bronze_laps")


@dlt.expect_or_drop("valid_coordinates", "x IS NOT NULL AND y IS NOT NULL")
@dlt.table(
    name="bronze_locations_quality",
    comment="Locations data with quality checks applied"
)
def bronze_locations_quality():
    """
    Apply data quality expectations to location data.
    Drops records with missing coordinates.
    """
    return dlt.read_stream("bronze_locations")
