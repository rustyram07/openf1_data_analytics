"""
Schema Definitions for Bronze Layer Tables
Defines PySpark schemas for all F1 data entities.
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType, ArrayType, LongType, TimestampType
)


class BronzeSchemas:
    """Schema definitions for all bronze layer tables"""

    @staticmethod
    def get_sessions_schema() -> StructType:
        """Schema for sessions data"""
        return StructType([
            StructField("session_key", IntegerType(), True),
            StructField("session_name", StringType(), True),
            StructField("date_start", StringType(), True),
            StructField("date_end", StringType(), True),
            StructField("gmt_offset", StringType(), True),
            StructField("session_type", StringType(), True),
            StructField("meeting_key", IntegerType(), True),
            StructField("location", StringType(), True),
            StructField("country_key", IntegerType(), True),
            StructField("country_code", StringType(), True),
            StructField("country_name", StringType(), True),
            StructField("circuit_key", IntegerType(), True),
            StructField("circuit_short_name", StringType(), True),
            StructField("year", IntegerType(), True),
        ])

    @staticmethod
    def get_drivers_schema() -> StructType:
        """Schema for drivers data"""
        return StructType([
            StructField("session_key", IntegerType(), True),
            StructField("meeting_key", IntegerType(), True),
            StructField("driver_number", IntegerType(), True),
            StructField("broadcast_name", StringType(), True),
            StructField("full_name", StringType(), True),
            StructField("name_acronym", StringType(), True),
            StructField("team_name", StringType(), True),
            StructField("team_colour", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("headshot_url", StringType(), True),
            StructField("country_code", StringType(), True),
        ])

    @staticmethod
    def get_laps_schema() -> StructType:
        """Schema for laps data"""
        return StructType([
            StructField("session_key", IntegerType(), True),
            StructField("meeting_key", IntegerType(), True),
            StructField("driver_number", IntegerType(), True),
            StructField("lap_number", IntegerType(), True),
            StructField("date_start", StringType(), True),
            StructField("lap_duration", DoubleType(), True),
            StructField("is_pit_out_lap", BooleanType(), True),
            StructField("duration_sector_1", DoubleType(), True),
            StructField("duration_sector_2", DoubleType(), True),
            StructField("duration_sector_3", DoubleType(), True),
            StructField("i1_speed", IntegerType(), True),
            StructField("i2_speed", IntegerType(), True),
            StructField("st_speed", IntegerType(), True),
            StructField("segments_sector_1", ArrayType(IntegerType()), True),
            StructField("segments_sector_2", ArrayType(IntegerType()), True),
            StructField("segments_sector_3", ArrayType(IntegerType()), True),
            StructField("batch_day", StringType(), True),
            StructField("session_name", StringType(), True),
        ])

    @staticmethod
    def get_locations_schema() -> StructType:
        """Schema for locations data"""
        return StructType([
            StructField("session_key", IntegerType(), True),
            StructField("meeting_key", IntegerType(), True),
            StructField("driver_number", IntegerType(), True),
            StructField("date", StringType(), True),
            StructField("x", IntegerType(), True),
            StructField("y", IntegerType(), True),
            StructField("z", IntegerType(), True),
            StructField("batch_day", StringType(), True),
            StructField("session_name", StringType(), True),
        ])

    @classmethod
    def get_schema(cls, entity: str) -> StructType:
        """Get schema for a specific entity"""
        schema_methods = {
            "sessions": cls.get_sessions_schema,
            "drivers": cls.get_drivers_schema,
            "laps": cls.get_laps_schema,
            "locations": cls.get_locations_schema
        }

        method = schema_methods.get(entity)
        if method:
            return method()
        else:
            raise ValueError(f"Unknown entity: {entity}")


# Export singleton instance
schemas = BronzeSchemas()
