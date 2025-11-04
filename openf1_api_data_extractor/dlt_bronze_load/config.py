"""
Configuration for DLT Bronze Layer Pipeline
Centralized configuration for paths, catalogs, schemas, and pipeline settings.
"""

import os
from typing import Dict, List

class BronzeConfig:
    """Configuration class for Bronze Layer pipeline"""

    # Databricks catalog and schema
    BRONZE_CATALOG = "dev_f1_data_analytics"
    BRONZE_SCHEMA = "bronze_raw"

    # Volume paths (for production DLT pipeline with streaming)
    LANDING_VOLUME_PATH = "/Volumes/dev_f1_data_analytics/raw_data/landing"

    # Local data directory (for development/batch loading)
    LOCAL_DATA_DIR = "/Users/ramamoorthysrinivasagam/Documents/openf1_data_analytics/local_test_data"

    # Schema locations for CloudFiles
    SCHEMA_LOCATION_BASE = f"{LANDING_VOLUME_PATH}/_schemas"

    # Base table properties for all bronze tables
    BRONZE_TABLE_PROPERTIES = {
        "quality": "bronze",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }

    # CloudFiles settings for streaming ingestion
    CLOUDFILES_OPTIONS = {
        "cloudFiles.format": "json",
        "cloudFiles.inferColumnTypes": "true",
        "cloudFiles.schemaEvolutionMode": "addNewColumns"
    }

    # Data source file patterns
    FILE_PATTERNS = {
        "sessions": "sessions*.json",
        "drivers": "drivers*.json",
        "laps": "laps*.json",
        "locations": "locations*.json"
    }

    # Z-Order columns for optimization
    ZORDER_COLUMNS = {
        "sessions": ["session_key", "date_start"],
        "drivers": ["session_key", "driver_number"],
        "laps": ["session_key", "driver_number", "lap_number"],
        "locations": ["session_key", "driver_number", "date"]
    }

    # Table comments
    TABLE_COMMENTS = {
        "sessions": "Raw sessions data from OpenF1 API",
        "drivers": "Raw drivers data from OpenF1 API",
        "laps": "Raw lap timing data from OpenF1 API",
        "locations": "Raw location telemetry data from OpenF1 API"
    }

    # Batch processing settings (for large datasets like locations)
    BATCH_SIZE = 100000  # Records per batch for locations table

    @classmethod
    def get_full_table_name(cls, table_name: str) -> str:
        """Get fully qualified table name"""
        return f"{cls.BRONZE_CATALOG}.{cls.BRONZE_SCHEMA}.{table_name}"

    @classmethod
    def get_schema_location(cls, entity: str) -> str:
        """Get schema location for CloudFiles"""
        return f"{cls.SCHEMA_LOCATION_BASE}/{entity}"

    @classmethod
    def get_table_properties(cls, entity: str) -> Dict[str, str]:
        """Get table properties including z-order columns"""
        props = cls.BRONZE_TABLE_PROPERTIES.copy()
        if entity in cls.ZORDER_COLUMNS:
            props["pipelines.autoOptimize.zOrderCols"] = ",".join(cls.ZORDER_COLUMNS[entity])
        return props

    @classmethod
    def get_file_pattern(cls, entity: str) -> str:
        """Get file pattern for specific entity"""
        return cls.FILE_PATTERNS.get(entity, f"{entity}*.json")

    @classmethod
    def get_table_comment(cls, entity: str) -> str:
        """Get table comment for specific entity"""
        return cls.TABLE_COMMENTS.get(entity, f"Raw {entity} data")

    @classmethod
    def get_local_files(cls, entity: str) -> List[str]:
        """Get list of local files for a specific entity"""
        import glob
        pattern = os.path.join(cls.LOCAL_DATA_DIR, cls.get_file_pattern(entity))
        return glob.glob(pattern)


# Export singleton instance
config = BronzeConfig()
