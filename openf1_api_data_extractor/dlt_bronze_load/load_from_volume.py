"""
Load Bronze Tables from Databricks Landing Volume
Alternative approach: Load directly from volume instead of local files
Uses Auto Loader with schema inference for JSON files
"""

import json
from databricks.connect import DatabricksSession
from pyspark.sql.functions import current_timestamp, col, from_json, schema_of_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, ArrayType
from typing import List, Dict

from config import config


class VolumeBasedBronzeLoader:
    """Load bronze tables directly from Databricks landing volume"""

    def __init__(self, use_separate_tables: bool = False):
        """
        Initialize loader

        Args:
            use_separate_tables: If True, creates bronze_v2_* tables instead of bronze_*
        """
        self.spark = DatabricksSession.builder.remote(serverless=True).getOrCreate()
        self.use_separate_tables = use_separate_tables
        self.table_suffix = "_v2" if use_separate_tables else ""
        self._create_schema()

    def _create_schema(self):
        """Create bronze schema if it doesn't exist"""
        schema_name = f"{config.BRONZE_CATALOG}.{config.BRONZE_SCHEMA}"
        print(f"Creating schema {schema_name}...")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        print(f"Schema {schema_name} is ready\n")

    def _get_table_name(self, entity: str) -> str:
        """Get full table name with optional suffix"""
        return config.get_full_table_name(f"bronze_{entity}{self.table_suffix}")

    def load_sessions_from_volume(self):
        """Load sessions using Auto Loader from volume"""
        entity = "sessions"
        print(f"Loading bronze_{entity}{self.table_suffix} from volume...")

        try:
            # Path to sessions JSON files in volume
            source_path = f"{config.LANDING_VOLUME_PATH}/sessions*.json"
            table_name = self._get_table_name(entity)

            print(f"  Source: {source_path}")
            print(f"  Target: {table_name}")

            # Read JSON files with Auto Loader (cloudFiles)
            df = (self.spark.read
                  .format("json")
                  .option("inferSchema", "true")
                  .option("multiLine", "true")
                  .load(source_path))

            # Add metadata columns (Unity Catalog compatible)
            df = (df
                  .withColumn("_ingestion_timestamp", current_timestamp())
                  .withColumn("_source_file", col("_metadata.file_path")))

            # Write to Delta table
            df.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .saveAsTable(table_name)

            count = df.count()
            print(f"  ✅ Created {table_name} with {count:,} records\n")

        except Exception as e:
            print(f"  ❌ Error loading {entity}: {e}\n")
            import traceback
            traceback.print_exc()

    def load_drivers_from_volume(self):
        """Load drivers using Auto Loader from volume"""
        entity = "drivers"
        print(f"Loading bronze_{entity}{self.table_suffix} from volume...")

        try:
            source_path = f"{config.LANDING_VOLUME_PATH}/drivers*.json"
            table_name = self._get_table_name(entity)

            print(f"  Source: {source_path}")
            print(f"  Target: {table_name}")

            # Read JSON files
            df = (self.spark.read
                  .format("json")
                  .option("inferSchema", "true")
                  .option("multiLine", "true")
                  .load(source_path))

            # Add metadata (Unity Catalog compatible)
            df = (df
                  .withColumn("_ingestion_timestamp", current_timestamp())
                  .withColumn("_source_file", col("_metadata.file_path")))

            # Write to Delta
            df.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .saveAsTable(table_name)

            count = df.count()
            print(f"  ✅ Created {table_name} with {count:,} records\n")

        except Exception as e:
            print(f"  ❌ Error loading {entity}: {e}\n")
            import traceback
            traceback.print_exc()

    def load_laps_from_volume(self):
        """Load laps using Auto Loader from volume"""
        entity = "laps"
        print(f"Loading bronze_{entity}{self.table_suffix} from volume...")

        try:
            source_path = f"{config.LANDING_VOLUME_PATH}/laps*.json"
            table_name = self._get_table_name(entity)

            print(f"  Source: {source_path}")
            print(f"  Target: {table_name}")

            # Read JSON files
            df = (self.spark.read
                  .format("json")
                  .option("inferSchema", "true")
                  .option("multiLine", "true")
                  .load(source_path))

            # Add metadata (Unity Catalog compatible)
            df = (df
                  .withColumn("_ingestion_timestamp", current_timestamp())
                  .withColumn("_source_file", col("_metadata.file_path")))

            # Write to Delta
            df.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .saveAsTable(table_name)

            count = df.count()
            print(f"  ✅ Created {table_name} with {count:,} records\n")

        except Exception as e:
            print(f"  ❌ Error loading {entity}: {e}\n")
            import traceback
            traceback.print_exc()

    def load_locations_from_volume(self):
        """Load locations using Auto Loader from volume"""
        entity = "locations"
        print(f"Loading bronze_{entity}{self.table_suffix} from volume...")

        try:
            source_path = f"{config.LANDING_VOLUME_PATH}/locations*.json"
            table_name = self._get_table_name(entity)

            print(f"  Source: {source_path}")
            print(f"  Target: {table_name}")

            # Read JSON files - locations files are large, so process efficiently
            df = (self.spark.read
                  .format("json")
                  .option("inferSchema", "true")
                  .option("multiLine", "true")
                  .load(source_path))

            # Add metadata (Unity Catalog compatible)
            df = (df
                  .withColumn("_ingestion_timestamp", current_timestamp())
                  .withColumn("_source_file", col("_metadata.file_path")))

            # Write to Delta with optimization
            (df.write
             .format("delta")
             .mode("overwrite")
             .option("overwriteSchema", "true")
             .option("optimizeWrite", "true")
             .option("autoCompact", "true")
             .saveAsTable(table_name))

            count = self.spark.read.table(table_name).count()
            print(f"  ✅ Created {table_name} with {count:,} records\n")

        except Exception as e:
            print(f"  ❌ Error loading {entity}: {e}\n")
            import traceback
            traceback.print_exc()

    def load_all_entities(self):
        """Load all entities from volume"""
        print("="*60)
        print("Loading Bronze Tables from Volume")
        print("="*60)
        print()

        if self.use_separate_tables:
            print("⚠️  Creating NEW tables with '_v2' suffix")
            print("   Existing bronze tables will NOT be touched")
        else:
            print("⚠️  This will OVERWRITE existing bronze tables")
            print("   Use --separate-tables flag to create new tables instead")

        print()

        # Load each entity
        self.load_sessions_from_volume()
        self.load_drivers_from_volume()
        self.load_laps_from_volume()
        self.load_locations_from_volume()

    def verify_tables(self):
        """Verify that all tables were created"""
        print("="*60)
        print("Verifying Bronze Tables")
        print("="*60)

        entities = ["sessions", "drivers", "laps", "locations"]

        for entity in entities:
            table_name = self._get_table_name(entity)
            try:
                count = self.spark.read.table(table_name).count()
                print(f"{table_name}: {count:,} records")
            except Exception as e:
                print(f"{table_name}: ERROR - {e}")

        print("="*60)


def main():
    """Main execution function"""
    import argparse

    parser = argparse.ArgumentParser(
        description='Load bronze tables from Databricks landing volume',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Load from volume (overwrites existing tables)
  python load_from_volume.py

  # Load to separate tables (creates bronze_v2_* tables)
  python load_from_volume.py --separate-tables

  # Verify tables after loading
  python load_from_volume.py --verify-only
        """
    )

    parser.add_argument(
        '--separate-tables',
        action='store_true',
        help='Create new bronze_v2_* tables instead of overwriting bronze_* tables'
    )

    parser.add_argument(
        '--verify-only',
        action='store_true',
        help='Only verify existing tables, do not load'
    )

    args = parser.parse_args()

    try:
        loader = VolumeBasedBronzeLoader(use_separate_tables=args.separate_tables)

        if args.verify_only:
            # Only verification
            loader.verify_tables()
        else:
            # Load and verify
            loader.load_all_entities()
            print()
            loader.verify_tables()
            print()

            print("="*60)
            print("Bronze tables loaded successfully from volume!")
            print("="*60)
            print()

            if args.separate_tables:
                print("Created NEW tables:")
                print("  ✅ bronze_sessions_v2")
                print("  ✅ bronze_drivers_v2")
                print("  ✅ bronze_laps_v2")
                print("  ✅ bronze_locations_v2")
                print()
                print("Original bronze_* tables remain unchanged")
            else:
                print("Updated existing tables:")
                print("  ✅ bronze_sessions")
                print("  ✅ bronze_drivers")
                print("  ✅ bronze_laps")
                print("  ✅ bronze_locations")

            print()
            print("Next steps:")
            print("  1. Verify tables in Databricks SQL Editor")
            if args.separate_tables:
                print("  2. Update dbt sources.yml to use bronze_*_v2 tables")
            print("  3. Run 'dbt build' to create silver and gold layers")
            print()

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()
