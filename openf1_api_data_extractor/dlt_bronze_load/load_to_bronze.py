"""
Load Data to Bronze Layer Tables
Batch loading script for loading JSON files from local directory to bronze Delta tables.
Use this script for initial data loads or batch processing.
"""

import json
import os
import pandas as pd
from databricks.connect import DatabricksSession
from pyspark.sql.functions import current_timestamp
from typing import List

from config import config
from schemas import schemas


class BronzeLoader:
    """Handles batch loading of data to bronze layer tables"""

    def __init__(self):
        """Initialize Spark session"""
        self.spark = DatabricksSession.builder.remote(serverless=True).getOrCreate()
        self._create_schema()

    def _create_schema(self):
        """Create bronze schema if it doesn't exist"""
        schema_name = f"{config.BRONZE_CATALOG}.{config.BRONZE_SCHEMA}"
        print(f"Creating schema {schema_name}...")
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        print(f"Schema {schema_name} is ready\n")

    def _load_json_files(self, entity: str) -> List[dict]:
        """Load all JSON files for an entity and combine them"""
        files = config.get_local_files(entity)
        all_data = []

        if not files:
            return []

        print(f"  Found {len(files)} {entity} files")

        for file_path in files:
            with open(file_path, 'r') as f:
                data = json.load(f)
                if isinstance(data, list):
                    all_data.extend(data)
                else:
                    all_data.append(data)

        return all_data

    def _load_entity_with_pandas(self, entity: str) -> None:
        """Load entity data using pandas (handles schema inconsistencies well)"""
        print(f"Loading bronze_{entity} table...")

        try:
            # Load JSON data
            data = self._load_json_files(entity)

            if not data:
                print(f"  No {entity} data found\n")
                return

            print(f"  Loaded {len(data)} {entity} records")

            # Convert to pandas DataFrame (handles schema variations)
            pdf = pd.DataFrame(data)

            # Convert pandas to Spark DataFrame
            df = self.spark.createDataFrame(pdf)

            # Add metadata columns
            df = df.withColumn("_ingestion_timestamp", current_timestamp())

            # Get table name
            table_name = config.get_full_table_name(f"bronze_{entity}")

            # Write to Delta table
            print(f"  Writing to {table_name}...")
            df.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .saveAsTable(table_name)

            count = df.count()
            print(f"  Created {table_name} with {count} records\n")

        except Exception as e:
            print(f"  Error loading {entity}: {e}\n")
            import traceback
            traceback.print_exc()

    def _load_entity_with_spark(self, entity: str) -> None:
        """Load entity data using Spark (faster for large datasets)"""
        print(f"Loading bronze_{entity} table...")

        try:
            files = config.get_local_files(entity)

            if not files:
                print(f"  No {entity} files found\n")
                return

            print(f"  Found {len(files)} {entity} files")

            # Read JSON files directly with Spark
            df = self.spark.read.option("multiLine", "true").json(files)

            # Add metadata columns
            df = df.withColumn("_ingestion_timestamp", current_timestamp())

            # Get table name
            table_name = config.get_full_table_name(f"bronze_{entity}")

            # Write to Delta table
            print(f"  Writing to {table_name}...")
            df.write.format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .saveAsTable(table_name)

            count = df.count()
            print(f"  Created {table_name} with {count} records\n")

        except Exception as e:
            print(f"  Error loading {entity}: {e}\n")
            import traceback
            traceback.print_exc()

    def load_locations_batched(self) -> None:
        """Load locations data in batches (for very large datasets)"""
        import time
        entity = "locations"
        print(f"Loading bronze_{entity} table (batched)...")

        try:
            # Load all location data
            data = self._load_json_files(entity)

            if not data:
                print(f"  No {entity} data found\n")
                return

            print(f"  Loaded {len(data)} {entity} records")

            table_name = config.get_full_table_name(f"bronze_{entity}")

            # Drop existing table to start fresh
            print(f"  Dropping existing table if exists...")
            self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            time.sleep(2)  # Wait for table to be fully dropped

            # Process in batches
            batch_size = config.BATCH_SIZE
            total_batches = (len(data) + batch_size - 1) // batch_size

            for i in range(0, len(data), batch_size):
                batch = data[i:i + batch_size]
                batch_num = i // batch_size + 1

                # Convert batch to DataFrame
                pdf = pd.DataFrame(batch)
                df = self.spark.createDataFrame(pdf)

                # Add metadata columns
                df = df.withColumn("_ingestion_timestamp", current_timestamp())

                # Write to Delta table with retry logic for concurrent updates
                mode = "overwrite" if i == 0 else "append"
                max_retries = 3
                retry_count = 0

                while retry_count < max_retries:
                    try:
                        df.write.format("delta") \
                            .mode(mode) \
                            .option("overwriteSchema", "true" if i == 0 else "false") \
                            .saveAsTable(table_name)
                        break  # Success, exit retry loop
                    except Exception as write_error:
                        if "MetadataChangedException" in str(write_error) or "DELTA_METADATA_CHANGED" in str(write_error):
                            retry_count += 1
                            if retry_count < max_retries:
                                wait_time = 2 ** retry_count  # Exponential backoff: 2, 4, 8 seconds
                                print(f"  Concurrent update detected, retrying in {wait_time}s... (attempt {retry_count}/{max_retries})")
                                time.sleep(wait_time)
                            else:
                                print(f"  Failed after {max_retries} retries")
                                raise
                        else:
                            # Different error, don't retry
                            raise

                print(f"  Loaded batch {batch_num}/{total_batches}: {len(batch)} records")

            # Get final count
            total_count = self.spark.read.table(table_name).count()
            print(f"  ✅ Created {table_name} with {total_count:,} records (100% complete)\n")

        except Exception as e:
            print(f"  ❌ Error loading {entity}: {e}\n")
            import traceback
            traceback.print_exc()

    def load_all_entities_pandas(self):
        """Load all entities using pandas approach (good for schema variations)"""
        entities = ["sessions", "drivers", "laps"]

        for entity in entities:
            self._load_entity_with_pandas(entity)

        # Load locations with batching
        self.load_locations_batched()

    def load_all_entities_spark(self):
        """Load all entities using Spark approach (faster, requires consistent schemas)"""
        entities = ["sessions", "drivers", "laps", "locations"]

        for entity in entities:
            self._load_entity_with_spark(entity)

    def verify_tables(self):
        """Verify that all tables were created and show row counts"""
        print("="*60)
        print("Verifying Bronze Tables")
        print("="*60)

        entities = ["sessions", "drivers", "laps", "locations"]

        for entity in entities:
            table_name = config.get_full_table_name(f"bronze_{entity}")
            try:
                count = self.spark.read.table(table_name).count()
                print(f"{table_name}: {count:,} records")
            except Exception as e:
                print(f"{table_name}: ERROR - {e}")

        print("="*60)
