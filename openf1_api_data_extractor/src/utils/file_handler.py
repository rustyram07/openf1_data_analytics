"""
File Handler Utilities
Handles saving data to local files and uploading to Databricks volumes.
"""

import json
import os
import sys
from typing import List, Dict, Any, Optional

# Add parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from config import config


class FileHandler:
    """Handles file operations for extraction pipeline"""

    def __init__(self, output_mode: str = 'local'):
        """
        Initialize file handler

        Args:
            output_mode: 'local', 'databricks', or 'desktop'
        """
        self.output_mode = output_mode
        self.output_path = config.get_output_path(output_mode)
        config.ensure_output_dir(output_mode)

    def save_json(
        self,
        data: List[Dict[str, Any]],
        filename: str,
        subdirectory: Optional[str] = None
    ) -> str:
        """
        Save data to JSON file

        Args:
            data: Data to save (list of dictionaries)
            filename: Output filename
            subdirectory: Optional subdirectory within output path

        Returns:
            Full path to saved file
        """
        if subdirectory:
            full_dir = os.path.join(self.output_path, subdirectory)
            os.makedirs(full_dir, exist_ok=True)
        else:
            full_dir = self.output_path

        filepath = os.path.join(full_dir, filename)

        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)

        print(f"  Saved {len(data):,} records to {filename}")
        return filepath

    def save_csv(
        self,
        data: List[Dict[str, Any]],
        filename: str,
        subdirectory: Optional[str] = None
    ) -> str:
        """
        Save data to CSV file

        Args:
            data: Data to save (list of dictionaries)
            filename: Output filename
            subdirectory: Optional subdirectory within output path

        Returns:
            Full path to saved file
        """
        import pandas as pd

        if subdirectory:
            full_dir = os.path.join(self.output_path, subdirectory)
            os.makedirs(full_dir, exist_ok=True)
        else:
            full_dir = self.output_path

        filepath = os.path.join(full_dir, filename)

        df = pd.DataFrame(data)
        df.to_csv(filepath, index=False)

        print(f"  Saved {len(data):,} records to {filename}")
        return filepath

    def upload_to_databricks(
        self,
        local_path: str,
        volume_path: str,
        spark=None,
        dbutils=None
    ):
        """
        Upload JSON file to Databricks volume

        Args:
            local_path: Local file path
            volume_path: Databricks volume path
            spark: Spark session (optional)
            dbutils: DBUtils instance (optional)
        """
        try:
            if dbutils:
                # Method 1: Use dbutils
                with open(local_path, 'r') as f:
                    content = f.read()
                dbutils.fs.put(volume_path, content, overwrite=True)
                print(f"  Uploaded to {volume_path}")

            elif spark:
                # Method 2: Use Spark
                from pyspark.sql.functions import current_timestamp
                df = spark.read.json(local_path).withColumn(
                    "ingestion_timestamp",
                    current_timestamp()
                )
                df.write.format("json").mode("overwrite").save(volume_path)
                print(f"  Uploaded to {volume_path}")

            else:
                print(f"  Warning: No Databricks connection available, skipping upload")

        except Exception as e:
            print(f"  Warning: Failed to upload to Databricks: {e}")

    def generate_filename(
        self,
        entity: str,
        session: Dict[str, Any],
        extension: str = 'json'
    ) -> str:
        """
        Generate standardized filename

        Args:
            entity: Entity type (e.g., 'sessions', 'drivers', 'laps', 'locations')
            session: Session metadata dictionary
            extension: File extension (default: 'json')

        Returns:
            Filename string
        """
        date_start = session.get('date_start', 'Unknown')
        batch_day = date_start.split('T')[0] if 'T' in date_start else 'unknown'

        location = session.get('location', 'Unknown')
        location_clean = location.replace(' ', '_').replace('/', '_')

        session_name = session.get('session_name', 'Unknown')
        session_name_clean = session_name.replace(' ', '_')

        filename = f"{entity}_{batch_day}_{location_clean}_{session_name_clean}.{extension}"
        return filename

    def list_files(self, pattern: Optional[str] = None) -> List[str]:
        """
        List files in output directory

        Args:
            pattern: Optional glob pattern

        Returns:
            List of file paths
        """
        import glob

        if pattern:
            search_path = os.path.join(self.output_path, pattern)
        else:
            search_path = os.path.join(self.output_path, '*')

        return glob.glob(search_path)


# Export file format helpers
def format_for_json(records: List[Dict[str, Any]]) -> str:
    """Format records as JSON string"""
    return json.dumps(records, indent=2)


def format_for_csv(records: List[Dict[str, Any]]) -> str:
    """Format records as CSV string"""
    import pandas as pd
    df = pd.DataFrame(records)
    return df.to_csv(index=False)
