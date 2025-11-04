"""
Databricks Extractor
Extracts F1 data from OpenF1 API and uploads to Databricks volumes.
Based on f1_test/f1_extract.py
"""

import time
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from databricks.connect import DatabricksSession
from pyspark.sql.functions import current_timestamp

import sys
import os
# Add parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from config import config
from src.utils import api_client, FileHandler


class DatabricksExtractor:
    """Extract F1 data and load to Databricks volumes"""

    def __init__(self):
        """Initialize Databricks extractor"""
        self.config = config
        self.api_client = api_client
        self.file_handler = FileHandler(output_mode='local')

        # Initialize Databricks Session
        self.spark = DatabricksSession.builder.remote(
            serverless=config.databricks.serverless
        ).getOrCreate()

        # Get dbutils if available
        try:
            from pyspark.dbutils import DBUtils
            self.dbutils = DBUtils(self.spark)
        except ImportError:
            self.dbutils = None

        print("=" * 70)
        print("OpenF1 Data Extractor - Databricks Edition")
        print("=" * 70)

    def fetch_location_with_pagination(
        self,
        session_key: int,
        driver_number: int,
        date_start: str
    ) -> List[Dict]:
        """
        Fetch location data with time-based pagination to avoid API limits.
        Splits the session into smaller time chunks to respect the 4MB/10s limit.

        Note: Telemetry data often starts 15-30 minutes BEFORE the official session start time.
        We adjust the start time accordingly to capture all available data.
        """
        location_data = []

        try:
            # Parse session start time
            start_time = datetime.fromisoformat(date_start.replace('Z', '+00:00'))

            # Start 30 minutes BEFORE official session time to capture early telemetry
            # (cars often go on track for installation laps, outlaps, etc.)
            adjusted_start_time = start_time - timedelta(minutes=30)

            # Split session into chunks
            chunk_duration = config.extraction.location_chunk_minutes
            max_chunks = config.extraction.max_location_chunks

            consecutive_empty_chunks = 0
            max_consecutive_empty = 3  # Stop if we get 3 empty chunks in a row

            for chunk in range(max_chunks):
                chunk_start = adjusted_start_time + timedelta(minutes=chunk * chunk_duration)
                chunk_end = chunk_start + timedelta(minutes=chunk_duration)

                chunk_start_str = chunk_start.strftime('%Y-%m-%dT%H:%M:%S')
                chunk_end_str = chunk_end.strftime('%Y-%m-%dT%H:%M:%S')

                # Rate limiting handled by thread-safe rate limiter in api_client

                chunk_data = self.api_client.fetch_locations(
                    session_key=session_key,
                    driver_number=driver_number,
                    date_start=chunk_start_str,
                    date_end=chunk_end_str
                )

                if chunk_data and isinstance(chunk_data, list):
                    if len(chunk_data) > 0:
                        location_data.extend(chunk_data)
                        consecutive_empty_chunks = 0  # Reset counter
                    else:
                        consecutive_empty_chunks += 1
                        if consecutive_empty_chunks >= max_consecutive_empty:
                            # Multiple empty chunks in a row - likely past session end
                            break
                elif chunk_data is None:
                    # API error - try next chunk
                    consecutive_empty_chunks += 1
                    if consecutive_empty_chunks >= max_consecutive_empty:
                        break
                    continue
                else:
                    # chunk_data is empty list (422 error or no data)
                    consecutive_empty_chunks += 1
                    if consecutive_empty_chunks >= max_consecutive_empty:
                        break

        except Exception as e:
            print(f"  Warning: Location pagination error for driver {driver_number}: {str(e)}")

        return location_data

    def fetch_driver_data(
        self,
        session_key: int,
        driver_number: int,
        date_start: Optional[str] = None
    ) -> Tuple[int, List[Dict], List[Dict]]:
        """Fetch location and lap data for a single driver"""
        location_data = []
        lap_data = []

        # Rate limiting is now handled by the thread-safe rate limiter in api_client
        # No manual delay needed here

        # Fetch lap data first (smaller dataset)
        if config.extraction.fetch_laps:
            laps = self.api_client.fetch_laps(session_key, driver_number)
            if laps:
                lap_data = laps

        # Fetch location data with pagination if enabled
        if config.extraction.fetch_locations and date_start:
            if config.extraction.use_pagination_for_locations:
                location_data = self.fetch_location_with_pagination(
                    session_key,
                    driver_number,
                    date_start
                )
            else:
                locs = self.api_client.fetch_locations(session_key, driver_number)
                if locs:
                    location_data = locs

        return driver_number, location_data, lap_data

    def process_session(self, session: Dict, idx: int, total: int):
        """Process a single session"""
        session_key = session.get('session_key')
        session_name = session.get('session_name', 'Unknown')
        location = session.get('location', 'Unknown')
        date_start = session.get('date_start', 'Unknown')

        print(f"\n{'=' * 70}")
        print(f"Session {idx}/{total}: {session_name}")
        print(f"Location: {location}")
        print(f"Date: {date_start}")
        print(f"Session Key: {session_key}")
        print(f"{'=' * 70}")

        # Generate filename prefix
        batch_day = date_start.split('T')[0] if 'T' in date_start else 'unknown'
        location_clean = location.replace(' ', '_').replace('/', '_')
        file_prefix = f"{batch_day}_{location_clean}_{session_name.replace(' ', '_')}"

        # Save session metadata
        if config.extraction.fetch_sessions:
            print("\n[1/4] Saving session metadata...")
            session_data = [session]
            sessions_file = f"sessions_{file_prefix}.json"
            local_sessions = self.file_handler.save_json(session_data, sessions_file)
            self.file_handler.upload_to_databricks(
                local_sessions,
                f"{config.paths.landing_volume_path}/{sessions_file}",
                self.spark,
                self.dbutils
            )

        # Fetch drivers
        if config.extraction.fetch_drivers:
            print("\n[2/4] Fetching drivers...")
            drivers = self.api_client.fetch_drivers(session_key)

            if not drivers:
                print("No drivers found for this session")
                return

            print(f"  Found {len(drivers)} drivers")

            # Save drivers
            drivers_file = f"drivers_{file_prefix}.json"
            local_drivers = self.file_handler.save_json(drivers, drivers_file)
            self.file_handler.upload_to_databricks(
                local_drivers,
                f"{config.paths.landing_volume_path}/{drivers_file}",
                self.spark,
                self.dbutils
            )

            # Get driver numbers
            driver_numbers = [driver['driver_number'] for driver in drivers]
        else:
            driver_numbers = []

        # Fetch location and lap data in parallel
        print(f"\n[3/4] Fetching location and lap data for {len(driver_numbers)} drivers...")
        all_locations = []
        all_laps = []

        with ThreadPoolExecutor(max_workers=config.api.max_workers) as executor:
            futures = {
                executor.submit(
                    self.fetch_driver_data,
                    session_key,
                    driver_number,
                    date_start
                ): driver_number
                for driver_number in driver_numbers
            }

            completed = 0
            total_drivers = len(driver_numbers)

            for future in as_completed(futures):
                driver_number, loc_data, lap_data = future.result()

                # Add metadata to each record
                for loc in loc_data:
                    loc['driver_number'] = driver_number
                    loc['session_key'] = session_key
                    loc['session_name'] = session_name
                    loc['batch_day'] = batch_day

                for lap in lap_data:
                    lap['driver_number'] = driver_number
                    lap['session_key'] = session_key
                    lap['session_name'] = session_name
                    lap['batch_day'] = batch_day

                all_locations.extend(loc_data)
                all_laps.extend(lap_data)

                completed += 1
                if completed % 5 == 0 or completed == total_drivers:
                    print(f"  Progress: [{completed}/{total_drivers}] drivers", end=" ")
                    print(f"(Locations: {len(all_locations)}, Laps: {len(all_laps)})")

        # Save consolidated data
        print(f"\n[4/4] Saving consolidated data...")

        if all_locations:
            locations_file = f"locations_{file_prefix}.json"
            local_locations = self.file_handler.save_json(all_locations, locations_file)
            self.file_handler.upload_to_databricks(
                local_locations,
                f"{config.paths.landing_volume_path}/{locations_file}",
                self.spark,
                self.dbutils
            )
        else:
            print("  ⚠️  No location data available for this session")
            print("      (Location telemetry may not be available for future/old sessions)")
            print("      Tip: Use --mode year --year 2024 for sessions with complete telemetry")

        if all_laps:
            laps_file = f"laps_{file_prefix}.json"
            local_laps = self.file_handler.save_json(all_laps, laps_file)
            self.file_handler.upload_to_databricks(
                local_laps,
                f"{config.paths.landing_volume_path}/{laps_file}",
                self.spark,
                self.dbutils
            )
        else:
            print("  No lap data to save")

        print(f"\nCompleted session {idx}/{total}")

    def run(self):
        """Main execution method"""
        # Fetch sessions based on config mode
        if config.extraction.fetch_mode == 'year':
            print(f"\nFetching sessions for year {config.extraction.fetch_year}...")
            sessions = self.api_client.fetch_sessions(year=config.extraction.fetch_year)
        else:
            print(f"\nFetching latest {config.extraction.num_latest_sessions} sessions...")
            sessions = self.api_client.fetch_sessions()

        if not sessions:
            print("Failed to fetch sessions")
            return

        # Sort by date and get latest N (only if in 'latest' mode)
        sessions.sort(key=lambda x: x.get('date_start', ''), reverse=True)

        if config.extraction.fetch_mode == 'latest':
            sessions_to_process = sessions[:config.extraction.num_latest_sessions]
            print(f"Found {len(sessions)} total sessions")
            print(f"Processing latest {len(sessions_to_process)} sessions\n")
        else:
            sessions_to_process = sessions
            print(f"Found and processing {len(sessions_to_process)} sessions for year {config.extraction.fetch_year}\n")

        # Process each session
        for idx, session in enumerate(sessions_to_process, 1):
            self.process_session(session, idx, len(sessions_to_process))

        print(f"\n{'=' * 70}")
        print(f"All {len(sessions_to_process)} sessions processed successfully!")
        print(f"Local files: {config.paths.local_temp_dir}")
        print(f"Databricks: {config.paths.landing_volume_path}")
        print(f"{'=' * 70}")


# Export main function
def extract_to_databricks(mode: str = None, num_sessions: int = None, year: int = None):
    """
    Main entry point for Databricks extraction

    Args:
        mode: Extraction mode ('latest', 'year', or 'manual')
        num_sessions: Number of latest sessions to fetch (for 'latest' mode)
        year: Year to fetch sessions from (for 'year' mode)
    """
    # Override config with command-line arguments if provided
    if mode:
        config.extraction.fetch_mode = mode
    if num_sessions:
        config.extraction.num_latest_sessions = num_sessions
    if year:
        config.extraction.fetch_year = year

    extractor = DatabricksExtractor()
    extractor.run()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Extract F1 data from OpenF1 API and load to Databricks',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract latest 10 sessions (default)
  python databricks_extractor.py

  # Extract latest 5 sessions
  python databricks_extractor.py --mode latest --num-sessions 5

  # Extract all sessions from 2024
  python databricks_extractor.py --mode year --year 2024

  # Extract all sessions from 2023
  python databricks_extractor.py --mode year --year 2023

  # Use manual dates (configured in settings.py)
  python databricks_extractor.py --mode manual
        """
    )

    parser.add_argument(
        '--mode',
        type=str,
        choices=['latest', 'year', 'manual'],
        default='latest',
        help='Extraction mode: "latest" (default), "year", or "manual"'
    )

    parser.add_argument(
        '--num-sessions',
        type=int,
        default=None,
        help='Number of latest sessions to extract (for latest mode). Default: 10'
    )

    parser.add_argument(
        '--year',
        type=int,
        default=None,
        help='Year to extract sessions from (for year mode). Example: 2024'
    )

    args = parser.parse_args()

    # Validate arguments
    if args.mode == 'year' and args.year is None:
        parser.error("--year is required when using --mode year")

    # Display configuration
    print("=" * 70)
    print("OpenF1 Data Extractor - Configuration")
    print("=" * 70)
    print(f"Mode: {args.mode}")
    if args.mode == 'latest':
        num = args.num_sessions or config.extraction.num_latest_sessions
        print(f"Sessions: Latest {num}")
    elif args.mode == 'year':
        print(f"Year: {args.year}")
    elif args.mode == 'manual':
        print(f"Manual dates: {len(config.extraction.manual_batch_days)} configured")
    print("=" * 70)
    print()

    extract_to_databricks(
        mode=args.mode,
        num_sessions=args.num_sessions,
        year=args.year
    )
