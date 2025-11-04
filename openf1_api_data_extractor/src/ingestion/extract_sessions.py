#!/usr/bin/env python3
"""
Enhanced Session Extractor with Oldest/Latest Support
Wrapper around databricks_extractor.py to support extracting oldest sessions.
"""

import sys
import os
import argparse
from databricks_extractor import DatabricksExtractor

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from config import config


def extract_sessions(mode='latest', num_sessions=1, year=None, order='DESC'):
    """
    Extract F1 sessions with flexible ordering

    Args:
        mode: 'latest', 'year', or 'oldest'
        num_sessions: Number of sessions to extract
        year: Year to extract (for year mode)
        order: 'DESC' for newest first, 'ASC' for oldest first
    """
    # Override config
    if mode == 'year':
        config.extraction.fetch_mode = 'year'
        config.extraction.fetch_year = year or 2024
    else:
        config.extraction.fetch_mode = 'latest'
        config.extraction.num_latest_sessions = num_sessions

    # Create extractor
    extractor = DatabricksExtractor()

    # Fetch sessions
    if config.extraction.fetch_mode == 'year':
        print(f"\nFetching sessions for year {config.extraction.fetch_year}...")
        sessions = extractor.api_client.fetch_sessions(year=config.extraction.fetch_year)
    else:
        print(f"\nFetching all available sessions...")
        sessions = extractor.api_client.fetch_sessions()

    if not sessions:
        print("Failed to fetch sessions")
        return

    # Sort sessions
    reverse_sort = (order == 'DESC')
    sessions.sort(key=lambda x: x.get('date_start', ''), reverse=reverse_sort)

    # Select sessions
    if config.extraction.fetch_mode == 'year':
        sessions_to_process = sessions
        print(f"Found and processing {len(sessions_to_process)} sessions for year {config.extraction.fetch_year}")
    else:
        sessions_to_process = sessions[:num_sessions]
        if order == 'ASC':
            print(f"Found {len(sessions)} total sessions")
            print(f"Processing oldest {len(sessions_to_process)} session(s)")
        else:
            print(f"Found {len(sessions)} total sessions")
            print(f"Processing latest {len(sessions_to_process)} session(s)")

    print()

    # Process each session
    for idx, session in enumerate(sessions_to_process, 1):
        extractor.process_session(session, idx, len(sessions_to_process))

    print(f"\n{'=' * 70}")
    print(f"All {len(sessions_to_process)} sessions processed successfully!")
    print(f"Local files: {config.paths.local_temp_dir}")
    print(f"Databricks: {config.paths.landing_volume_path}")
    print(f"{'=' * 70}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Extract F1 sessions with flexible ordering',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract latest 1 session (default)
  python extract_sessions.py

  # Extract latest 5 sessions
  python extract_sessions.py --num-sessions 5

  # Extract oldest 1 session
  python extract_sessions.py --order oldest

  # Extract oldest 5 sessions
  python extract_sessions.py --order oldest --num-sessions 5

  # Extract all 2024 sessions
  python extract_sessions.py --mode year --year 2024
        """
    )

    parser.add_argument(
        '--mode',
        type=str,
        choices=['latest', 'year'],
        default='latest',
        help='Extraction mode: "latest" (default) or "year"'
    )

    parser.add_argument(
        '--num-sessions',
        type=int,
        default=1,
        help='Number of sessions to extract (default: 1)'
    )

    parser.add_argument(
        '--year',
        type=int,
        default=2024,
        help='Year to extract sessions from (for year mode). Default: 2024'
    )

    parser.add_argument(
        '--order',
        type=str,
        choices=['latest', 'oldest'],
        default='latest',
        help='Session order: "latest" (newest first, default) or "oldest" (oldest first)'
    )

    args = parser.parse_args()

    # Validate arguments
    if args.mode == 'year' and args.year is None:
        parser.error("--year is required when using --mode year")

    # Convert order to SQL-style
    sort_order = 'DESC' if args.order == 'latest' else 'ASC'

    # Display configuration
    print("=" * 70)
    print("OpenF1 Data Extractor - Enhanced Configuration")
    print("=" * 70)
    print(f"Mode: {args.mode}")
    if args.mode == 'latest':
        print(f"Sessions: {args.order.capitalize()} {args.num_sessions}")
    elif args.mode == 'year':
        print(f"Year: {args.year}")
    print("=" * 70)
    print()

    extract_sessions(
        mode=args.mode,
        num_sessions=args.num_sessions,
        year=args.year,
        order=sort_order
    )
