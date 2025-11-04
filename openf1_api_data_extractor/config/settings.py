"""
Configuration Settings for F1 Data Extraction
Centralized configuration for API endpoints, paths, and extraction settings.
"""

import os
from typing import List, Dict
from dataclasses import dataclass


@dataclass
class APIConfig:
    """API-related configuration"""
    base_url: str = "https://api.openf1.org/v1"
    timeout: int = 30
    max_retries: int = 3
    retry_delay: int = 2  # seconds
    max_workers: int = 3  # parallel downloads (API limit: 3 requests per second)


@dataclass
class PathConfig:
    """Path configuration for data storage"""
    # Databricks Volume path (for production)
    landing_volume_path: str = "/Volumes/dev_f1_data_analytics/raw_data/landing"

    # Local temp directory (for development/testing)
    local_temp_dir: str = os.path.join(
        os.path.expanduser('~'),
        'Documents',
        'openf1_data_analytics',
        'local_test_data'
    )

    # Desktop output (for desktop extraction)
    desktop_output_dir: str = os.path.join(
        os.path.expanduser('~'),
        'Desktop',
        'openf1_data2'
    )


@dataclass
class ExtractionConfig:
    """Extraction mode and settings"""
    # Fetch modes: 'latest' (default), 'year', 'manual'
    fetch_mode: str = 'latest'

    # Mode 1: Latest N sessions (DEFAULT)
    num_latest_sessions: int = 10

    # Mode 2: Specific year
    fetch_year: int = 2024

    # Mode 3: Manual dates
    manual_batch_days: List[str] = None

    # Data options
    fetch_locations: bool = True
    fetch_laps: bool = True
    fetch_drivers: bool = True
    fetch_sessions: bool = True

    # Performance
    use_pagination_for_locations: bool = True
    location_chunk_minutes: int = 15
    max_location_chunks: int = 12

    def __post_init__(self):
        if self.manual_batch_days is None:
            self.manual_batch_days = [
                '2025-10-26',
                '2025-10-25',
                '2025-10-19',
                '2025-10-05',
                '2025-09-21',
                '2025-09-07',
                '2025-08-31',
                '2025-08-03'
            ]


@dataclass
class DatabricksConfig:
    """Databricks-specific configuration"""
    serverless: bool = True
    control_table: str = "dev_f1_data_analytics.raw_data.control"
    control_endpoint: str = "sessions"


class Config:
    """Main configuration class"""

    def __init__(self):
        self.api = APIConfig()
        self.paths = PathConfig()
        self.extraction = ExtractionConfig()
        self.databricks = DatabricksConfig()

    def get_output_path(self, mode: str = 'databricks') -> str:
        """Get output path based on mode"""
        if mode == 'databricks':
            return self.paths.landing_volume_path
        elif mode == 'local':
            return self.paths.local_temp_dir
        elif mode == 'desktop':
            return self.paths.desktop_output_dir
        else:
            raise ValueError(f"Unknown mode: {mode}")

    def ensure_output_dir(self, mode: str = 'local'):
        """Ensure output directory exists"""
        path = self.get_output_path(mode)
        if not path.startswith('/Volumes/'):  # Don't create for Databricks volumes
            os.makedirs(path, exist_ok=True)

    def to_dict(self) -> Dict:
        """Convert config to dictionary"""
        return {
            'api': {
                'base_url': self.api.base_url,
                'timeout': self.api.timeout,
                'max_retries': self.api.max_retries,
                'max_workers': self.api.max_workers
            },
            'extraction': {
                'fetch_mode': self.extraction.fetch_mode,
                'num_latest_sessions': self.extraction.num_latest_sessions,
                'fetch_year': self.extraction.fetch_year
            },
            'paths': {
                'landing_volume': self.paths.landing_volume_path,
                'local_temp': self.paths.local_temp_dir
            }
        }


# Singleton instance
config = Config()
