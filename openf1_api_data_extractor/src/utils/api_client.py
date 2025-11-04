"""
API Client for OpenF1 API
Handles HTTP requests with retry logic and error handling.
"""

import requests
import time
import sys
import os
import threading
from typing import Optional, Dict, Any, List

# Add parent directory to path to allow imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from config import config


class RateLimiter:
    """Thread-safe rate limiter to ensure we don't exceed API limits"""

    def __init__(self, max_requests_per_second: float = 2.5):
        """
        Initialize rate limiter

        Args:
            max_requests_per_second: Maximum requests allowed per second
                                    (default 2.5 to stay safely under API's 3/sec limit)
        """
        self.max_requests_per_second = max_requests_per_second
        self.min_interval = 1.0 / max_requests_per_second
        self.last_request_time = 0
        self.lock = threading.Lock()

    def wait_if_needed(self):
        """Wait if necessary to respect rate limits (thread-safe)"""
        with self.lock:
            current_time = time.time()
            time_since_last_request = current_time - self.last_request_time

            if time_since_last_request < self.min_interval:
                sleep_time = self.min_interval - time_since_last_request
                time.sleep(sleep_time)

            self.last_request_time = time.time()


class APIClient:
    """Client for OpenF1 API with retry logic"""

    # Class-level rate limiter shared across all instances
    # Set to 2.8 req/sec to stay safely under API's 3/sec limit with thread overhead
    _rate_limiter = RateLimiter(max_requests_per_second=2.8)

    def __init__(self):
        self.base_url = config.api.base_url
        self.timeout = config.api.timeout
        self.max_retries = config.api.max_retries
        self.retry_delay = config.api.retry_delay

    def fetch_with_retry(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None
    ) -> Optional[List[Dict]]:
        """
        Fetch data from API with retry logic

        Args:
            endpoint: API endpoint (e.g., 'sessions', 'drivers')
            params: Query parameters

        Returns:
            JSON response as list of dictionaries, or None on failure
        """
        url = f"{self.base_url}/{endpoint}"

        for attempt in range(self.max_retries):
            try:
                # Use thread-safe rate limiter before making request
                self._rate_limiter.wait_if_needed()

                resp = requests.get(url, params=params, timeout=self.timeout)

                if resp.ok:
                    data = resp.json()
                    return data if isinstance(data, list) else [data]

                elif resp.status_code == 429:
                    # Rate limit exceeded - wait longer before retrying
                    wait_time = 1.5 * (attempt + 1)  # Exponential backoff for rate limits
                    print(f"  Rate limit (429), waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                    continue

                elif resp.status_code in [500, 502, 503, 504]:
                    print(f"  Server error {resp.status_code}, retrying in {self.retry_delay ** attempt}s...")
                    time.sleep(self.retry_delay ** attempt)
                    continue

                else:
                    print(f"  Error {resp.status_code}: {resp.text[:200]}")
                    return None

            except requests.exceptions.Timeout:
                print(f"  Timeout, retrying in {self.retry_delay ** attempt}s...")
                time.sleep(self.retry_delay ** attempt)

            except Exception as e:
                print(f"  Error: {e}")
                return None

        print(f"  Failed after {self.max_retries} attempts")
        return None

    def check_health(self) -> tuple[bool, Any]:
        """Check if the API is responding"""
        try:
            resp = requests.get(
                f"{self.base_url}/sessions",
                timeout=10,
                params={"limit": 1}
            )
            return resp.ok, resp.status_code
        except Exception as e:
            return False, str(e)

    def fetch_sessions(
        self,
        year: Optional[int] = None,
        limit: Optional[int] = None
    ) -> Optional[List[Dict]]:
        """Fetch sessions with optional year filter
        Note: limit parameter is not supported by OpenF1 API and will be applied client-side"""
        params = {}
        if year:
            params['year'] = year

        # Don't pass limit to API - it causes empty results
        # We'll apply limit client-side
        sessions = self.fetch_with_retry('sessions', params)

        # Apply limit client-side if specified
        if sessions and limit:
            return sessions[:limit]

        return sessions

    def fetch_drivers(self, session_key: int) -> Optional[List[Dict]]:
        """Fetch drivers for a session"""
        return self.fetch_with_retry('drivers', {'session_key': session_key})

    def fetch_laps(
        self,
        session_key: int,
        driver_number: Optional[int] = None
    ) -> Optional[List[Dict]]:
        """Fetch laps for a session and optionally a specific driver"""
        params = {'session_key': session_key}
        if driver_number:
            params['driver_number'] = driver_number

        return self.fetch_with_retry('laps', params)

    def fetch_locations(
        self,
        session_key: int,
        driver_number: Optional[int] = None,
        date_start: Optional[str] = None,
        date_end: Optional[str] = None
    ) -> Optional[List[Dict]]:
        """
        Fetch locations for a session and optionally a specific driver

        Note: The location API requires date parameters to be passed as SEPARATE
        query parameters (e.g., date>value&date<value), not as a single comma-separated value.
        This method constructs the URL manually to achieve this.
        """
        # Build base parameters (standard query params)
        params = {'session_key': session_key}
        if driver_number:
            params['driver_number'] = driver_number

        # For date filtering, we need to manually construct the URL
        # because the API expects: ?date>start&date<end (two separate 'date' params)
        # but requests.get() with params dict would encode it as ?date=...
        if date_start or date_end:
            return self._fetch_locations_with_date_filter(
                session_key, driver_number, date_start, date_end
            )

        # No date filter - use standard fetch
        return self.fetch_with_retry('location', params)

    def _fetch_locations_with_date_filter(
        self,
        session_key: int,
        driver_number: Optional[int],
        date_start: Optional[str],
        date_end: Optional[str]
    ) -> Optional[List[Dict]]:
        """
        Fetch locations with date filtering using manual URL construction.

        The OpenF1 location API requires date parameters as separate query params:
        ?session_key=123&driver_number=1&date>2024-01-01T10:00:00&date<2024-01-01T11:00:00

        Standard requests params dict doesn't support this format, so we build the URL manually.
        """
        url = f"{self.base_url}/location?session_key={session_key}"

        if driver_number:
            url += f"&driver_number={driver_number}"

        if date_start:
            # Remove timezone suffix for cleaner URLs (API accepts both formats)
            date_start_clean = date_start.replace('Z', '').replace('+00:00', '')
            url += f"&date>{date_start_clean}"

        if date_end:
            date_end_clean = date_end.replace('Z', '').replace('+00:00', '')
            url += f"&date<{date_end_clean}"

        # Use the retry logic but with manual URL (no params dict)
        for attempt in range(self.max_retries):
            try:
                # Use thread-safe rate limiter before making request
                self._rate_limiter.wait_if_needed()

                resp = requests.get(url, timeout=self.timeout)

                if resp.ok:
                    data = resp.json()
                    return data if isinstance(data, list) else [data]

                elif resp.status_code == 429:
                    # Rate limit exceeded - wait longer before retrying
                    wait_time = 1.5 * (attempt + 1)
                    print(f"  Rate limit (429), waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                    continue

                elif resp.status_code == 422:
                    # 422 means "too much data requested" - this is expected for large ranges
                    # Return empty list to signal caller to use smaller chunks
                    return []

                elif resp.status_code in [500, 502, 503, 504]:
                    print(f"  Server error {resp.status_code}, retrying in {self.retry_delay ** attempt}s...")
                    time.sleep(self.retry_delay ** attempt)
                    continue

                else:
                    print(f"  Error {resp.status_code}: {resp.text[:200]}")
                    return None

            except requests.exceptions.Timeout:
                print(f"  Timeout, retrying in {self.retry_delay ** attempt}s...")
                time.sleep(self.retry_delay ** attempt)

            except Exception as e:
                print(f"  Error: {e}")
                return None

        print(f"  Failed after {self.max_retries} attempts")
        return None

    def add_rate_limit_delay(self, delay: float = 0.35):
        """Add delay to respect API rate limits (3 requests per second)"""
        time.sleep(delay)


# Singleton instance
api_client = APIClient()
