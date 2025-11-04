"""
Session Finder Utility
Helps find valid session keys from the OpenF1 API.
"""

from typing import List, Dict, Optional, Any
from .api_client import api_client


class SessionFinder:
    """Find and filter F1 sessions"""

    def __init__(self):
        self.client = api_client

    def check_api_health(self) -> tuple[bool, Any]:
        """Check if API is responding"""
        return self.client.check_health()

    def get_recent_sessions(
        self,
        year: int = 2024,
        limit: int = 20
    ) -> Optional[List[Dict]]:
        """
        Get recent sessions from a specific year

        Args:
            year: Year to fetch
            limit: Maximum number of sessions

        Returns:
            List of session dictionaries sorted by date (most recent first)
        """
        sessions = self.client.fetch_sessions(year=year)

        if not sessions:
            return None

        # Sort by date descending
        sorted_sessions = sorted(
            sessions,
            key=lambda x: x.get('date_start', ''),
            reverse=True
        )

        return sorted_sessions[:limit]

    def get_race_sessions(
        self,
        year: int = 2024,
        limit: int = 10
    ) -> Optional[List[Dict]]:
        """Get only race sessions"""
        sessions = self.get_recent_sessions(year=year, limit=100)

        if not sessions:
            return None

        race_sessions = [
            s for s in sessions
            if s.get('session_type') == 'Race'
        ]

        return race_sessions[:limit]

    def print_session_info(self, session: Dict):
        """Print formatted session information"""
        print(f"Session Key: {session.get('session_key')}")
        print(f"   Name: {session.get('session_name')}")
        print(f"   Date: {session.get('date_start')}")
        print(f"   Location: {session.get('location')}, {session.get('country_name')}")
        print(f"   Type: {session.get('session_type')}")
        print()

    def print_sessions_summary(self, sessions: List[Dict]):
        """Print summary of multiple sessions"""
        print(f"\nFound {len(sessions)} sessions:\n")

        for i, session in enumerate(sessions, 1):
            print(f"{i}. ", end="")
            self.print_session_info(session)

        # Suggest a race session
        race_sessions = [s for s in sessions if s.get('session_type') == 'Race']
        if race_sessions:
            print("=" * 80)
            print("💡 Recommended session key for testing (most recent Race):")
            print("=" * 80)
            latest_race = race_sessions[0]
            print(f"SESSION_KEY = \"{latest_race.get('session_key')}\"")
            print(f"# {latest_race.get('session_name')} - {latest_race.get('location')}")
            print()


# Singleton instance
session_finder = SessionFinder()
