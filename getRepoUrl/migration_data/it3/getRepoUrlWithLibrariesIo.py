#!/usr/bin/env python3
"""
Script to fetch GitHub repository URLs for Maven artifacts using libraries.io API.
Updates the githubRepo.csv file by replacing 'unknown' values with actual repository URLs.
"""

import collections
import csv
import time
import requests
from pathlib import Path
from typing import Optional

# Configuration
API_KEY = "af23eda43defa928662d654a175419bc"
API_BASE_URL = "https://libraries.io/api/maven"
CSV_FILE = Path(__file__).parent / "githubRepo.csv"
RATE_LIMIT_CALLS = 60   # max requests per 60-second window (libraries.io free tier)
RATE_LIMIT_PERIOD = 60.0

_rate_timestamps: collections.deque = collections.deque()


def rate_limit_wait() -> None:
    """Block until a slot is available in the sliding 60-req/60s window."""
    while True:
        now = time.monotonic()
        while _rate_timestamps and now - _rate_timestamps[0] >= RATE_LIMIT_PERIOD:
            _rate_timestamps.popleft()
        if len(_rate_timestamps) < RATE_LIMIT_CALLS:
            _rate_timestamps.append(now)
            return
        sleep_time = RATE_LIMIT_PERIOD - (now - _rate_timestamps[0]) + 0.05
        time.sleep(max(sleep_time, 0.05))


def get_repository_url(artifact: str) -> Optional[str]:
    """
    Fetch the GitHub repository URL for a Maven artifact from libraries.io API.

    Args:
        artifact: Maven artifact in format "groupId:artifactId"

    Returns:
        Repository URL if found, None otherwise
    """
    rate_limit_wait()
    url = f"{API_BASE_URL}/{artifact}"
    params = {"api_key": API_KEY}

    try:
        response = requests.get(url, params=params, timeout=10)

        if response.status_code == 200:
            data = response.json()
            repo_url = data.get("repository_url")
            repo_security_url = data.get("security_policy_url")

            if repo_url:
                return repo_url
            elif repo_security_url:
                return repo_security_url
            else:
                return None
        elif response.status_code == 404:
            return None
        elif response.status_code == 429:
            print(" Rate limited — waiting 60s...")
            time.sleep(60)
            return get_repository_url(artifact)
        else:
            print(f" API error: {response.status_code}")
            return None

    except requests.RequestException as e:
        print(f" Request failed: {e}")
        return None


def update_csv_file():
    """
    Read the CSV file, update 'unknown' entries with actual repository URLs,
    and save the results back to the file.
    """
    # Read the CSV file
    print(f"Reading CSV file: {CSV_FILE}")

    with open(CSV_FILE, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=';')
        rows = list(reader)

    # Count unknown entries
    unknown_count = sum(1 for row in rows if row['repository'] == 'unknown')
    print(f"Found {unknown_count} artifacts with 'unknown' repository URLs")

    if unknown_count == 0:
        print("No updates needed!")
        return

    # Process each row
    updated_count = 0
    processed_count = 0

    for row in rows:
        if row['repository'] == 'unknown':
            processed_count += 1
            artifact = row['artifact']

            print(f"\n[{processed_count}/{unknown_count}] Processing: {artifact}")

            # Fetch repository URL from API
            repo_url = get_repository_url(artifact)

            if repo_url:
                row['repository'] = repo_url
                updated_count += 1

                # Save progress after each successful update
                save_csv(rows)

    # Final save
    save_csv(rows)

    print(f"\n{'='*60}")
    print(f"Processing complete!")
    print(f"Total artifacts processed: {processed_count}")
    print(f"Successfully updated: {updated_count}")
    print(f"Failed/Not found: {processed_count - updated_count}")
    print(f"{'='*60}")


def save_csv(rows):
    """
    Save the rows back to the CSV file.

    Args:
        rows: List of dictionaries representing CSV rows
    """
    with open(CSV_FILE, 'w', encoding='utf-8', newline='') as f:
        fieldnames = ['artifact', 'repository']
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        writer.writerows(rows)


def main():
    """Main entry point."""
    print("="*60)
    print("GitHub Repository URL Updater")
    print("Using libraries.io API")
    print("="*60)
    print()

    if not CSV_FILE.exists():
        print(f"Error: CSV file not found at {CSV_FILE}")
        return

    try:
        update_csv_file()
    except KeyboardInterrupt:
        print("\n\nProcess interrupted by user.")
        print("Progress has been saved to the CSV file.")
    except Exception as e:
        print(f"\n\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
