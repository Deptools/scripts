#!/usr/bin/env python3
"""
Script to fetch license information for Maven artifacts using libraries.io API.
Reads artifacts from artifactsNoLicenses.csv, queries the API, and writes results
to licenses-export.json. Also updates licenses.json with any new license identifiers.
Resumable: already-processed artifacts are skipped on restart.
"""

import collections
import csv
import json
import time
import requests
from pathlib import Path
from typing import Optional

# Configuration
API_KEY = "af23eda43defa928662d654a175419bc"
API_BASE_URL = "https://libraries.io/api/maven"
CSV_FILE = Path(__file__).parent / "artifactsNoLicenses.csv"
OUTPUT_FILE = Path(__file__).parent / "licenses-export.json"
LICENSES_FILE = Path(__file__).parent / "licenses.json"
RATE_LIMIT_CALLS = 60   # max requests per 60-second window (libraries.io free tier)
RATE_LIMIT_PERIOD = 60.0
SAVE_INTERVAL = 500  # save every N processed items

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


def get_licenses(artifact: str) -> Optional[list[str]]:
    """
    Fetch license information for a Maven artifact from libraries.io API.

    Extraction priority:
    1. normalized_licenses (list of SPDX IDs)
    2. licenses (raw string, used as single entry)
    3. None if both are empty

    Args:
        artifact: Maven artifact in format "groupId:artifactId"

    Returns:
        List of license name strings, or None if none found / API error
    """
    rate_limit_wait()
    url = f"{API_BASE_URL}/{artifact}"
    params = {"api_key": API_KEY}

    try:
        response = requests.get(url, params=params, timeout=10)

        if response.status_code == 200:
            data = response.json()

            normalized = data.get("normalized_licenses") or []
            if normalized:
                return [lic for lic in normalized if lic]

            raw = data.get("licenses") or ""
            if raw.strip():
                return [raw.strip()]

            return None

        elif response.status_code == 404:
            return None
        elif response.status_code == 429:
            print(" Rate limited — waiting 60s...")
            time.sleep(60)
            return get_licenses(artifact)
        else:
            print(f" API error: {response.status_code}")
            return None

    except requests.RequestException as e:
        print(f" Request failed: {e}")
        return None


def load_json_file(path: Path) -> dict:
    """Load a JSON file, returning an empty dict if the file doesn't exist."""
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_json_file(path: Path, data: dict) -> None:
    """Save data to a JSON file with 2-space indentation."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def update_licenses_registry(registry: dict, license_names: list[str]) -> bool:
    """
    Add any missing license identifiers to the registry with empty fields.

    Args:
        registry: The current licenses.json content (mutated in-place)
        license_names: License names to ensure are present

    Returns:
        True if any new keys were added, False otherwise
    """
    changed = False
    for name in license_names:
        if name not in registry:
            registry[name] = {"url": "", "name": "", "type": ""}
            changed = True
    return changed


def save_csv(rows: list[dict]) -> None:
    """Write all rows back to the CSV file, preserving the done column."""
    with open(CSV_FILE, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["artifact", "done"])
        writer.writeheader()
        writer.writerows(rows)


def process_artifacts() -> None:
    """
    Main processing loop:
    - Load all CSV rows into memory
    - Skip rows where done == "true"
    - Query API for each remaining artifact
    - Mark row as done and save periodically
    """
    # Load all CSV rows into memory
    with open(CSV_FILE, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    total_rows = len(rows)
    skipped_count = sum(1 for r in rows if r.get("done") == "true")
    print(f"Total artifacts in CSV  : {total_rows}")
    print(f"Already done (skipped)  : {skipped_count}")
    print()

    output: dict = load_json_file(OUTPUT_FILE)
    licenses_registry: dict = load_json_file(LICENSES_FILE)

    processed_count = 0
    found_count = 0
    not_found_count = 0
    registry_changed = False

    for row in rows:
        if row.get("done") == "true":
            continue

        artifact = row["artifact"].strip()
        processed_count += 1
        remaining = total_rows - skipped_count - processed_count + 1
        print(f"[{processed_count} new | {skipped_count} skipped | ~{remaining} left] {artifact}", end="")

        license_names = get_licenses(artifact)

        if license_names:
            entries = []
            for name in license_names:
                entries.append({"name": name})

            output[artifact] = entries
            found_count += 1
            print(f" → {license_names}")

            if update_licenses_registry(licenses_registry, license_names):
                registry_changed = True
        else:
            not_found_count += 1
            print(" → no license found")

        # Mark this artifact as done in the CSV row
        row["done"] = "true"

        # Periodic save
        if processed_count % SAVE_INTERVAL == 0:
            save_csv(rows)
            save_json_file(OUTPUT_FILE, output)
            if registry_changed:
                save_json_file(LICENSES_FILE, licenses_registry)
                registry_changed = False
            print(f"\n  [Saved progress at {processed_count} new items]\n")

    # Final save
    save_csv(rows)
    save_json_file(OUTPUT_FILE, output)
    if registry_changed:
        save_json_file(LICENSES_FILE, licenses_registry)

    print()
    print("=" * 60)
    print("Processing complete!")
    print(f"  New artifacts processed : {processed_count}")
    print(f"  Skipped (already done)  : {skipped_count}")
    print(f"  Licenses found          : {found_count}")
    print(f"  No license found        : {not_found_count}")
    print(f"  Total in output         : {len(output)}")
    print("=" * 60)


def main() -> None:
    """Entry point."""
    print("=" * 60)
    print("Maven License Fetcher — libraries.io API")
    print("=" * 60)
    print()

    if not CSV_FILE.exists():
        print(f"Error: input file not found: {CSV_FILE}")
        return

    try:
        process_artifacts()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user. Progress has been saved.")
    except Exception as e:
        print(f"\n\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
