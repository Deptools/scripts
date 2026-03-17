#!/usr/bin/env python3
"""
Script to check if new artifacts (from MAVEN_CENTRAL and MAVEN_GOOGLE CSVs)
have a repoUrl assigned in Neo4j (repoUrl = 'unknown').
For each such artifact, queries the libraries.io API to find a repository URL.
Outputs 'newArtifacts_noRepo.csv' containing only artifacts for which a URL
was found, with columns: artifactId;repoUrl.
"""

import collections
import csv
import logging
import os
import time
from typing import Optional
from urllib.parse import urlparse, parse_qs

import requests
from neo4j import GraphDatabase

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# --- Neo4j ---
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password1"

NEO4J_QUERY = "MATCH (n:Artifact {id: $id}) RETURN n.repoUrl AS repoUrl"

# --- libraries.io ---
LIBRARIES_IO_API_KEY = "af23eda43defa928662d654a175419bc"
LIBRARIES_IO_BASE_URL = "https://libraries.io/api/maven"
RATE_LIMIT_CALLS = 60
RATE_LIMIT_PERIOD = 60.0

# --- Files ---
BASE_DIR = os.path.dirname(__file__)
INPUT_CSVS = [
    os.path.join(BASE_DIR, "newArtifacts_MAVEN_CENTRAL.csv"),
    os.path.join(BASE_DIR, "newArtifacts_MAVEN_GOOGLE.csv"),
]
OUTPUT_CSV = os.path.join(BASE_DIR, "newArtifacts_noRepo.csv")
CSV_DELIMITER = ";"

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
    """Query libraries.io for a Maven artifact and return its repository URL."""
    rate_limit_wait()
    url = f"{LIBRARIES_IO_BASE_URL}/{artifact}"
    params = {"api_key": LIBRARIES_IO_API_KEY}

    try:
        response = requests.get(url, params=params, timeout=10)

        if response.status_code == 200:
            data = response.json()
            repo_url = data.get("repository_url") or data.get("security_policy_url")
            return repo_url if repo_url else None
        elif response.status_code == 404:
            return None
        elif response.status_code == 429:
            logger.warning("Rate limited by libraries.io — waiting 60s...")
            time.sleep(60)
            return get_repository_url(artifact)
        else:
            logger.warning("libraries.io API error %d for %s", response.status_code, artifact)
            return None

    except requests.RequestException as e:
        logger.error("Request failed for %s: %s", artifact, e)
        return None


def convert_gitbox_to_github(url: str) -> Optional[str]:
    """Convert an Apache GitBox URL to its GitHub equivalent, or return None if not applicable."""
    if "gitbox.apache.org/repos/asf" not in url:
        return None

    parsed = urlparse(url)
    repo_name = None

    params = parse_qs(parsed.query)
    if "p" in params:
        repo_name = params["p"][0]
    elif parsed.path.startswith("/repos/asf/"):
        repo_name = parsed.path.split("/repos/asf/")[1]

    if not repo_name:
        return None

    repo_name = repo_name.split(";")[0]
    repo_name = repo_name.replace(".git", "")

    if not repo_name:
        return None

    return f"https://github.com/apache/{repo_name}"


def load_artifact_ids(csv_paths: list[str]) -> list[str]:
    artifact_ids = []
    for path in csv_paths:
        if not os.path.exists(path):
            logger.warning("File not found, skipping: %s", path)
            continue
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                artifact_id = row.get("artifactId", "").strip()
                if artifact_id:
                    artifact_ids.append(artifact_id)
        logger.info("Loaded artifacts from %s", os.path.basename(path))
    logger.info("Total artifacts to check: %d", len(artifact_ids))
    return artifact_ids


def append_result(path: str, artifact_id: str, repo_url: str) -> None:
    """Append a single result row to the output CSV (progressive save)."""
    write_header = not os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=CSV_DELIMITER)
        if write_header:
            writer.writerow(["artifactId", "repoUrl"])
        writer.writerow([artifact_id, repo_url])


def main() -> None:
    artifact_ids = load_artifact_ids(INPUT_CSVS)

    # Remove existing output to start fresh
    if os.path.exists(OUTPUT_CSV):
        os.remove(OUTPUT_CSV)

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    total_checked = 0
    total_unknown = 0
    total_resolved = 0
    total_not_found_neo4j = 0

    try:
        with driver.session() as session:
            for artifact_id in artifact_ids:
                total_checked += 1

                record = session.run(NEO4J_QUERY, id=artifact_id).single()
                if record is None:
                    logger.debug("Not found in Neo4j: %s", artifact_id)
                    total_not_found_neo4j += 1
                    continue

                if record["repoUrl"] != "unknown":
                    continue

                total_unknown += 1
                logger.info(
                    "[%d/%d] Querying libraries.io for: %s",
                    total_unknown,
                    len(artifact_ids),
                    artifact_id,
                )

                repo_url = get_repository_url(artifact_id)
                if repo_url:
                    converted = convert_gitbox_to_github(repo_url)
                    if converted:
                        logger.info("  -> Converted GitBox: %s -> %s", repo_url, converted)
                        repo_url = converted
                    append_result(OUTPUT_CSV, artifact_id, repo_url)
                    total_resolved += 1
                    logger.info("  -> Found: %s", repo_url)
                else:
                    logger.info("  -> Not found on libraries.io, skipping")

    finally:
        driver.close()

    logger.info(
        "Done. Checked: %d | Unknown in Neo4j: %d | Resolved via libraries.io: %d"
        " | Not found in Neo4j: %d | Output: %s",
        total_checked,
        total_unknown,
        total_resolved,
        total_not_found_neo4j,
        OUTPUT_CSV,
    )


if __name__ == "__main__":
    main()
