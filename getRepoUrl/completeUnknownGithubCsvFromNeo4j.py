#!/usr/bin/env python3
"""
Script to get Artifact nodes ids from the neo4j graph with  WHERE repoUrl = "unknown".
Updates the githubRepo.csv file to add this artifact id in it.
"""

import csv
import logging
import os

from neo4j import GraphDatabase

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password1"

CSV_PATH = os.path.join(os.path.dirname(__file__), "maven_githubRepo.csv")
CSV_DELIMITER = ";"
BATCH_SIZE = 1000

QUERY = 'MATCH (n:Artifact) WHERE n.repoUrl = "unknown" RETURN n.id AS id'


def load_existing_artifacts(csv_path: str) -> set:
    existing = set()
    if not os.path.exists(csv_path):
        return existing
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=CSV_DELIMITER)
        for row in reader:
            artifact = row.get("artifact") or row.get("Artifact")
            if artifact:
                existing.add(artifact.strip())
    logger.info("Loaded %d existing artifacts from CSV", len(existing))
    return existing


def append_to_csv(csv_path: str, rows: list[str]) -> None:
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=CSV_DELIMITER)
        for artifact_id in rows:
            writer.writerow([artifact_id, "unknown"])


def main() -> None:
    existing = load_existing_artifacts(CSV_PATH)

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    total_found = 0
    total_added = 0
    total_skipped = 0
    batch: list[str] = []

    try:
        with driver.session() as session:
            result = session.run(QUERY)
            for record in result:
                artifact_id = record["id"]
                if artifact_id is None:
                    continue
                artifact_id = str(artifact_id).strip()
                total_found += 1

                if artifact_id in existing:
                    total_skipped += 1
                else:
                    batch.append(artifact_id)
                    existing.add(artifact_id)
                    total_added += 1

                if len(batch) >= BATCH_SIZE:
                    append_to_csv(CSV_PATH, batch)
                    logger.info("Written batch of %d lines (total added so far: %d)", len(batch), total_added)
                    batch = []

        if batch:
            append_to_csv(CSV_PATH, batch)
            logger.info("Written final batch of %d lines", len(batch))

    finally:
        driver.close()

    logger.info(
        "Done. Total found: %d | Added: %d | Skipped (duplicates): %d",
        total_found,
        total_added,
        total_skipped,
    )


if __name__ == "__main__":
    main()
