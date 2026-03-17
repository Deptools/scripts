#!/usr/bin/env python3
"""
Script to check if new artifacts (from MAVEN_CENTRAL and MAVEN_GOOGLE CSVs)
have a repoUrl assigned in Neo4j. Outputs a new CSV with artifacts whose
repoUrl is 'unknown'.
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

BASE_DIR = os.path.dirname(__file__)
INPUT_CSVS = [
    os.path.join(BASE_DIR, "newArtifacts_MAVEN_CENTRAL.csv"),
    os.path.join(BASE_DIR, "newArtifacts_MAVEN_GOOGLE.csv"),
]
OUTPUT_CSV = os.path.join(BASE_DIR, "newArtifacts_noRepo.csv")

QUERY = "MATCH (n:Artifact {id: $id}) RETURN n.repoUrl AS repoUrl"


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


def write_output_csv(path: str, artifact_ids: list[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["artifactId"])
        for artifact_id in artifact_ids:
            writer.writerow([artifact_id])


def main() -> None:
    artifact_ids = load_artifact_ids(INPUT_CSVS)

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    unknown_artifacts = []
    not_found = 0

    try:
        with driver.session() as session:
            for artifact_id in artifact_ids:
                result = session.run(QUERY, id=artifact_id)
                record = result.single()
                if record is None:
                    logger.debug("Artifact not found in Neo4j: %s", artifact_id)
                    not_found += 1
                    continue
                repo_url = record["repoUrl"]
                if repo_url == "unknown":
                    unknown_artifacts.append(artifact_id)
    finally:
        driver.close()

    write_output_csv(OUTPUT_CSV, unknown_artifacts)

    logger.info(
        "Done. Checked: %d | Unknown repoUrl: %d | Not found in Neo4j: %d | Output: %s",
        len(artifact_ids),
        len(unknown_artifacts),
        not_found,
        OUTPUT_CSV,
    )


if __name__ == "__main__":
    main()
