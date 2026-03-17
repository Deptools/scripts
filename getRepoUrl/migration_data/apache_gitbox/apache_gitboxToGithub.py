#!/usr/bin/env python3
"""
convert_repos.py

Description
-----------
Convertit les URLs GitBox Apache vers leurs équivalents GitHub dans un CSV.

Formats GitBox supportés :
1) https://gitbox.apache.org/repos/asf/commons-compress
2) https://gitbox.apache.org/repos/asf?p=camel-kafka-connector.git
3) https://gitbox.apache.org/repos/asf/commons-lang.git
4) https://gitbox.apache.org/repos/asf/commons-lang.git;a=summary

Entrée
------
CSV séparé par ';' avec au minimum les colonnes :
artifact;repo

Sortie
------
output.csv : lignes converties avec URLs GitHub
error.csv  : lignes dont l'URL ne correspond pas à un format reconnu

Usage
-----
python convert_repos.py input.csv output.csv
"""

import csv
import sys
from urllib.parse import urlparse, parse_qs


def convert_gitbox_to_github(url):
    """
    Convertit une URL GitBox vers GitHub.
    Retourne None si le format n'est pas reconnu.
    """

    if "gitbox.apache.org/repos/asf" not in url:
        return None

    parsed = urlparse(url)
    repo_name = None

    # Format 2 : ?p=repo.git
    params = parse_qs(parsed.query)
    if "p" in params:
        repo_name = params["p"][0]

    # Format 1 et 3 : /repos/asf/repo ou /repos/asf/repo.git
    elif parsed.path.startswith("/repos/asf/"):
        repo_name = parsed.path.split("/repos/asf/")[1]

    if not repo_name:
        return None

    # Nettoyage
    repo_name = repo_name.split(";")[0]  # enlève ;a=summary
    repo_name = repo_name.replace(".git", "")

    if not repo_name:
        return None

    return f"https://github.com/apache/{repo_name}"


def convert_csv(input_file, output_file):

    error_file = "error.csv"

    with open(input_file, newline='', encoding="utf-8") as infile, \
         open(output_file, "w", newline='', encoding="utf-8") as outfile, \
         open(error_file, "w", newline='', encoding="utf-8") as errfile:

        reader = csv.DictReader(infile, delimiter=";")

        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames, delimiter=";")
        error_writer = csv.DictWriter(errfile, fieldnames=reader.fieldnames, delimiter=";")

        writer.writeheader()
        error_writer.writeheader()

        for row in reader:

            converted = convert_gitbox_to_github(row["repo"])

            if converted:
                row["repo"] = converted
                writer.writerow(row)
            else:
                error_writer.writerow(row)


if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: python convert_repos.py input.csv output.csv")
        sys.exit(1)

    convert_csv(sys.argv[1], sys.argv[2])