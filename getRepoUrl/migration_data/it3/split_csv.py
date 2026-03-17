import csv
import os

INPUT_FILE = os.path.join(os.path.dirname(__file__), "githubRepo.csv")
NOT_FOUND_FILE = os.path.join(os.path.dirname(__file__), "notFound.csv")
FOUND_FILE = os.path.join(os.path.dirname(__file__), "found.csv")
TRASH_FILE = os.path.join(os.path.dirname(__file__), "trash.csv")

with open(INPUT_FILE, newline="", encoding="utf-8") as infile, \
     open(NOT_FOUND_FILE, "w", newline="", encoding="utf-8") as not_found_out, \
     open(FOUND_FILE, "w", newline="", encoding="utf-8") as found_out, \
     open(TRASH_FILE, "w", newline="", encoding="utf-8") as trash_out:

    reader = csv.DictReader(infile, delimiter=";")

    not_found_writer = csv.DictWriter(not_found_out, fieldnames=reader.fieldnames, delimiter=";")
    found_writer = csv.DictWriter(found_out, fieldnames=reader.fieldnames, delimiter=";")
    trash_writer = csv.DictWriter(trash_out, fieldnames=reader.fieldnames, delimiter=";")

    not_found_writer.writeheader()
    found_writer.writeheader()
    trash_writer.writeheader()

    not_found_count = 0
    found_count = 0
    trash_count = 0

    for row in reader:
        repo = row["repository"]
        if repo == "unknown":
            not_found_writer.writerow(row)
            not_found_count += 1
        elif repo.startswith("http"):
            found_writer.writerow(row)
            found_count += 1
        else:
            trash_writer.writerow(row)
            trash_count += 1

print(f"notFound.csv : {not_found_count} lignes")
print(f"found.csv    : {found_count} lignes")
print(f"trash.csv    : {trash_count} lignes")
