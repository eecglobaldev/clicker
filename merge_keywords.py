"""
One-time migration: merge keywords_wings.csv + keywords_eecglobal.csv into keywords_unified.csv.

Usage:
    uv run python merge_keywords.py
"""

import csv
from pathlib import Path

BASE = Path(__file__).resolve().parent

# Source files and their target domains
SOURCES: list[tuple[str, str]] = [
    ("keywords_eecglobal.csv", "eecglobal.com"),
    ("keywords_ptetest.csv", "ptetestindia.com"),
    ("keywords_wings.csv", "winginstitute.com"),
]

OUTPUT = BASE / "keywords_unified.csv"


def main() -> None:
    rows: list[tuple[str, str]] = []

    for filename, domain in SOURCES:
        path = BASE / filename
        if not path.exists():
            print(f"Skipping {filename} (file not found)")
            continue
        count = 0
        with path.open(newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row:
                    continue
                kw = row[0].strip()
                if not kw or kw.lower() == "keyword":
                    continue
                rows.append((kw, domain))
                count += 1
        print(f"Read {count} keywords from {filename} → {domain}")

    if not rows:
        print("No keywords found. Nothing to write.")
        return

    with OUTPUT.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["keyword", "target_domain"])
        for kw, domain in rows:
            writer.writerow([kw, domain])

    print(f"\nWrote {len(rows)} keywords to {OUTPUT.name}")


if __name__ == "__main__":
    main()
