"""
Generate keyword lists for eecglobal.com and winginstitute.com using Gujarat cities,
then merge with existing ptetestindia.com keywords into keywords_unified.csv.

Gujarat cities sourced from: https://www.mapmyindia.com/downloads/City-List.pdf

Usage:
    uv run python generate_keywords_v2.py
"""

import csv
import random
from pathlib import Path

BASE = Path(__file__).resolve().parent

# ---------------------------------------------------------------------------
# Gujarat cities (from MapMyIndia City-List.pdf)
# Excludes industrial areas, GIDC zones, SIR zones, and hamlets.
# ---------------------------------------------------------------------------
GUJARAT_CITIES = [
    # Major cities (population > 100k)
    "Ahmedabad",
    "Surat",
    "Vadodara",
    "Rajkot",
    "Bhavnagar",
    "Jamnagar",
    "Junagadh",
    "Gandhinagar",
    "Gandhidham",
    "Anand",
    "Nadiad",
    "Morbi",
    "Mehsana",
    "Surendranagar",
    "Bharuch",
    "Navsari",
    "Valsad",
    "Vapi",
    "Porbandar",
    "Godhra",
    "Bhuj",
    "Palanpur",
    "Ankleshwar",
    "Botad",
    "Amreli",
    "Patan",
]

# ---------------------------------------------------------------------------
# eecglobal.com — test prep coaching
# ---------------------------------------------------------------------------
EEC_COURSES = ["IELTS", "TOEFL", "GRE", "PTE", "DSAT", "Digital SAT", "D-SAT", "SAT"]
EEC_VARIATIONS = ["coaching", "classes", "training", "institute"]

# ---------------------------------------------------------------------------
# winginstitute.com — aviation & hospitality
# ---------------------------------------------------------------------------
WINGS_COURSES = [
    "air hostess",
    "cabin crew",
    "aviation",
    "ground staff",
    "hotel management",
    "travel and tourism",
]
WINGS_VARIATIONS = ["coaching", "classes", "training", "institute"]

# ---------------------------------------------------------------------------
# Misspelling dictionary — applied randomly to ~8% of keywords
# ---------------------------------------------------------------------------
MISSPELLINGS = {
    "coaching": ["coching", "coachin"],
    "tuition": ["tution", "tusion"],
    "classes": ["clasess", "clases"],
    "training": ["traning", "trainng"],
    "institute": ["insitute", "institue"],
}

MISSPELLING_RATE = 0.08


def maybe_misspell(keyword: str) -> str:
    if random.random() > MISSPELLING_RATE:
        return keyword
    kw_lower = keyword.lower()
    candidates = [word for word in MISSPELLINGS if word in kw_lower]
    if not candidates:
        return keyword
    word = random.choice(candidates)
    replacement = random.choice(MISSPELLINGS[word])
    idx = kw_lower.find(word)
    return keyword[:idx] + replacement + keyword[idx + len(word) :]


def generate_domain_keywords(
    courses: list[str], variations: list[str], cities: list[str]
) -> list[str]:
    """Generate 'Best {course} {variation} in {city}' keywords."""
    keywords: list[str] = []
    seen: set[str] = set()

    for course in courses:
        for variation in variations:
            for city in cities:
                kw = f"Best {course} {variation} in {city}"
                kw_final = maybe_misspell(kw)
                lower = kw_final.lower()
                if lower not in seen:
                    seen.add(lower)
                    keywords.append(kw_final)

    random.shuffle(keywords)
    return keywords


def load_ptetest_keywords() -> list[str]:
    """Load existing ptetestindia keywords (unchanged)."""
    path = BASE / "keywords_ptetest.csv"
    if not path.exists():
        print(f"WARNING: {path.name} not found. Skipping ptetest keywords.")
        return []
    keywords = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            kw = row[0].strip()
            if not kw or kw.lower() == "keyword":
                continue
            keywords.append(kw)
    return keywords


def write_domain_csv(filename: str, keywords: list[str]) -> None:
    """Write a single-column keyword CSV."""
    path = BASE / filename
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["keyword"])
        for kw in keywords:
            writer.writerow([kw])
    print(f"  {filename}: {len(keywords)} keywords")


def write_unified_csv(
    eec_keywords: list[str],
    wings_keywords: list[str],
    pte_keywords: list[str],
) -> None:
    """Merge all keywords into keywords_unified.csv with target_domain column."""
    rows: list[tuple[str, str]] = []
    for kw in eec_keywords:
        rows.append((kw, "eecglobal.com"))
    for kw in pte_keywords:
        rows.append((kw, "ptetestindia.com"))
    for kw in wings_keywords:
        rows.append((kw, "winginstitute.com"))

    output = BASE / "keywords_unified.csv"
    with output.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["keyword", "target_domain"])
        for kw, domain in rows:
            writer.writerow([kw, domain])

    print(f"\n  keywords_unified.csv: {len(rows)} total keywords")
    print(f"    eecglobal.com:      {len(eec_keywords)}")
    print(f"    ptetestindia.com:    {len(pte_keywords)}")
    print(f"    winginstitute.com:   {len(wings_keywords)}")


def main():
    random.seed(42)

    print(f"Gujarat cities: {len(GUJARAT_CITIES)}")
    print(f"EEC courses: {len(EEC_COURSES)} x {len(EEC_VARIATIONS)} variations")
    print(f"Wings courses: {len(WINGS_COURSES)} x {len(WINGS_VARIATIONS)} variations")
    print()

    # Generate keywords
    print("Generating keywords...")
    eec_keywords = generate_domain_keywords(EEC_COURSES, EEC_VARIATIONS, GUJARAT_CITIES)
    wings_keywords = generate_domain_keywords(WINGS_COURSES, WINGS_VARIATIONS, GUJARAT_CITIES)
    pte_keywords = load_ptetest_keywords()

    # Write individual domain CSVs
    write_domain_csv("keywords_eecglobal.csv", eec_keywords)
    write_domain_csv("keywords_wings.csv", wings_keywords)

    # Write unified CSV
    write_unified_csv(eec_keywords, wings_keywords, pte_keywords)

    # Sample output
    print("\n--- Sample eecglobal keywords ---")
    for kw in eec_keywords[:10]:
        print(f"  {kw}")

    print("\n--- Sample wings keywords ---")
    for kw in wings_keywords[:10]:
        print(f"  {kw}")


if __name__ == "__main__":
    main()
