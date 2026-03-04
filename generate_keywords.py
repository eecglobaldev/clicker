"""
Generate keyword list for eecglobal.com → keywords_eecglobal.csv
Strictly follows 3 formats with variable substitution + random misspellings.
"""

import csv
import random
from pathlib import Path

# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------
COURSES = [
    "IELTS", "TOEFL", "GRE", "PTE",
    "DSAT", "Digital SAT", "D-SAT", "SAT",
]

COACHING_TYPES = ["coaching", "tuition", "classes"]

PLACES = [
    "Alkapuri, Vadodara",
    "Nizampura, Vadodara",
    "Manjalpur, Vadodara",
    "New Vip Road, Vadodara",
    "Vadodara",
    "Nadiad",
    "Vallabh Vidyanagar, Anand",
    "Anand",
    "Parvat Patia, Surat",
    "Mota Varachha, Surat",
    "Katargam, Surat",
    "Ghod Dod Road, Surat",
    "Vesu, Surat",
    "Surat",
    "Vapi",
    "Navsari",
    "Bharuch",
    "Memnagar, Ahmedabad",
    "Ghatlodiya, Ahmedabad",
    "Chandkheda, Ahmedabad",
    "Maninagar, Ahmedabad",
    "Odhav, Ahmedabad",
    "Nikol, Ahmedabad",
    "Bapunagar, Ahmedabad",
    "Naroda, Ahmedabad",
    "Ahmedabad",
    "Kalol",
    "Himatnagar",
    "Mehsana",
    "Visnagar",
    "Gujarat",
    "India",
]

CONSULTANT_TYPES = ["study abroad consultant", "Education consultant"]

COUNTRY_FORMATS = [
    lambda c: c,                    # "USA"
    lambda c: f"study in {c}",      # "study in USA"
]

COUNTRIES = [
    "USA", "United Kingdom", "Canada", "Australia", "New Zealand",
    "Ireland", "Germany", "France", "Netherlands", "Italy",
    "Spain", "Portugal", "Belgium", "Switzerland", "Austria",
    "Luxembourg", "Malta", "Cyprus", "Greece", "Finland",
    "Sweden", "Denmark", "Norway", "Iceland", "Poland",
    "Czech Republic", "Hungary", "Romania", "Croatia", "Slovakia",
    "Slovenia", "Estonia", "Latvia", "Lithuania", "Bulgaria",
    "Japan", "South Korea", "Singapore", "UAE", "Russia",
]

ADMISSION_TYPES = ["admission", "visa"]

# ---------------------------------------------------------------------------
# Misspelling dictionary — applied randomly to ~8% of keywords
# ---------------------------------------------------------------------------
MISSPELLINGS = {
    "coaching": ["coching", "coachin"],
    "tuition": ["tution", "tusion"],
    "classes": ["clasess", "clases"],
    "consultant": ["consulatant", "consaltant"],
    "education": ["eduction", "educaton"],
    "abroad": ["abrod", "aboad"],
    "admission": ["addmission", "admision"],
    "studying": ["studing", "studyng"],
}

MISSPELLING_RATE = 0.08


def maybe_misspell(keyword: str) -> str:
    """With MISSPELLING_RATE probability, replace one word with a misspelling."""
    if random.random() > MISSPELLING_RATE:
        return keyword
    kw_lower = keyword.lower()
    candidates = [word for word in MISSPELLINGS if word in kw_lower]
    if not candidates:
        return keyword
    word = random.choice(candidates)
    replacement = random.choice(MISSPELLINGS[word])
    idx = kw_lower.find(word)
    return keyword[:idx] + replacement + keyword[idx + len(word):]


def generate_keywords() -> list[str]:
    keywords: list[str] = []
    seen: set[str] = set()

    def add(kw: str):
        kw = kw.strip()
        kw_final = maybe_misspell(kw)
        lower = kw_final.lower()
        if lower not in seen:
            seen.add(lower)
            keywords.append(kw_final)

    # Format 1: Best {course} {coaching/tuition/classes} in {place}
    for course in COURSES:
        for ctype in COACHING_TYPES:
            for place in PLACES:
                add(f"Best {course} {ctype} in {place}")

    # Format 2: Best {consultant_type} for {country / study in country}
    for ctype in CONSULTANT_TYPES:
        for cfmt in COUNTRY_FORMATS:
            for country in COUNTRIES:
                add(f"Best {ctype} for {cfmt(country)}")

    # Format 3: How to get {admission/visa} for studying in {country}
    for atype in ADMISSION_TYPES:
        for country in COUNTRIES:
            add(f"How to get {atype} for studying in {country}")

    random.shuffle(keywords)
    return keywords


def verify_coverage(keywords: list[str]):
    joined = "\n".join(keywords).lower()
    missing_courses = [c for c in COURSES if c.lower() not in joined]
    missing_places = [p for p in PLACES if p.lower() not in joined]
    missing_countries = [c for c in COUNTRIES if c.lower() not in joined]

    if missing_courses:
        print(f"⚠ Missing courses: {missing_courses}")
    if missing_places:
        print(f"⚠ Missing places: {missing_places}")
    if missing_countries:
        print(f"⚠ Missing countries: {missing_countries}")
    if not missing_courses and not missing_places and not missing_countries:
        print("✅ All courses, places, and countries covered.")


def main():
    random.seed(42)
    keywords = generate_keywords()

    out_path = Path(__file__).resolve().parent / "keywords_eecglobal.csv"
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["keyword"])
        for kw in keywords:
            writer.writerow([kw])

    print(f"Generated {len(keywords)} keywords → {out_path.name}")
    verify_coverage(keywords)

    print("\n--- Sample keywords (first 20) ---")
    for kw in keywords[:20]:
        print(f"  {kw}")


if __name__ == "__main__":
    main()
