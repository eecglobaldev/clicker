"""
Combine keywords.csv and keywords_eecglobal.csv into a new file.
Pattern: 2 keywords from eecglobal, then 1 keyword from main.
"""

import csv
from pathlib import Path


def read_keywords(filepath: Path) -> list[str]:
    """Read keywords from CSV file, handling headers and quotes."""
    keywords = []
    with filepath.open(newline="", encoding="utf-8") as f:
        # Try to detect if there's a header
        sample = f.read(1024)
        f.seek(0)

        # Check if first line looks like a header
        first_line = sample.split("\n")[0] if sample else ""
        has_header = "keyword" in first_line.lower() or first_line.strip().startswith("keyword")

        reader = csv.reader(f)
        for row in reader:
            if not row:
                continue
            keyword = row[0].strip()
            if not keyword:
                continue
            # Skip header row if present
            if has_header and reader.line_num == 1 and keyword.lower() == "keyword":
                continue
            keywords.append(keyword)
    return keywords


def combine_keywords(
    main_keywords: list[str], eecglobal_keywords: list[str], pattern: tuple[int, int] = (2, 1)
) -> list[str]:
    """
    Combine keywords in specified pattern.
    Default pattern: 2 from eecglobal, 1 from main.
    """
    result = []
    eec_idx = 0
    main_idx = 0
    eec_count, main_count = pattern

    while eec_idx < len(eecglobal_keywords) or main_idx < len(main_keywords):
        # Add eecglobal keywords (2 by default)
        for _ in range(eec_count):
            if eec_idx < len(eecglobal_keywords):
                result.append(eecglobal_keywords[eec_idx])
                eec_idx += 1

        # Add main keywords (1 by default)
        for _ in range(main_count):
            if main_idx < len(main_keywords):
                result.append(main_keywords[main_idx])
                main_idx += 1

    return result


def write_keywords(keywords: list[str], filepath: Path) -> None:
    """Write keywords to CSV file with proper quoting."""
    with filepath.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(["keyword"])  # Header
        for kw in keywords:
            writer.writerow([kw])


def main():
    base_dir = Path(__file__).resolve().parent

    main_file = base_dir / "keywords.csv"
    eecglobal_file = base_dir / "keywords_eecglobal.csv"
    output_file = base_dir / "keywords_combined.csv"

    # Read both files
    print(f"Reading {main_file.name}...")
    main_keywords = read_keywords(main_file)
    print(f"  -> {len(main_keywords)} keywords")

    print(f"Reading {eecglobal_file.name}...")
    eecglobal_keywords = read_keywords(eecglobal_file)
    print(f"  -> {len(eecglobal_keywords)} keywords")

    # Combine with pattern: 2 eecglobal, 1 main
    print("\nCombining with pattern: 2 eecglobal + 1 main...")
    combined = combine_keywords(main_keywords, eecglobal_keywords, pattern=(2, 1))
    print(f"  -> {len(combined)} total keywords")

    # Write output
    write_keywords(combined, output_file)
    print(f"\nWritten to: {output_file.name}")

    # Show sample
    print("\n--- First 10 keywords in combined list ---")
    for i, kw in enumerate(combined[:10], 1):
        source = ""
        # Determine source for display
        if i % 3 in (1, 2):  # Positions 1, 2, 4, 5, 7, 8...
            source = "(eecglobal)"
        else:  # Positions 3, 6, 9...
            source = "(main)"
        print(f"  {i}. {kw} {source}")

    print(f"\nBreakdown:")
    print(f"  - eecglobal keywords used: {min(len(eecglobal_keywords), (len(combined) // 3) * 2 + min(len(eecglobal_keywords) % 3, 2))}")
    print(f"  - main keywords used: {min(len(main_keywords), len(combined) // 3 + (1 if len(combined) % 3 == 0 else 0))}")


if __name__ == "__main__":
    main()
