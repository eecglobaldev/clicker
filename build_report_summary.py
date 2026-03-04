"""
Read results_log CSV files in Report/ or Report_eecglobal/ and write summary.csv
with per-keyword lowest and highest position (best and worst across runs).
Position ordering: (page_num, position) — e.g. 2nd page 25th result is better than 3rd page 5th.

Usage:
  uv run python build_report_summary.py              # run for both Report and Report_eecglobal
  uv run python build_report_summary.py Report       # Report only
  uv run python build_report_summary.py Report_eecglobal
"""

import csv
import sys
from pathlib import Path

# (directory name, glob pattern for log files)
REPORT_CONFIGS: list[tuple[str, str]] = [
    ("Report", "results_log_*.csv"),
    ("Report_eecglobal", "results_log_eecglobal*.csv"),
]


def ordinal(n: int) -> str:
    """Return ordinal form: 1 -> 1st, 2 -> 2nd, 3 -> 3rd, 4 -> 4th, etc."""
    if 10 <= n % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def format_position(page: int, pos: int) -> str:
    return f"{ordinal(page)} page {ordinal(pos)} result"


def build_summary_for(report_dir: Path, glob_pattern: str) -> int:
    """Process all matching CSVs in report_dir, write summary.csv. Returns number of keywords."""
    log_files = sorted(report_dir.glob(glob_pattern))
    if not log_files:
        return 0

    by_keyword: dict[str, list[tuple[int, int]]] = {}

    for path in log_files:
        with path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                keyword = (row.get("keyword") or "").strip()
                if not keyword or (reader.fieldnames and keyword.lower() == "keyword"):
                    continue
                page_s = (row.get("page_num") or "").strip()
                pos_s = (row.get("position") or "").strip()
                if not page_s or not pos_s or pos_s.lower() == "not_found":
                    continue
                try:
                    page_num = int(page_s)
                    position = int(pos_s)
                except ValueError:
                    continue
                by_keyword.setdefault(keyword, []).append((page_num, position))

    rows: list[dict] = []
    for keyword in sorted(by_keyword.keys()):
        positions = by_keyword[keyword]
        low_page, low_pos = min(positions)
        high_page, high_pos = max(positions)
        rows.append({
            "keyword": keyword,
            "lowest_page": low_page,
            "lowest_position": low_pos,
            "lowest_display": format_position(low_page, low_pos),
            "highest_page": high_page,
            "highest_position": high_pos,
            "highest_display": format_position(high_page, high_pos),
            "runs_found": len(positions),
        })

    fieldnames = [
        "keyword",
        "lowest_page",
        "lowest_position",
        "lowest_display",
        "highest_page",
        "highest_position",
        "highest_display",
        "runs_found",
    ]
    out_path = report_dir / "summary.csv"
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return len(rows)


def main() -> None:
    base = Path(__file__).resolve().parent

    if len(sys.argv) > 1:
        name = sys.argv[1]
        configs = [(n, g) for n, g in REPORT_CONFIGS if n == name]
        if not configs:
            print(f"Unknown report: {name}. Use Report or Report_eecglobal.", file=sys.stderr)
            sys.exit(1)
    else:
        configs = REPORT_CONFIGS

    for dir_name, glob_pattern in configs:
        report_dir = base / dir_name
        if not report_dir.exists():
            print(f"Skipping {dir_name}/ (not found)")
            continue
        count = build_summary_for(report_dir, glob_pattern)
        print(f"Wrote {count} keywords to {report_dir / 'summary.csv'}")


if __name__ == "__main__":
    main()
