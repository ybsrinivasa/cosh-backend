"""
P7-01 — Pre-Migration Check
Scans Cosh 1.0 Connect Data CSV files for non-empty mismatchedIdsValues entries.
These rows cannot be auto-resolved by BL-C-04 string matching and must be
corrected before Connect migration (P7-03) begins.

Usage:
    python scripts/pre_migration_check.py --dir /path/to/cosh1_csvs
    python scripts/pre_migration_check.py --dir /path/to/cosh1_csvs --out report.csv
"""
import csv
import sys
import os
import argparse
from pathlib import Path
from datetime import datetime


MISMATCHED_COLUMN = "mismatchedIdsValues"


def check_file(filepath: Path) -> list[dict]:
    issues = []
    try:
        with open(filepath, encoding="utf-8-sig", errors="replace") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []

            if MISMATCHED_COLUMN not in headers:
                return []  # This CSV doesn't have the mismatch column — skip

            for row_num, row in enumerate(reader, start=2):
                value = (row.get(MISMATCHED_COLUMN) or "").strip()
                if value:
                    issues.append({
                        "file": filepath.name,
                        "row": row_num,
                        "mismatched_value": value,
                        "full_row_preview": "; ".join(
                            f"{k}={v}" for k, v in row.items()
                            if v and k != MISMATCHED_COLUMN
                        )[:200],
                    })
    except Exception as e:
        print(f"  WARNING: could not read {filepath.name}: {e}", file=sys.stderr)

    return issues


def run_check(csv_dir: str, out_file: str = None):
    csv_path = Path(csv_dir)
    if not csv_path.exists():
        print(f"ERROR: directory '{csv_dir}' does not exist.", file=sys.stderr)
        sys.exit(1)

    csv_files = sorted(csv_path.glob("*.csv"))
    if not csv_files:
        print(f"No CSV files found in {csv_dir}")
        return

    print(f"\nCosh 1.0 Pre-Migration Check")
    print(f"Scanning {len(csv_files)} CSV file(s) in: {csv_dir}")
    print(f"Looking for non-empty '{MISMATCHED_COLUMN}' column\n")

    all_issues = []
    files_with_issues = 0
    files_without_column = 0

    for f in csv_files:
        issues = check_file(f)
        if issues is None:
            files_without_column += 1
        elif issues:
            files_with_issues += 1
            all_issues.extend(issues)
            print(f"  {f.name}: {len(issues)} mismatched row(s)")
        else:
            print(f"  {f.name}: OK")

    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"  Files scanned:           {len(csv_files)}")
    print(f"  Files with issues:       {files_with_issues}")
    print(f"  Total mismatched rows:   {len(all_issues)}")

    if all_issues:
        print(f"\n  ACTION REQUIRED: Resolve all {len(all_issues)} mismatched row(s)")
        print(f"  before running Connect migration (P7-03).")

        if out_file:
            out_path = Path(out_file)
            with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=["file", "row", "mismatched_value", "full_row_preview"])
                writer.writeheader()
                writer.writerows(all_issues)
            print(f"\n  Report saved to: {out_path.resolve()}")
        else:
            print(f"\n  Mismatched rows:")
            for issue in all_issues:
                print(f"    {issue['file']} row {issue['row']}: {issue['mismatched_value']}")
            print(f"\n  Tip: add --out report.csv to save a full report file.")
    else:
        print(f"\n  All files are clean. Ready to proceed with P7-03.")

    print()
    return len(all_issues)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cosh 1.0 pre-migration check for mismatchedIdsValues")
    parser.add_argument("--dir", required=True, help="Directory containing Cosh 1.0 Connect Data CSV files")
    parser.add_argument("--out", default=None, help="Optional: path to save the issue report as CSV")
    args = parser.parse_args()

    issue_count = run_check(args.dir, args.out)
    sys.exit(1 if issue_count else 0)
