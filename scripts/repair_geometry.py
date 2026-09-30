#!/usr/bin/env python3
"""Repair OGM Aardvark Geometry fields in one or more primary CSV exports."""

from __future__ import annotations

import argparse
from collections import Counter
import csv
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.geometry_repair import repair_geometry_fields  # noqa: E402


REPORT_COLUMNS = [
    "input_file",
    "row_number",
    "record_key",
    "ID",
    "Title",
    "old_bounding_box",
    "new_bounding_box",
    "old_geometry",
    "new_geometry",
    "action",
    "requires_review",
    "note",
]


def repair_csv(
    input_path: Path,
    output_path: Path,
    *,
    limit: int | None = None,
) -> tuple[list[dict[str, str]], Counter]:
    """Repair a primary CSV and return its record-level report and action counts."""
    report_rows: list[dict[str, str]] = []
    counts: Counter = Counter()

    with input_path.open(encoding="utf-8-sig", errors="replace", newline="") as source:
        reader = csv.DictReader(source)
        if not reader.fieldnames:
            raise ValueError(f"{input_path} is missing a header row.")
        missing = {"Bounding Box", "Geometry"} - set(reader.fieldnames)
        if missing:
            raise ValueError(
                f"{input_path} is missing required columns: {', '.join(sorted(missing))}"
            )

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8", newline="") as destination:
            writer = csv.DictWriter(destination, fieldnames=reader.fieldnames)
            writer.writeheader()

            for index, row in enumerate(reader, start=1):
                if limit is not None and index > limit:
                    break

                original = dict(row)
                repair = repair_geometry_fields(
                    row.get("Bounding Box"), row.get("Geometry")
                )
                row["Bounding Box"] = repair.bounding_box
                row["Geometry"] = repair.geometry

                changed_columns = {
                    column
                    for column in reader.fieldnames
                    if original.get(column, "") != row.get(column, "")
                }
                if not changed_columns.issubset({"Bounding Box", "Geometry"}):
                    raise AssertionError(
                        f"Unexpected changes in row {index + 1}: {sorted(changed_columns)}"
                    )

                writer.writerow(row)
                counts[repair.action] += 1
                counts["rows"] += 1
                if changed_columns:
                    counts["changed_rows"] += 1
                if repair.requires_review:
                    counts["review_rows"] += 1

                report_rows.append(
                    {
                        "input_file": str(input_path),
                        "row_number": str(index + 1),
                        "record_key": _record_key(row),
                        "ID": row.get("ID", ""),
                        "Title": row.get("Title", ""),
                        "old_bounding_box": original.get("Bounding Box", ""),
                        "new_bounding_box": row.get("Bounding Box", ""),
                        "old_geometry": original.get("Geometry", ""),
                        "new_geometry": row.get("Geometry", ""),
                        "action": repair.action,
                        "requires_review": str(repair.requires_review).lower(),
                        "note": repair.note,
                    }
                )

    return report_rows, counts


def _record_key(row: dict[str, str]) -> str:
    for column in ("ID", "County", "City", "Label", "Title", "Name"):
        value = str(row.get(column, "") or "").strip()
        if value:
            return value
    return ""


def write_report(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as destination:
        writer = csv.DictWriter(destination, fieldnames=REPORT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_csvs", nargs="+", type=Path)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "outputs" / "geometry-repair",
        help="Directory for repaired CSVs and reports.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Process only the first N records of each input for a smoke test.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.limit is not None and args.limit < 1:
        raise SystemExit("--limit must be a positive integer.")

    all_reports: list[dict[str, str]] = []
    suffix = "-sample-fixed.csv" if args.limit is not None else "-fixed.csv"
    for input_path in args.input_csvs:
        if not input_path.exists():
            raise SystemExit(f"Input CSV not found: {input_path}")
        output_path = args.output_dir / f"{input_path.stem}{suffix}"
        reports, counts = repair_csv(input_path, output_path, limit=args.limit)
        all_reports.extend(reports)
        print(f"Input: {input_path}")
        print(f"Output: {output_path}")
        print(
            f"Rows: {counts['rows']}; changed: {counts['changed_rows']}; review: {counts['review_rows']}"
        )
        for action, count in sorted(counts.items()):
            if action not in {"rows", "changed_rows", "review_rows"}:
                print(f"  {action}: {count}")

    report_path = args.output_dir / "geometry-remediation-report.csv"
    review_path = args.output_dir / "geometry-manual-review.csv"
    write_report(report_path, all_reports)
    write_report(
        review_path,
        [row for row in all_reports if row["requires_review"] == "true"],
    )
    print(f"Report: {report_path}")
    print(f"Manual review: {review_path}")


if __name__ == "__main__":
    main()
