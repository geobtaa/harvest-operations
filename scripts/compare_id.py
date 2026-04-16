#!/usr/bin/env python3
import argparse
import csv
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare rows in a subset CSV against an original CSV by ID and "
            "output the changed rows from the subset."
        )
    )
    parser.add_argument(
        "subset_csv",
        nargs="?",
        default="codework/3-allPublished.csv",
        help="Subset CSV containing updated rows. Default: codework/3-allPublished.csv",
    )
    parser.add_argument(
        "original_csv",
        nargs="?",
        default="codework/orig.csv",
        help="Original CSV to compare against. Default: codework/orig.csv",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="codework/different_rows.csv",
        help="Output CSV for differing subset rows. Default: codework/different_rows.csv",
    )
    return parser.parse_args()


def load_rows_by_id(csv_path: Path) -> tuple[list[str], dict[str, dict[str, str]]]:
    with csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"{csv_path} is missing headers")
        if "ID" not in reader.fieldnames:
            raise ValueError(f'{csv_path} is missing required "ID" column')

        rows_by_id: dict[str, dict[str, str]] = {}
        for row_number, row in enumerate(reader, start=2):
            row_id = (row.get("ID") or "").strip()
            if not row_id:
                raise ValueError(f'{csv_path} row {row_number} is missing an "ID" value')
            if row_id in rows_by_id:
                raise ValueError(f'{csv_path} row {row_number} has duplicate ID "{row_id}"')
            rows_by_id[row_id] = {field: row.get(field, "") for field in reader.fieldnames}

    return reader.fieldnames, rows_by_id


def find_different_subset_rows(
    subset_rows: dict[str, dict[str, str]],
    original_rows: dict[str, dict[str, str]],
    fieldnames: list[str],
) -> list[dict[str, str]]:
    differing_rows: list[dict[str, str]] = []

    for row_id, subset_row in subset_rows.items():
        original_row = original_rows.get(row_id)
        if original_row is None:
            differing_rows.append(subset_row)
            continue

        if any(subset_row.get(field, "") != original_row.get(field, "") for field in fieldnames):
            differing_rows.append(subset_row)

    return differing_rows


def write_rows(csv_path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    subset_path = Path(args.subset_csv)
    original_path = Path(args.original_csv)
    output_path = Path(args.output)

    try:
        subset_fields, subset_rows = load_rows_by_id(subset_path)
        original_fields, original_rows = load_rows_by_id(original_path)
    except FileNotFoundError as exc:
        print(f"File not found: {exc.filename}", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if subset_fields != original_fields:
        print("CSV headers do not match between input files", file=sys.stderr)
        return 1

    differing_rows = find_different_subset_rows(subset_rows, original_rows, subset_fields)
    write_rows(output_path, subset_fields, differing_rows)

    print(
        f"Wrote {len(differing_rows)} differing rows from {subset_path} to {output_path}",
        file=sys.stdout,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
