#!/usr/bin/env python3
import csv
import sys
from pathlib import Path


def load_codes(csv_path: Path) -> set[str]:
    with csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None or "Code" not in reader.fieldnames:
            raise ValueError(f'{csv_path} is missing required "Code" column')

        codes = set()
        for row_number, row in enumerate(reader, start=2):
            code = (row.get("Code") or "").strip()
            if not code:
                continue
            codes.add(code)

    return codes


def main() -> int:
    all_published_path = Path("allPublished.csv")
    series_path = Path("series.csv")

    try:
        all_published_codes = load_codes(all_published_path)
        series_codes = load_codes(series_path)
    except FileNotFoundError as exc:
        print(f"File not found: {exc.filename}", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    missing_codes = sorted(all_published_codes - series_codes)

    print(f"Codes in {all_published_path} not present in {series_path}: {len(missing_codes)}")
    for code in missing_codes:
        print(code)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
