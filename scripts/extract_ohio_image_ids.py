#!/usr/bin/env python3
"""
Read Ohio manifest URLs from a CSV and append the first image @id from each JSON.

The default input is inputs/ohio-dist.csv. The output preserves every input
column and appends a first_image_id column.
"""

from __future__ import annotations

import argparse
import csv
import time
from pathlib import Path
from typing import Any

import requests


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

DEFAULT_INPUT_CSV = PROJECT_ROOT / "inputs" / "ohio-dist.csv"
DEFAULT_OUTPUT_CSV = PROJECT_ROOT / "inputs" / "ohio-dist-with-image-ids.csv"
DEFAULT_URL_FIELD = "distribution_url"
DEFAULT_OUTPUT_FIELD = "first_image_id"
DEFAULT_TIMEOUT = 30
DEFAULT_RETRIES = 3
DEFAULT_RETRY_WAIT = 1.0


def resolve_path(path_value: str | Path) -> Path:
    candidate = Path(path_value).expanduser()
    if candidate.is_absolute():
        return candidate
    return (PROJECT_ROOT / candidate).resolve()


def read_rows(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        return [dict(row) for row in reader], fieldnames


def write_rows(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def first_id_in_images_section(value: Any) -> str:
    if isinstance(value, dict):
        images_value = value.get("images")
        if images_value is not None:
            image_id = first_at_id(images_value)
            if image_id:
                return image_id

        for child in value.values():
            image_id = first_id_in_images_section(child)
            if image_id:
                return image_id

    if isinstance(value, list):
        for child in value:
            image_id = first_id_in_images_section(child)
            if image_id:
                return image_id

    return ""


def first_at_id(value: Any) -> str:
    if isinstance(value, dict):
        at_id = value.get("@id")
        if isinstance(at_id, str) and at_id.strip():
            return at_id.strip()

        for child in value.values():
            image_id = first_at_id(child)
            if image_id:
                return image_id

    if isinstance(value, list):
        for child in value:
            image_id = first_at_id(child)
            if image_id:
                return image_id

    return ""


def fetch_json(
    session: requests.Session,
    url: str,
    *,
    timeout: int,
    retries: int,
    retry_wait: float,
) -> Any:
    last_error: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt == retries:
                break
            time.sleep(retry_wait * attempt)

    raise RuntimeError(f"Could not fetch valid JSON from {url}: {last_error}")


def add_image_ids(
    rows: list[dict[str, str]],
    *,
    url_field: str,
    output_field: str,
    timeout: int,
    retries: int,
    retry_wait: float,
    progress: bool,
) -> list[dict[str, str]]:
    with requests.Session() as session:
        session.headers.update({"Accept": "application/json"})

        row_count = len(rows)
        for row_number, row in enumerate(rows, start=1):
            url = str(row.get(url_field) or "").strip()
            if progress:
                print(f"Row {row_number}/{row_count}: {url}", flush=True)
            if not url:
                row[output_field] = ""
                continue

            try:
                payload = fetch_json(
                    session,
                    url,
                    timeout=timeout,
                    retries=retries,
                    retry_wait=retry_wait,
                )
                row[output_field] = first_id_in_images_section(payload)
            except RuntimeError as exc:
                row[output_field] = ""
                print(f"Row {row_number} failed: {exc}")

    return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Append the first @id found in an images section for each JSON URL "
            "in a CSV."
        )
    )
    parser.add_argument(
        "--input",
        default=str(DEFAULT_INPUT_CSV),
        help=f"Input CSV path. Default: {DEFAULT_INPUT_CSV}",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_CSV),
        help=f"Output CSV path. Default: {DEFAULT_OUTPUT_CSV}",
    )
    parser.add_argument(
        "--url-field",
        default=DEFAULT_URL_FIELD,
        help=f"Column containing JSON URLs. Default: {DEFAULT_URL_FIELD}",
    )
    parser.add_argument(
        "--output-field",
        default=DEFAULT_OUTPUT_FIELD,
        help=f"Name of appended @id column. Default: {DEFAULT_OUTPUT_FIELD}",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help=f"Request timeout in seconds. Default: {DEFAULT_TIMEOUT}",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=DEFAULT_RETRIES,
        help=f"Attempts per URL. Default: {DEFAULT_RETRIES}",
    )
    parser.add_argument(
        "--retry-wait",
        type=float,
        default=DEFAULT_RETRY_WAIT,
        help=f"Base retry wait in seconds. Default: {DEFAULT_RETRY_WAIT}",
    )
    parser.add_argument(
        "--progress",
        action="store_true",
        help="Print each row before it is fetched.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = resolve_path(args.input)
    output_path = resolve_path(args.output)

    rows, fieldnames = read_rows(input_path)
    if args.url_field not in fieldnames:
        raise ValueError(f"Input CSV must include a `{args.url_field}` column.")

    output_fieldnames = list(fieldnames)
    if args.output_field not in output_fieldnames:
        output_fieldnames.append(args.output_field)

    rows = add_image_ids(
        rows,
        url_field=args.url_field,
        output_field=args.output_field,
        timeout=args.timeout,
        retries=args.retries,
        retry_wait=args.retry_wait,
        progress=args.progress,
    )
    write_rows(output_path, rows, output_fieldnames)
    print(f"Wrote {len(rows)} rows to {output_path}")


if __name__ == "__main__":
    main()
