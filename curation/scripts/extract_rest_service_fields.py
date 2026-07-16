"""Extract ArcGIS REST service fields into per-layer CSV data dictionaries."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


OUTPUT_COLUMNS = [
    "friendlier_id",
    "field_name",
    "field_type",
    "values",
    "definition",
    "definition_source",
    "parent_field_name",
    "position",
]

DEFAULT_INPUT_CSV = "restServiceFields.csv"
DEFAULT_OUTPUT_DIR = "30g-02/data_dictionaries"


def build_json_url(service_url: str) -> str:
    parts = urlsplit(service_url.strip())
    query_params = dict(parse_qsl(parts.query, keep_blank_values=True))
    query_params["f"] = "pjson"
    return urlunsplit(
        (parts.scheme, parts.netloc, parts.path, urlencode(query_params), parts.fragment)
    )


def fetch_service_metadata(service_url: str, timeout: int) -> dict[str, Any]:
    json_url = build_json_url(service_url)
    request = Request(json_url, headers={"User-Agent": "curation field extractor/1.0"})

    with urlopen(request, timeout=timeout) as response:
        payload = response.read().decode("utf-8")

    metadata = json.loads(payload)
    if not isinstance(metadata, dict):
        raise ValueError("Service metadata did not return a JSON object")

    if metadata.get("error"):
        raise ValueError(f"Service returned an error payload: {metadata['error']}")

    return metadata


def output_filename_to_path(output_dir: str, output_filename: str) -> str:
    filename = output_filename.strip()
    if not filename.lower().endswith(".csv"):
        filename = f"{filename}.csv"
    return os.path.join(output_dir, filename)


def extract_rest_service_fields(input_csv: str, output_dir: str, timeout: int = 60) -> None:
    os.makedirs(output_dir, exist_ok=True)

    with open(input_csv, newline="", encoding="utf-8-sig") as infile:
        reader = csv.DictReader(infile)
        if reader.fieldnames is None:
            raise ValueError(f"{input_csv} is missing a header row")

        required_columns = {"friendlier_id", "output_filename", "service_url"}
        missing_columns = required_columns - set(reader.fieldnames)
        if missing_columns:
            missing_text = ", ".join(sorted(missing_columns))
            raise ValueError(f"{input_csv} is missing required columns: {missing_text}")

        for row in reader:
            friendlier_id = (row.get("friendlier_id") or "").strip()
            output_filename = (row.get("output_filename") or "").strip()
            service_url = (row.get("service_url") or "").strip()

            if not output_filename:
                logging.warning("Skipping row with blank output_filename: %s", row)
                continue

            if not service_url:
                logging.warning("Skipping %s because service_url is blank", output_filename)
                continue

            output_path = output_filename_to_path(output_dir, output_filename)

            try:
                metadata = fetch_service_metadata(service_url, timeout=timeout)
            except Exception as exc:
                logging.warning("Could not fetch %s: %s", service_url, exc)
                continue

            fields = metadata.get("fields") or []
            if not isinstance(fields, list):
                logging.warning("No usable fields list found for %s", service_url)
                fields = []

            with open(output_path, "w", newline="", encoding="utf-8") as outfile:
                writer = csv.DictWriter(outfile, fieldnames=OUTPUT_COLUMNS)
                writer.writeheader()

                for field in fields:
                    if not isinstance(field, dict):
                        continue

                    writer.writerow(
                        {
                            "friendlier_id": friendlier_id,
                            "field_name": field.get("name", ""),
                            "field_type": field.get("type", ""),
                            "values": "",
                            "definition": field.get("alias", ""),
                            "definition_source": "",
                            "parent_field_name": "",
                            "position": "",
                        }
                    )

            logging.info("Wrote %s", output_path)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract field data dictionaries from ArcGIS REST service URLs."
    )
    parser.add_argument(
        "input_csv",
        nargs="?",
        default=DEFAULT_INPUT_CSV,
        help=f"CSV file containing friendlier_id, output_filename, and service_url (default: {DEFAULT_INPUT_CSV})",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help=f"Directory where per-service CSV files will be written (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="HTTP timeout in seconds for each service request",
    )
    args = parser.parse_args()

    extract_rest_service_fields(args.input_csv, args.output_dir, timeout=args.timeout)


if __name__ == "__main__":
    main()
