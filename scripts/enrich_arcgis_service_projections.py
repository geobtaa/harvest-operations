#!/usr/bin/env python3
"""
Add a `projection` column to a distributions CSV by querying ArcGIS REST service metadata.

The script inspects each `distribution_url`. For ArcGIS REST service URLs such as
`.../MapServer`, `.../FeatureServer`, `.../ImageServer`, and layer endpoints below them,
it requests the service metadata as `f=pjson` and extracts the first available spatial
reference.
"""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

DEFAULT_TIMEOUT = 20
PROJECTION_FIELD = "projection"
DISTRIBUTION_URL_FIELD = "distribution_url"
ARCGIS_SERVICE_PATTERN = re.compile(
    r"/(?:MapServer|FeatureServer|ImageServer)(?:/\d+)?/?$",
    re.IGNORECASE,
)
WKT_NAME_PATTERN = re.compile(r'[A-Z_]+\["([^"]+)"')


def resolve_path(path_value: str | Path) -> Path:
    candidate = Path(path_value).expanduser()
    if candidate.is_absolute():
        return candidate
    return (PROJECT_ROOT / candidate).resolve()


def default_output_path(input_csv: Path) -> Path:
    return input_csv.with_name(f"{input_csv.stem}_projection{input_csv.suffix}")


def read_rows(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        rows = [{str(key): str(value or "") for key, value in row.items() if key} for row in reader]
    return fieldnames, rows


def write_rows(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def get_output_fieldnames(fieldnames: list[str]) -> list[str]:
    if PROJECTION_FIELD in fieldnames:
        return fieldnames
    return [*fieldnames, PROJECTION_FIELD]


def is_arcgis_service_url(distribution_url: str) -> bool:
    cleaned_url = strip_arcmap_query(distribution_url)
    return bool(ARCGIS_SERVICE_PATTERN.search(cleaned_url))


def strip_arcmap_query(distribution_url: str) -> str:
    parts = urlsplit(str(distribution_url or "").strip())
    if not parts.scheme or not parts.netloc:
        return str(distribution_url or "").strip()

    query_pairs = [
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if not (
            key.casefold() == "f" and value.casefold() == "lyr"
            or key.casefold() == "v" and value == "9.3"
        )
    ]
    query = urlencode(query_pairs)
    return urlunsplit((parts.scheme, parts.netloc, parts.path.rstrip("/"), query, ""))


def build_metadata_url(distribution_url: str) -> str:
    cleaned_url = strip_arcmap_query(distribution_url)
    parts = urlsplit(cleaned_url)
    if not parts.scheme or not parts.netloc:
        return cleaned_url

    query_pairs = [
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if key.casefold() != "f"
    ]
    query_pairs.append(("f", "pjson"))
    query = urlencode(query_pairs)
    return urlunsplit((parts.scheme, parts.netloc, parts.path.rstrip("/"), query, ""))


def extract_spatial_reference(payload: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        payload.get("spatialReference"),
        payload.get("sourceSpatialReference"),
        payload.get("extent", {}).get("spatialReference"),
        payload.get("fullExtent", {}).get("spatialReference"),
        payload.get("initialExtent", {}).get("spatialReference"),
        payload.get("tileInfo", {}).get("spatialReference"),
    ]

    for candidate in candidates:
        if isinstance(candidate, dict) and candidate:
            return candidate
    return {}


def format_projection(spatial_reference: dict[str, Any]) -> str:
    if not spatial_reference:
        return ""

    latest_wkid = spatial_reference.get("latestWkid")
    wkid = spatial_reference.get("wkid")
    if latest_wkid and wkid and str(latest_wkid) != str(wkid):
        return f"EPSG:{latest_wkid} (wkid {wkid})"
    if latest_wkid:
        return f"EPSG:{latest_wkid}"
    if wkid:
        return f"EPSG:{wkid}"

    wkt = spatial_reference.get("wkt") or spatial_reference.get("latestWkt") or ""
    if isinstance(wkt, str) and wkt.strip():
        match = WKT_NAME_PATTERN.search(wkt)
        return match.group(1) if match else wkt.strip()

    return ""


def fetch_projection(distribution_url: str, session: requests.Session, timeout: int) -> str:
    if not is_arcgis_service_url(distribution_url):
        return ""

    metadata_url = build_metadata_url(distribution_url)
    response = session.get(metadata_url, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    spatial_reference = extract_spatial_reference(payload)
    return format_projection(spatial_reference)


def enrich_rows_with_projections(
    rows: list[dict[str, str]],
    *,
    session: requests.Session,
    timeout: int = DEFAULT_TIMEOUT,
    only_missing: bool = False,
) -> tuple[list[dict[str, str]], dict[str, int]]:
    cache: dict[str, str] = {}
    enriched_rows: list[dict[str, str]] = []
    counts = {
        "rows_processed": 0,
        "service_rows_seen": 0,
        "rows_updated": 0,
        "errors": 0,
        "cache_hits": 0,
    }

    for row in rows:
        counts["rows_processed"] += 1
        updated_row = dict(row)
        distribution_url = updated_row.get(DISTRIBUTION_URL_FIELD, "").strip()
        existing_projection = updated_row.get(PROJECTION_FIELD, "").strip()

        if not is_arcgis_service_url(distribution_url):
            enriched_rows.append(updated_row)
            continue

        counts["service_rows_seen"] += 1
        if only_missing and existing_projection:
            enriched_rows.append(updated_row)
            continue

        if distribution_url in cache:
            counts["cache_hits"] += 1
            projection = cache[distribution_url]
        else:
            try:
                projection = fetch_projection(distribution_url, session, timeout)
            except Exception as exc:
                counts["errors"] += 1
                print(f"[projection] Failed: {distribution_url} ({exc})")
                projection = existing_projection
            cache[distribution_url] = projection

        if projection != existing_projection:
            updated_row[PROJECTION_FIELD] = projection
            counts["rows_updated"] += 1

        enriched_rows.append(updated_row)

    return enriched_rows, counts


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-csv",
        required=True,
        help="Distributions CSV to enrich.",
    )
    parser.add_argument(
        "--output-csv",
        help="Where to write the enriched CSV. Defaults to <input>_projection.csv.",
    )
    parser.add_argument(
        "--in-place",
        action="store_true",
        help="Overwrite the input CSV instead of writing a separate output file.",
    )
    parser.add_argument(
        "--only-missing",
        action="store_true",
        help="Only request projections for rows where `projection` is currently blank.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help=f"HTTP timeout in seconds. Default: {DEFAULT_TIMEOUT}",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    input_csv = resolve_path(args.input_csv)
    if not input_csv.exists():
        parser.error(f"Input CSV not found: {input_csv}")

    if args.in_place and args.output_csv:
        parser.error("Use either --in-place or --output-csv, not both.")

    output_csv = (
        input_csv
        if args.in_place
        else resolve_path(args.output_csv) if args.output_csv else default_output_path(input_csv)
    )

    fieldnames, rows = read_rows(input_csv)
    if DISTRIBUTION_URL_FIELD not in fieldnames:
        parser.error(
            f"Input CSV must include a `{DISTRIBUTION_URL_FIELD}` column: {input_csv}"
        )

    session = requests.Session()
    try:
        enriched_rows, counts = enrich_rows_with_projections(
            rows,
            session=session,
            timeout=args.timeout,
            only_missing=args.only_missing,
        )
    finally:
        session.close()

    output_fieldnames = get_output_fieldnames(fieldnames)
    write_rows(output_csv, output_fieldnames, enriched_rows)

    print(f"Input CSV: {input_csv}")
    print(f"Rows processed: {counts['rows_processed']}")
    print(f"ArcGIS service rows: {counts['service_rows_seen']}")
    print(f"Rows updated: {counts['rows_updated']}")
    print(f"Cache hits: {counts['cache_hits']}")
    print(f"Errors: {counts['errors']}")
    print(f"Output CSV: {output_csv}")


if __name__ == "__main__":
    main()
