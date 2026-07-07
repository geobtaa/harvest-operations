#!/usr/bin/env python3
"""
Query ArcGIS REST metadata for distribution URLs in psu.csv.

The output CSV contains the source friendlier_id and URL plus the ArcGIS name,
extent formatted as W,S,E,N, and spatial reference.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

DEFAULT_INPUT_CSV = "psu.csv"
DEFAULT_OUTPUT_CSV = "psu_arcgis_metadata.csv"
DEFAULT_TIMEOUT = 20

DISTRIBUTION_URL_FIELD = "distribution_url"
FRIENDLIER_ID_FIELD = "friendlier_id"
OUTPUT_FIELDNAMES = [
    "friendlier_id",
    "distribution_url",
    "name",
    "extent",
    "spatial_reference",
]

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


def read_source_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        if DISTRIBUTION_URL_FIELD not in fieldnames:
            raise ValueError(f"Input CSV must include a `{DISTRIBUTION_URL_FIELD}` column.")
        return [
            {
                FRIENDLIER_ID_FIELD: str(row.get(FRIENDLIER_ID_FIELD) or "").strip(),
                DISTRIBUTION_URL_FIELD: str(row.get(DISTRIBUTION_URL_FIELD) or "").strip(),
            }
            for row in reader
            if str(row.get(DISTRIBUTION_URL_FIELD) or "").strip()
        ]


def write_rows(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


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


def is_arcgis_service_url(distribution_url: str) -> bool:
    return bool(ARCGIS_SERVICE_PATTERN.search(strip_arcmap_query(distribution_url)))


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


def extract_name(payload: dict[str, Any]) -> str:
    document_info = payload.get("documentInfo")
    document_title = ""
    if isinstance(document_info, dict):
        document_title = str(document_info.get("Title") or "").strip()

    for value in [
        payload.get("name"),
        payload.get("mapName"),
        payload.get("serviceDescription"),
        document_title,
    ]:
        if isinstance(value, str) and value.strip():
            return value.strip()

    return ""


def extract_extent(payload: dict[str, Any]) -> dict[str, Any]:
    for key in ["extent", "fullExtent", "initialExtent"]:
        candidate = payload.get(key)
        if isinstance(candidate, dict) and has_extent_coordinates(candidate):
            return candidate
    return {}


def has_extent_coordinates(extent: dict[str, Any]) -> bool:
    return all(key in extent for key in ["xmin", "ymin", "xmax", "ymax"])


def format_extent(extent: dict[str, Any]) -> str:
    if not extent:
        return ""
    return ",".join(
        format_coordinate(extent[key]) for key in ["xmin", "ymin", "xmax", "ymax"]
    )


def format_coordinate(value: Any) -> str:
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"{value:.12g}"
    text = str(value).strip()
    try:
        number = float(text)
    except ValueError:
        return text
    return f"{number:.12g}"


def extract_spatial_reference(payload: dict[str, Any]) -> dict[str, Any]:
    extent = extract_extent(payload)
    candidates = [
        payload.get("spatialReference"),
        payload.get("sourceSpatialReference"),
        extent.get("spatialReference"),
        payload.get("fullExtent", {}).get("spatialReference")
        if isinstance(payload.get("fullExtent"), dict)
        else None,
        payload.get("initialExtent", {}).get("spatialReference")
        if isinstance(payload.get("initialExtent"), dict)
        else None,
        payload.get("tileInfo", {}).get("spatialReference")
        if isinstance(payload.get("tileInfo"), dict)
        else None,
    ]

    for candidate in candidates:
        if isinstance(candidate, dict) and candidate:
            return candidate
    return {}


def format_spatial_reference(spatial_reference: dict[str, Any]) -> str:
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

    return json.dumps(spatial_reference, sort_keys=True)


def fetch_metadata_row(
    distribution_url: str,
    *,
    session: requests.Session,
    timeout: int,
) -> dict[str, str]:
    output_row = {
        "friendlier_id": "",
        "distribution_url": distribution_url,
        "name": "",
        "extent": "",
        "spatial_reference": "",
    }

    if not is_arcgis_service_url(distribution_url):
        return output_row

    response = session.get(build_metadata_url(distribution_url), timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        return output_row

    extent = extract_extent(payload)
    spatial_reference = extract_spatial_reference(payload)
    output_row.update(
        {
            "name": extract_name(payload),
            "extent": format_extent(extent),
            "spatial_reference": format_spatial_reference(spatial_reference),
        }
    )
    return output_row


def query_source_rows(
    source_rows: list[dict[str, str]],
    *,
    session: requests.Session,
    timeout: int = DEFAULT_TIMEOUT,
) -> tuple[list[dict[str, str]], dict[str, int]]:
    rows: list[dict[str, str]] = []
    cache: dict[str, dict[str, str]] = {}
    counts = {
        "urls_processed": 0,
        "arcgis_urls_seen": 0,
        "errors": 0,
        "cache_hits": 0,
    }

    for source_row in source_rows:
        friendlier_id = source_row.get(FRIENDLIER_ID_FIELD, "").strip()
        distribution_url = source_row.get(DISTRIBUTION_URL_FIELD, "").strip()
        counts["urls_processed"] += 1
        if is_arcgis_service_url(distribution_url):
            counts["arcgis_urls_seen"] += 1

        metadata_url = build_metadata_url(distribution_url)
        if metadata_url in cache:
            counts["cache_hits"] += 1
            row = dict(cache[metadata_url])
            row[FRIENDLIER_ID_FIELD] = friendlier_id
            rows.append(row)
            continue

        try:
            row = fetch_metadata_row(
                distribution_url,
                session=session,
                timeout=timeout,
            )
            row[FRIENDLIER_ID_FIELD] = friendlier_id
        except Exception as exc:
            counts["errors"] += 1
            print(f"[metadata] Failed: {distribution_url} ({exc})")
            row = {
                "friendlier_id": friendlier_id,
                "distribution_url": distribution_url,
                "name": "",
                "extent": "",
                "spatial_reference": "",
            }

        cache[metadata_url] = dict(row)
        rows.append(row)

    return rows, counts


def read_distribution_urls(path: Path) -> list[str]:
    return [row[DISTRIBUTION_URL_FIELD] for row in read_source_rows(path)]


def query_distribution_urls(
    distribution_urls: list[str],
    *,
    session: requests.Session,
    timeout: int = DEFAULT_TIMEOUT,
) -> tuple[list[dict[str, str]], dict[str, int]]:
    source_rows = [
        {
            FRIENDLIER_ID_FIELD: "",
            DISTRIBUTION_URL_FIELD: distribution_url,
        }
        for distribution_url in distribution_urls
    ]
    return query_source_rows(source_rows, session=session, timeout=timeout)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-csv",
        default=DEFAULT_INPUT_CSV,
        help=f"Input CSV with a `{DISTRIBUTION_URL_FIELD}` column. Default: {DEFAULT_INPUT_CSV}",
    )
    parser.add_argument(
        "--output-csv",
        default=DEFAULT_OUTPUT_CSV,
        help=f"Output CSV path. Default: {DEFAULT_OUTPUT_CSV}",
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
    output_csv = resolve_path(args.output_csv)

    if not input_csv.exists():
        parser.error(f"Input CSV not found: {input_csv}")

    source_rows = read_source_rows(input_csv)
    session = requests.Session()
    try:
        rows, counts = query_source_rows(
            source_rows,
            session=session,
            timeout=args.timeout,
        )
    finally:
        session.close()

    write_rows(output_csv, rows)

    print(f"Input CSV: {input_csv}")
    print(f"URLs processed: {counts['urls_processed']}")
    print(f"ArcGIS URLs: {counts['arcgis_urls_seen']}")
    print(f"Cache hits: {counts['cache_hits']}")
    print(f"Errors: {counts['errors']}")
    print(f"Output CSV: {output_csv}")


if __name__ == "__main__":
    main()
