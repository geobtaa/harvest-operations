#!/usr/bin/env python3
"""
Download raw OAI-PMH XML responses for one or more sets.

This is intended as a separate acquisition step so parser development can work
from local XML snapshots instead of repeatedly hitting the source endpoint.
"""

import argparse
import csv
import json
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import requests
import yaml


OAI_NS = {"oai": "http://www.openarchives.org/OAI/2.0/"}
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

# Optional hardcoded defaults for one-off runs. CLI arguments still override these.
DEFAULT_BASE_URL = "https://digital.lib.uiowa.edu/oai/request"
DEFAULT_SETS_CSV = PROJECT_ROOT / "config" / "iowa-sets.csv"
DEFAULT_NAME = "iowa-library"


def slugify(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip())
    value = re.sub(r"-{2,}", "-", value)
    return value.strip("-") or "unnamed"


def default_output_dir(base_url: str, name: Optional[str]) -> Path:
    folder_name = slugify(name) if name else slugify(urlparse(base_url).netloc)
    return PROJECT_ROOT / "inputs" / "oai-downloads" / folder_name


def resolve_path(path_value: str, config_path: Optional[Path] = None) -> Path:
    candidate = Path(path_value).expanduser()
    if candidate.is_absolute():
        return candidate

    project_candidate = (PROJECT_ROOT / candidate).resolve()
    if project_candidate.exists():
        return project_candidate

    if config_path is not None:
        config_candidate = (config_path.parent / candidate).resolve()
        if config_candidate.exists():
            return config_candidate

    return project_candidate


def load_job_config(config_path: Path) -> dict:
    with config_path.open(encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def load_sets_from_csv(csv_path: Path, set_column: str, title_column: str) -> list[dict]:
    sets: list[dict] = []
    with csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            set_spec = str(row.get(set_column, "")).strip()
            set_title = str(row.get(title_column, "")).strip()
            if not set_spec:
                continue
            sets.append({"set_spec": set_spec, "set_title": set_title})
    return sets


def load_sets(args: argparse.Namespace) -> list[dict]:
    sets: list[dict] = []

    if args.sets_csv:
        sets.extend(
            load_sets_from_csv(
                csv_path=Path(args.sets_csv),
                set_column=args.set_column,
                title_column=args.title_column,
            )
        )

    if args.set:
        for set_spec in args.set:
            clean_set = set_spec.strip()
            if clean_set:
                sets.append({"set_spec": clean_set, "set_title": ""})

    deduped: list[dict] = []
    seen: set[str] = set()
    for item in sets:
        set_spec = item["set_spec"]
        if set_spec in seen:
            continue
        seen.add(set_spec)
        deduped.append(item)

    if not deduped:
        raise ValueError("Provide at least one set via --set or --sets-csv.")

    return deduped


def oai_params(
    metadata_prefix: str,
    set_spec: Optional[str] = None,
    resumption_token: Optional[str] = None,
) -> dict:
    if resumption_token:
        return {
            "verb": "ListRecords",
            "resumptionToken": resumption_token,
        }

    params = {
        "verb": "ListRecords",
        "metadataPrefix": metadata_prefix,
    }
    if set_spec:
        params["set"] = set_spec
    return params


def parse_oai_response(xml_text: str) -> tuple[Optional[str], list[dict]]:
    token = None
    errors: list[dict] = []

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        errors.append({"code": "xml_parse_error", "message": str(exc)})
        return token, errors

    for error in root.findall(".//oai:error", OAI_NS):
        errors.append(
            {
                "code": error.attrib.get("code", ""),
                "message": (error.text or "").strip(),
            }
        )

    token_el = root.find(".//oai:resumptionToken", OAI_NS)
    if token_el is not None:
        token_text = (token_el.text or "").strip()
        token = token_text or None

    return token, errors


def write_text(path: Path, contents: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents, encoding="utf-8")


def download_set(
    session: requests.Session,
    base_url: str,
    metadata_prefix: str,
    set_spec: str,
    set_title: str,
    output_dir: Path,
    delay: float,
    timeout: int,
) -> dict:
    set_dir = output_dir / slugify(set_spec)
    set_dir.mkdir(parents=True, exist_ok=True)

    page = 1
    token = None
    downloaded_files: list[str] = []
    errors_seen: list[dict] = []

    while True:
        params = oai_params(
            metadata_prefix=metadata_prefix,
            set_spec=set_spec,
            resumption_token=token,
        )
        response = session.get(base_url, params=params, timeout=timeout)
        response.raise_for_status()

        xml_path = set_dir / f"{page:04d}.xml"
        write_text(xml_path, response.text)
        downloaded_files.append(str(xml_path))

        next_token, page_errors = parse_oai_response(response.text)
        if page_errors:
            errors_seen.extend(page_errors)
            break

        if not next_token:
            break

        token = next_token
        page += 1
        if delay > 0:
            time.sleep(delay)

    manifest = {
        "set_spec": set_spec,
        "set_title": set_title,
        "metadata_prefix": metadata_prefix,
        "base_url": base_url,
        "downloaded_files": downloaded_files,
        "error_count": len(errors_seen),
        "errors": errors_seen,
    }
    write_text(set_dir / "manifest.json", json.dumps(manifest, indent=2))
    return manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Download raw XML from an OAI-PMH endpoint for offline parser development."
    )
    parser.add_argument("--config", help="Job config YAML to load downloader values from.")
    parser.add_argument("--base-url", help="OAI-PMH base URL.")
    parser.add_argument("--metadata-prefix", help="OAI metadataPrefix value.")
    parser.add_argument("--name", help="Optional collection name used for the output folder.")
    parser.add_argument(
        "--set",
        action="append",
        help="Set spec to harvest. Repeat this flag to download multiple sets.",
    )
    parser.add_argument("--sets-csv", help="CSV file containing set definitions.")
    parser.add_argument("--set-column", help="CSV column holding the set spec.")
    parser.add_argument("--title-column", help="CSV column holding the set title.")
    parser.add_argument(
        "--output-dir",
        help="Directory for downloaded XML. Defaults to inputs/oai-downloads/<name-or-host>/",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Seconds to sleep between paged OAI requests for the same set.",
    )
    parser.add_argument("--timeout", type=int, default=60, help="HTTP timeout in seconds.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the planned sets and output folder without making HTTP requests.",
    )
    return parser


def apply_config_and_defaults(args: argparse.Namespace, parser: argparse.ArgumentParser) -> argparse.Namespace:
    job_cfg = {}
    config_path = None

    if args.config:
        config_path = resolve_path(args.config)
        if not config_path.exists():
            parser.error(f"Config file not found: {config_path}")
        job_cfg = load_job_config(config_path)

    args.base_url = args.base_url or job_cfg.get("oai_base_url") or DEFAULT_BASE_URL
    args.metadata_prefix = (
        args.metadata_prefix
        or job_cfg.get("metadata_prefix")
        or job_cfg.get("feed_type")
        or "oai_qdc"
    )
    args.name = args.name or job_cfg.get("name") or DEFAULT_NAME
    args.sets_csv = args.sets_csv or job_cfg.get("sets_csv") or str(DEFAULT_SETS_CSV)
    args.set_column = args.set_column or job_cfg.get("sets_csv_set_column") or "set"
    args.title_column = args.title_column or job_cfg.get("sets_csv_title_column") or "title"

    if not args.base_url:
        parser.error("Provide --base-url or set DEFAULT_BASE_URL in scripts/oai_download.py.")

    if not args.set and not args.sets_csv:
        parser.error("Provide --sets-csv/--set or set DEFAULT_SETS_CSV in scripts/oai_download.py.")

    if args.sets_csv:
        args.sets_csv = str(resolve_path(args.sets_csv, config_path))

    return args


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args = apply_config_and_defaults(args, parser)

    sets = load_sets(args)
    output_dir = Path(args.output_dir).expanduser() if args.output_dir else default_output_dir(args.base_url, args.name)

    if args.dry_run:
        print(f"Base URL: {args.base_url}")
        print(f"metadataPrefix: {args.metadata_prefix}")
        print(f"Output directory: {output_dir}")
        print(f"Sets to download: {len(sets)}")
        for item in sets:
            label = f" ({item['set_title']})" if item["set_title"] else ""
            print(f"- {item['set_spec']}{label}")
        return

    output_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update({"User-Agent": "harvester-api oai downloader"})

    run_manifest = {
        "base_url": args.base_url,
        "metadata_prefix": args.metadata_prefix,
        "output_dir": str(output_dir),
        "downloaded_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "sets": [],
    }

    for item in sets:
        set_spec = item["set_spec"]
        set_title = item["set_title"]
        print(f"Downloading set {set_spec}...")
        manifest = download_set(
            session=session,
            base_url=args.base_url,
            metadata_prefix=args.metadata_prefix,
            set_spec=set_spec,
            set_title=set_title,
            output_dir=output_dir,
            delay=args.delay,
            timeout=args.timeout,
        )
        run_manifest["sets"].append(manifest)
        print(f"Saved {len(manifest['downloaded_files'])} XML file(s) for {set_spec}")
        if manifest["errors"]:
            print(f"Stopped on OAI error for {set_spec}: {manifest['errors']}")

    write_text(output_dir / "manifest.json", json.dumps(run_manifest, indent=2))
    print(f"Done. Files written to {output_dir}")


if __name__ == "__main__":
    main()
