#!/usr/bin/env python3
"""
Query an OAI-PMH ListSets endpoint and filter sets by keyword.

This is useful when the relevant harvestable sets can only be identified from
the endpoint's published set list.
"""

import argparse
import csv
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional

import requests
import yaml


OAI_NS = {"oai": "http://www.openarchives.org/OAI/2.0/"}
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

DEFAULT_BASE_URL = "https://digital.lib.uiowa.edu/oai/request"
DEFAULT_NAME = "oai"
DEFAULT_MATCH_TERMS = ("atlas", "plat book")


def normalize_space(value: str) -> str:
    return " ".join((value or "").split()).strip()


def slugify(value: str) -> str:
    cleaned = "".join(char if char.isalnum() or char in "._-" else "-" for char in value.strip())
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned.strip("-") or "unnamed"


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


def list_sets_params(resumption_token: Optional[str] = None) -> dict:
    if resumption_token:
        return {"verb": "ListSets", "resumptionToken": resumption_token}
    return {"verb": "ListSets"}


def extract_element_text(element: Optional[ET.Element]) -> str:
    if element is None:
        return ""
    return normalize_space(" ".join(text.strip() for text in element.itertext() if text and text.strip()))


def parse_list_sets_response(xml_text: str) -> tuple[list[dict], Optional[str], list[dict]]:
    sets: list[dict] = []
    errors: list[dict] = []
    token = None

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        return sets, token, [{"code": "xml_parse_error", "message": str(exc)}]

    for error in root.findall(".//oai:error", OAI_NS):
        errors.append(
            {
                "code": error.attrib.get("code", ""),
                "message": normalize_space(error.text or ""),
            }
        )

    for set_el in root.findall(".//oai:set", OAI_NS):
        set_spec = extract_element_text(set_el.find("oai:setSpec", OAI_NS))
        set_title = extract_element_text(set_el.find("oai:setName", OAI_NS))
        if not set_spec:
            continue
        sets.append(
            {
                "set_spec": set_spec,
                "set_title": set_title,
                "search_text": extract_element_text(set_el).lower(),
            }
        )

    token_el = root.find(".//oai:resumptionToken", OAI_NS)
    if token_el is not None:
        token = normalize_space(token_el.text or "") or None

    return sets, token, errors


def dedupe_sets(sets: list[dict]) -> list[dict]:
    deduped: list[dict] = []
    seen: set[str] = set()
    for item in sets:
        set_spec = item["set_spec"]
        if set_spec in seen:
            continue
        seen.add(set_spec)
        deduped.append(item)
    return deduped


def fetch_all_sets(
    session: requests.Session,
    base_url: str,
    delay: float,
    timeout: int,
) -> list[dict]:
    all_sets: list[dict] = []
    token = None

    while True:
        response = session.get(base_url, params=list_sets_params(token), timeout=timeout)
        response.raise_for_status()

        page_sets, next_token, errors = parse_list_sets_response(response.text)
        if errors:
            messages = "; ".join(
                f"{error['code'] or 'error'}: {error['message']}" for error in errors
            )
            raise RuntimeError(f"OAI ListSets returned errors: {messages}")

        all_sets.extend(page_sets)
        if not next_token:
            break

        token = next_token
        if delay > 0:
            time.sleep(delay)

    return dedupe_sets(all_sets)


def filter_sets_by_terms(sets: list[dict], match_terms: list[str]) -> list[dict]:
    normalized_terms = [normalize_space(term).lower() for term in match_terms if normalize_space(term)]
    matches: list[dict] = []

    for item in sets:
        matched_terms = [term for term in normalized_terms if term in item["search_text"]]
        if not matched_terms:
            continue

        matches.append(
            {
                "set_spec": item["set_spec"],
                "set_title": item["set_title"],
                "matched_terms": matched_terms,
            }
        )

    return matches


def load_existing_set_specs(csv_path: Path, set_column: str) -> set[str]:
    existing: set[str] = set()
    with csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            set_spec = normalize_space(str(row.get(set_column, "")))
            if set_spec:
                existing.add(set_spec)
    return existing


def default_output_csv(existing_sets_csv: Optional[Path], name: str) -> Path:
    if existing_sets_csv is not None:
        return existing_sets_csv.with_name(f"{existing_sets_csv.stem}-discovered.csv")
    return PROJECT_ROOT / "config" / f"{slugify(name)}-sets-discovered.csv"


def build_output_rows(
    matched_sets: list[dict],
    existing_set_specs: set[str],
    set_column: str,
    title_column: str,
    include_known: bool,
) -> list[dict]:
    rows: list[dict] = []

    for item in matched_sets:
        already_listed = item["set_spec"] in existing_set_specs
        if already_listed and not include_known:
            continue

        rows.append(
            {
                set_column: item["set_spec"],
                title_column: item["set_title"],
                "match_terms": "; ".join(item["matched_terms"]),
                "already_listed": "yes" if already_listed else "no",
            }
        )

    rows.sort(key=lambda row: (row[title_column].lower(), row[set_column].lower()))
    return rows


def write_rows(output_csv: Path, rows: list[dict], fieldnames: list[str]) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Discover OAI-PMH sets from ListSets and filter them by keyword."
    )
    parser.add_argument("--config", help="Job config YAML to load endpoint and set CSV values from.")
    parser.add_argument("--base-url", help="OAI-PMH base URL.")
    parser.add_argument("--name", help="Optional name used to derive the default output CSV.")
    parser.add_argument(
        "--existing-sets-csv",
        help="CSV of already known set specs. Defaults to sets_csv from the job config.",
    )
    parser.add_argument("--set-column", help="CSV column holding the set spec. Default: set")
    parser.add_argument("--title-column", help="CSV column holding the set title. Default: title")
    parser.add_argument(
        "--match",
        action="append",
        dest="match_terms",
        help="Case-insensitive substring to match against the ListSets response. Repeat as needed.",
    )
    parser.add_argument(
        "--include-known",
        action="store_true",
        help="Include sets already present in the existing sets CSV.",
    )
    parser.add_argument(
        "--output-csv",
        help="Write matching rows to this CSV. Defaults to <existing-sets-stem>-discovered.csv.",
    )
    parser.add_argument("--delay", type=float, default=0.5, help="Seconds to sleep between OAI pages.")
    parser.add_argument("--timeout", type=int, default=60, help="HTTP timeout in seconds.")
    parser.add_argument(
        "--no-write",
        action="store_true",
        help="Print matches without writing a CSV file.",
    )
    return parser


def apply_config_and_defaults(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> argparse.Namespace:
    job_cfg = {}
    config_path = None

    if args.config:
        config_path = resolve_path(args.config)
        if not config_path.exists():
            parser.error(f"Config file not found: {config_path}")
        job_cfg = load_job_config(config_path)

    args.base_url = args.base_url or job_cfg.get("oai_base_url") or DEFAULT_BASE_URL
    args.name = args.name or job_cfg.get("name") or DEFAULT_NAME
    args.existing_sets_csv = args.existing_sets_csv or job_cfg.get("sets_csv")
    args.set_column = args.set_column or job_cfg.get("sets_csv_set_column") or "set"
    args.title_column = args.title_column or job_cfg.get("sets_csv_title_column") or "title"
    args.match_terms = args.match_terms or list(DEFAULT_MATCH_TERMS)

    if args.existing_sets_csv:
        args.existing_sets_csv = str(resolve_path(args.existing_sets_csv, config_path))

    if args.output_csv:
        args.output_csv = str(resolve_path(args.output_csv, config_path))

    return args


def main() -> None:
    parser = build_parser()
    args = apply_config_and_defaults(parser.parse_args(), parser)

    existing_csv_path = Path(args.existing_sets_csv) if args.existing_sets_csv else None
    existing_set_specs = set()
    if existing_csv_path and existing_csv_path.exists():
        existing_set_specs = load_existing_set_specs(existing_csv_path, args.set_column)

    output_csv = (
        Path(args.output_csv)
        if args.output_csv
        else default_output_csv(existing_csv_path, args.name)
    )

    session = requests.Session()
    session.headers.update({"User-Agent": "harvester-api oai set discovery"})

    all_sets = fetch_all_sets(
        session=session,
        base_url=args.base_url,
        delay=args.delay,
        timeout=args.timeout,
    )
    matched_sets = filter_sets_by_terms(all_sets, args.match_terms)
    rows = build_output_rows(
        matched_sets=matched_sets,
        existing_set_specs=existing_set_specs,
        set_column=args.set_column,
        title_column=args.title_column,
        include_known=args.include_known,
    )

    print(f"Fetched {len(all_sets)} total set(s) from {args.base_url}")
    print(
        f"Matched {len(matched_sets)} set(s) using terms: "
        + ", ".join(f"'{term}'" for term in args.match_terms)
    )
    if existing_csv_path:
        print(f"Loaded {len(existing_set_specs)} existing set spec(s) from {existing_csv_path}")
    print(f"Returning {len(rows)} row(s) after excluding known sets: {not args.include_known}")

    for row in rows:
        print(
            f"- {row[args.set_column]} ({row[args.title_column]}) "
            f"[matched: {row['match_terms']}] [already_listed: {row['already_listed']}]"
        )

    if args.no_write:
        return

    fieldnames = [args.set_column, args.title_column, "match_terms", "already_listed"]
    write_rows(output_csv, rows, fieldnames)
    print(f"Wrote {len(rows)} row(s) to {output_csv}")


if __name__ == "__main__":
    main()
