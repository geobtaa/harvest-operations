import csv
import glob
import os
import time


def first_non_empty(*values: str) -> str:
    """Return the first non-empty string from a list of candidate values."""
    for value in values:
        cleaned = str(value or "").strip()
        if cleaned:
            return cleaned
    return ""


def read_csv_rows(csv_path: str) -> list[dict]:
    """Load a CSV file into memory as row dictionaries when the file exists."""
    if not csv_path or not os.path.exists(csv_path):
        return []

    with open(csv_path, newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def required_config_path(config: dict, key: str, source_label: str = "Harvester") -> str:
    """Read a required path from harvester config without embedding path defaults in code."""
    value = str(config.get(key, "")).strip()
    if not value:
        raise ValueError(f"[{source_label}] Missing required config value: {key}")
    return value


def normalize_lookup_key(value: str) -> str:
    """Normalize row identifiers so workflow records and website defaults can be matched."""
    return str(value or "").strip().lower()


def lookup_keys_for_row(row: dict) -> list[str]:
    """Generate candidate lookup keys from the row's code, identifier, and id values."""
    keys = []
    for raw_value in (
        row.get("Code", ""),
        row.get("Identifier", ""),
        row.get("ID", ""),
    ):
        normalized = normalize_lookup_key(raw_value)
        if not normalized:
            continue
        keys.append(normalized)
        if normalized.startswith("harvest_"):
            keys.append(normalized.removeprefix("harvest_"))

    seen = set()
    ordered_keys = []
    for key in keys:
        if key not in seen:
            seen.add(key)
            ordered_keys.append(key)
    return ordered_keys


def load_metadata_lookup(metadata_path: str) -> dict[str, dict]:
    """Build a metadata lookup keyed by known row identifiers."""
    lookup = {}
    for row in read_csv_rows(metadata_path):
        for key in lookup_keys_for_row(row):
            lookup[key] = row
    return lookup


def match_metadata_defaults(source_record: dict, metadata_lookup: dict[str, dict]) -> dict:
    """Retrieve the metadata defaults row that corresponds to a workflow record."""
    for key in lookup_keys_for_row(source_record):
        matched_row = metadata_lookup.get(key)
        if matched_row:
            return matched_row.copy()
    return {}


def build_updated_harvest_record_rows(workflow_input_path: str, today: str):
    """Append workflow input rows as-is, updating only Last Harvested for this run."""
    import pandas as pd

    harvest_record_df = pd.DataFrame(read_csv_rows(workflow_input_path))
    if harvest_record_df.empty:
        return harvest_record_df

    harvest_record_df["Last Harvested"] = today
    return harvest_record_df


def resolve_dated_path(configured_path: str) -> str:
    """Resolve a dated path template, falling back to the latest matching file."""
    configured_path = str(configured_path or "").strip()
    if not configured_path:
        return ""

    dated_path = configured_path.replace("{date}", time.strftime("%Y-%m-%d"))
    if os.path.exists(dated_path) or "{date}" not in configured_path:
        return dated_path

    matches = sorted(glob.glob(configured_path.replace("{date}", "*")))
    if matches:
        return matches[-1]

    return dated_path


def normalize_prefixed_title(title: str, prefix: str) -> str:
    """Strip a known prefix from a title when present."""
    cleaned = str(title or "").strip()
    if cleaned.startswith(prefix):
        return cleaned[len(prefix):].strip()
    return cleaned
