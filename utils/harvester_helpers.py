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
