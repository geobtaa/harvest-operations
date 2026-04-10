import csv
import re
from difflib import SequenceMatcher
from functools import lru_cache
from pathlib import Path


DEFAULT_RESOURCE_TYPE_VALUES_CSV = (
    Path(__file__).resolve().parents[1] / "schemas" / "resource_type_values.csv"
)
GENERIC_UNMATCHED_TERMS = {
    "data",
    "dataset",
    "datasets",
    "image",
    "images",
    "imagery",
    "map",
    "maps",
    "service",
    "services",
}


@lru_cache(maxsize=None)
def load_resource_type_values(csv_path=None):
    resource_types = []
    seen = set()

    path = Path(csv_path) if csv_path else DEFAULT_RESOURCE_TYPE_VALUES_CSV
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.reader(handle)
        for row in reader:
            if not row:
                continue
            value = _clean_value(row[0])
            if not value or value in seen:
                continue
            seen.add(value)
            resource_types.append(value)

    return tuple(resource_types)


@lru_cache(maxsize=None)
def _resource_type_index(csv_path=None):
    values = load_resource_type_values(csv_path)
    normalized_lookup = {}
    singular_lookup = {}

    for value in values:
        normalized_key = _normalize_match_key(value)
        if not normalized_key:
            continue
        normalized_lookup.setdefault(normalized_key, value)
        singular_lookup.setdefault(_singularize(normalized_key), value)

    return values, normalized_lookup, singular_lookup


def match_resource_type(value, csv_path=None, min_score=0.86, keep_unmatched=True):
    clean_value = _clean_value(value)
    if not clean_value:
        return ""

    _, normalized_lookup, singular_lookup = _resource_type_index(csv_path)
    normalized_key = _normalize_match_key(clean_value)
    if not normalized_key:
        return clean_value if keep_unmatched else ""

    if normalized_key in normalized_lookup:
        return normalized_lookup[normalized_key]

    singular_key = _singularize(normalized_key)
    if singular_key in singular_lookup:
        return singular_lookup[singular_key]

    if normalized_key in GENERIC_UNMATCHED_TERMS or len(normalized_key) < 6:
        return clean_value if keep_unmatched else ""

    best_value = ""
    best_score = 0.0
    for candidate_key, candidate_value in normalized_lookup.items():
        score = SequenceMatcher(None, normalized_key, candidate_key).ratio()
        if score > best_score:
            best_score = score
            best_value = candidate_value

    if best_score >= min_score:
        return best_value

    return clean_value if keep_unmatched else ""


def match_resource_type_values(values, csv_path=None, min_score=0.86, keep_unmatched=True):
    matched_values, unmatched_values = split_resource_type_values(
        values,
        csv_path=csv_path,
        min_score=min_score,
    )
    if keep_unmatched:
        return matched_values + unmatched_values
    return matched_values


def split_resource_type_values(values, csv_path=None, min_score=0.86):
    if isinstance(values, str):
        raw_values = values.split("|")
    elif isinstance(values, (list, tuple, set)):
        raw_values = list(values)
    else:
        raw_values = [values]

    matched_values = []
    unmatched_values = []
    seen_matched = set()
    seen_unmatched = set()
    for value in raw_values:
        matched_value = match_resource_type(
            value,
            csv_path=csv_path,
            min_score=min_score,
            keep_unmatched=False,
        )
        clean_value = _clean_value(value)

        if matched_value:
            if matched_value in seen_matched:
                continue
            seen_matched.add(matched_value)
            matched_values.append(matched_value)
            continue

        if not clean_value or clean_value in seen_unmatched:
            continue
        seen_unmatched.add(clean_value)
        unmatched_values.append(clean_value)

    return matched_values, unmatched_values


def _clean_value(value):
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _normalize_match_key(value):
    clean_value = _clean_value(value).lower()
    clean_value = clean_value.replace("&", " and ")
    clean_value = re.sub(r"[^a-z0-9]+", " ", clean_value)
    return re.sub(r"\s+", " ", clean_value).strip()


def _singularize(value):
    if value.endswith("ies") and len(value) > 4:
        return value[:-3] + "y"
    if value.endswith("es") and len(value) > 4:
        return value[:-2]
    if value.endswith("s") and len(value) > 4 and not value.endswith(("as", "is", "ss", "us")):
        return value[:-1]
    return value
