import re

import pandas as pd


def load_city_spatial_lookup(cities_csv_path, state):
    city_lookup = {}
    city_alias_lookup = {}

    if not state:
        return city_lookup, city_alias_lookup

    cities_df = pd.read_csv(cities_csv_path, encoding="utf-8", dtype=str).fillna("")
    state_prefix = f"{state}--"
    state_df = cities_df[cities_df["City"].str.startswith(state_prefix)].copy()

    for _, row in state_df.iterrows():
        full_name = _normalize_space(row.get("City", ""))
        if not full_name:
            continue

        city_lookup[full_name] = {
            "full_name": full_name,
            "bounding_box": _normalize_space(row.get("Bounding Box", "")),
            "geometry": _normalize_space(row.get("Geometry", "")),
            "geonames": _normalize_space(row.get("GeoNames", "")),
        }

        short_name = full_name.split("--")[-1].strip()
        aliases = {
            full_name,
            short_name,
            f"{state}--{short_name}",
        }
        for alias in aliases:
            alias_key = _normalize_space(alias)
            if alias_key:
                city_alias_lookup[alias_key] = full_name

    return city_lookup, city_alias_lookup


def match_city_spatial(normalized_values, city_lookup, city_alias_lookup):
    empty_match = {
        "full_name": "",
        "bounding_box": "",
        "geometry": "",
        "geonames": "",
    }
    if not city_lookup or not city_alias_lookup:
        return empty_match

    values = normalized_values if isinstance(normalized_values, list) else [normalized_values]
    for value in values:
        lookup_key = _resolve_city_lookup_key(value, city_lookup, city_alias_lookup)
        if lookup_key:
            return city_lookup.get(lookup_key, empty_match)

    return empty_match


def load_county_spatial_lookup(counties_csv_path, state):
    county_lookup = {}
    county_alias_lookup = {}

    if not state:
        return county_lookup, county_alias_lookup

    counties_df = pd.read_csv(counties_csv_path, encoding="utf-8", dtype=str).fillna("")
    state_prefix = f"{state}--"
    state_df = counties_df[counties_df["County"].str.startswith(state_prefix)].copy()

    for _, row in state_df.iterrows():
        full_name = _normalize_space(row.get("County", ""))
        if not full_name:
            continue

        county_lookup[full_name] = {
            "full_name": full_name,
            "bounding_box": _normalize_space(row.get("Bounding Box", "")),
            "geometry": _normalize_space(row.get("Geometry", "")),
            "geonames": _normalize_space(row.get("GeoNames", "")),
        }

        short_name = full_name.split("--")[-1].strip()
        base_name = short_name.replace(" County", "").strip()
        aliases = {
            full_name,
            short_name,
            base_name,
            f"{state}--{short_name}",
            f"{state}--{base_name}",
        }
        for alias in aliases:
            alias_key = _normalize_space(alias)
            if alias_key:
                county_alias_lookup[alias_key] = full_name

    return county_lookup, county_alias_lookup


def match_county_spatial(normalized_values, county_lookup, county_alias_lookup):
    empty_match = {
        "full_name": "",
        "bounding_box": "",
        "geometry": "",
        "geonames": "",
    }
    if not county_lookup or not county_alias_lookup:
        return empty_match

    values = normalized_values if isinstance(normalized_values, list) else [normalized_values]
    for value in values:
        lookup_key = _resolve_county_lookup_key(value, county_lookup, county_alias_lookup)
        if lookup_key:
            return county_lookup.get(lookup_key, empty_match)

    return empty_match


def load_state_spatial_lookup(states_csv_path):
    state_lookup = {}
    state_alias_lookup = {}

    states_df = pd.read_csv(states_csv_path, encoding="utf-8", dtype=str).fillna("")
    for _, row in states_df.iterrows():
        label = _normalize_space(row.get("Label", ""))
        if not label:
            continue

        state_lookup[label] = {
            "full_name": label,
            "bounding_box": _normalize_space(row.get("Bounding Box", "")),
            "geometry": _normalize_space(row.get("Geometry", "")),
            "geonames": _normalize_space(row.get("GeoNames ID", "")),
        }

        aliases = {
            label,
            _normalize_space(row.get("STUSPS", "")),
            f"United States--{label}",
        }
        for alias in aliases:
            alias_key = _normalize_space(alias)
            if alias_key:
                state_alias_lookup[alias_key] = label

    return state_lookup, state_alias_lookup


def match_state_spatial(normalized_values, state_lookup, state_alias_lookup):
    empty_match = {
        "full_name": "",
        "bounding_box": "",
        "geometry": "",
        "geonames": "",
    }
    if not state_lookup or not state_alias_lookup:
        return empty_match

    values = normalized_values if isinstance(normalized_values, list) else [normalized_values]
    for value in values:
        lookup_key = _resolve_state_lookup_key(value, state_lookup, state_alias_lookup)
        if lookup_key:
            return state_lookup.get(lookup_key, empty_match)

    return empty_match


def load_plss_bbox_lookup(plss_csv_path, state_abbr):
    plss_lookup = {}
    if not state_abbr:
        return plss_lookup

    plss_df = pd.read_csv(plss_csv_path, encoding="utf-8", dtype=str).fillna("")
    state_df = plss_df[plss_df["STATEABBR"].str.upper() == state_abbr.upper()].copy()

    for _, row in state_df.iterrows():
        key = (
            row.get("Township", "").zfill(3),
            _normalize_space(row.get("Township Direction", "")).upper(),
            row.get("Range", "").zfill(3),
            _normalize_space(row.get("Range Direction", "")).upper(),
        )
        if not all(key):
            continue

        bbox = _normalize_bbox_string(row.get("Bounding Box", ""))
        if not bbox:
            continue

        existing_bbox = plss_lookup.get(key, "")
        plss_lookup[key] = _merge_bboxes(existing_bbox, bbox) if existing_bbox else bbox

    return plss_lookup


def match_plss_bbox(normalized_values, plss_lookup):
    if not plss_lookup:
        return {"has_plss": False, "bounding_box": ""}

    plss_key = _extract_plss_key(normalized_values)
    if not plss_key:
        return {"has_plss": False, "bounding_box": ""}

    return {
        "has_plss": True,
        "bounding_box": plss_lookup.get(plss_key, ""),
    }


def _resolve_city_lookup_key(value, city_lookup, city_alias_lookup):
    spatial_key = _normalize_space(value)
    if not spatial_key:
        return ""

    if spatial_key in city_lookup:
        return spatial_key

    return city_alias_lookup.get(spatial_key, "")


def _resolve_county_lookup_key(value, county_lookup, county_alias_lookup):
    spatial_key = _normalize_space(value)
    if not spatial_key:
        return ""

    if spatial_key in county_lookup:
        return spatial_key

    return county_alias_lookup.get(spatial_key, "")


def _resolve_state_lookup_key(value, state_lookup, state_alias_lookup):
    spatial_key = _normalize_space(value)
    if not spatial_key:
        return ""

    if spatial_key in state_lookup:
        return spatial_key

    if spatial_key in state_alias_lookup:
        return state_alias_lookup[spatial_key]

    trimmed_key = spatial_key.split("--")[0]
    if trimmed_key in state_lookup:
        return trimmed_key

    return state_alias_lookup.get(trimmed_key, "")


def _extract_plss_key(normalized_values):
    values = normalized_values if isinstance(normalized_values, list) else [normalized_values]
    township_number = ""
    township_direction = ""
    range_number = ""
    range_direction = ""

    for value in values:
        clean_value = _normalize_space(value).lower()
        township_match = re.fullmatch(r"t(\d{1,3})([ns])", clean_value)
        if township_match:
            township_number = township_match.group(1).zfill(3)
            township_direction = township_match.group(2).upper()
            continue

        range_match = re.fullmatch(r"r(\d{1,3})([ew])", clean_value)
        if range_match:
            range_number = range_match.group(1).zfill(3)
            range_direction = range_match.group(2).upper()

    if not all([township_number, township_direction, range_number, range_direction]):
        return None

    return (
        township_number,
        township_direction,
        range_number,
        range_direction,
    )


def _normalize_bbox_string(value):
    clean_value = _normalize_space(value)
    if not clean_value:
        return ""

    parts = [_normalize_space(part) for part in clean_value.split(",")]
    if len(parts) != 4:
        return ""
    return ",".join(parts)


def _merge_bboxes(bbox_a, bbox_b):
    coords_a = _bbox_coordinates(bbox_a)
    coords_b = _bbox_coordinates(bbox_b)
    if not coords_a:
        return bbox_b
    if not coords_b:
        return bbox_a

    west = min(coords_a[0], coords_b[0])
    south = min(coords_a[1], coords_b[1])
    east = max(coords_a[2], coords_b[2])
    north = max(coords_a[3], coords_b[3])
    return f"{west},{south},{east},{north}"


def _bbox_coordinates(value):
    parts = _normalize_bbox_string(value).split(",")
    if len(parts) != 4:
        return None

    try:
        return [float(part) for part in parts]
    except ValueError:
        return None


def _normalize_space(value):
    return re.sub(r"\s+", " ", str(value or "")).strip()
