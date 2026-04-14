#!/usr/bin/env python3

import argparse
import csv
from collections import Counter
from pathlib import Path


REFERENCE_CONFIG = {
    "counties": {
        "path": "reference_data/spatial_counties.csv",
        "name_column": "County",
        "alt_name_column": None,
        "geonames_column": "GeoNames",
    },
    "cities": {
        "path": "reference_data/spatial_cities.csv",
        "name_column": "City",
        "alt_name_column": None,
        "geonames_column": "GeoNames",
    },
    "states": {
        "path": "reference_data/spatial_us_states.csv",
        "name_column": "Label",
        "alt_name_column": None,
        "geonames_column": "GeoNames ID",
    },
    "nations": {
        "path": "reference_data/spatial_nations.csv",
        "name_column": "Label",
        "alt_name_column": "altLabel",
        "geonames_column": "GeoNames ID",
    },
}

OUTPUT_COLUMNS = ["Bounding Box", "Geometry", "GeoNames"]


def normalize_name(value):
    if value is None:
        return ""
    return str(value).strip().casefold()


def lookup_spatial_values(place_names, lookup_map, unmatched_names):
    if not place_names:
        return ""

    found_values = []
    for raw_name in str(place_names).split("|"):
        clean_name = normalize_name(raw_name)
        if not clean_name:
            continue

        value = lookup_map.get(clean_name)
        if value:
            found_values.append(value)
        else:
            unmatched_names[raw_name.strip()] += 1

    return "|".join(found_values)


def combine_bounding_boxes(bboxes):
    if not bboxes:
        return ""

    min_lon = float("inf")
    min_lat = float("inf")
    max_lon = float("-inf")
    max_lat = float("-inf")

    for bbox in str(bboxes).split("|"):
        if not bbox:
            continue

        try:
            west, south, east, north = [float(value) for value in bbox.split(",")]
        except (TypeError, ValueError):
            continue

        min_lon = min(min_lon, west)
        min_lat = min(min_lat, south)
        max_lon = max(max_lon, east)
        max_lat = max(max_lat, north)

    if min_lon == float("inf"):
        return ""

    return f"{min_lon},{min_lat},{max_lon},{max_lat}"


def strip_outer_parentheses(value):
    text = str(value).strip()
    if len(text) < 2 or not text.startswith("(") or not text.endswith(")"):
        return None

    depth = 0
    for index, char in enumerate(text):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth < 0:
                return None
            if depth == 0 and index != len(text) - 1:
                return None

    if depth != 0:
        return None

    return text[1:-1].strip()


def split_top_level_wkt_parts(value):
    parts = []
    depth = 0
    start = 0
    text = str(value).strip()

    for index, char in enumerate(text):
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth < 0:
                return None
        elif char == "," and depth == 0:
            part = text[start:index].strip()
            if part:
                parts.append(part)
            start = index + 1

    if depth != 0:
        return None

    tail = text[start:].strip()
    if tail:
        parts.append(tail)

    return parts


def extract_polygon_parts(geometry):
    text = str(geometry).strip()
    if not text:
        return []

    upper_text = text.upper()
    if upper_text.startswith("MULTIPOLYGON"):
        payload = text[len("MULTIPOLYGON") :].strip()
        inner_payload = strip_outer_parentheses(payload)
        if inner_payload is None:
            return None
        return split_top_level_wkt_parts(inner_payload)

    if upper_text.startswith("POLYGON"):
        payload = text[len("POLYGON") :].strip()
        if strip_outer_parentheses(payload) is None:
            return None
        return [payload]

    return None


def combine_geometries(geometries):
    if not geometries:
        return ""

    geometry_values = [value.strip() for value in str(geometries).split("|") if value.strip()]
    if not geometry_values:
        return ""

    if len(geometry_values) == 1:
        return geometry_values[0]

    polygon_parts = []
    for geometry in geometry_values:
        extracted_parts = extract_polygon_parts(geometry)
        if extracted_parts is None:
            return "|".join(geometry_values)
        polygon_parts.extend(extracted_parts)

    if not polygon_parts:
        return ""

    return f"MULTIPOLYGON ({', '.join(polygon_parts)})"


def prefer_derived_value(derived_value, existing_value):
    return derived_value if derived_value else (existing_value or "")


def load_reference_maps(levels, repo_root):
    reference_paths = []
    bbox_map = {}
    geometry_map = {}
    geonames_map = {}

    for level in levels:
        config = REFERENCE_CONFIG[level]
        reference_path = repo_root / config["path"]
        reference_paths.append(reference_path)

        with reference_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                names = [row.get(config["name_column"], "")]
                alt_name_column = config["alt_name_column"]
                if alt_name_column:
                    names.append(row.get(alt_name_column, ""))

                bbox = row.get("Bounding Box", "")
                geometry = row.get("Geometry", "")
                geonames = row.get(config["geonames_column"], "")

                for name in names:
                    clean_name = normalize_name(name)
                    if not clean_name:
                        continue

                    bbox_map.setdefault(clean_name, bbox)
                    geometry_map.setdefault(clean_name, geometry)
                    geonames_map.setdefault(clean_name, geonames)

    return reference_paths, bbox_map, geometry_map, geonames_map


def resolve_spatial_column(fieldnames, requested_name):
    if requested_name in fieldnames:
        return requested_name

    normalized_requested = normalize_name(requested_name)
    for fieldname in fieldnames:
        if normalize_name(fieldname) == normalized_requested:
            return fieldname

    raise ValueError(
        f"Column '{requested_name}' was not found. Available columns: {', '.join(fieldnames)}"
    )


def default_output_path(input_path, level):
    return input_path.with_name(f"{input_path.stem}_{level}_matched{input_path.suffix}")


def process_csv(input_csv, output_csv, level, spatial_column, include_levels):
    repo_root = Path(__file__).resolve().parents[1]
    lookup_levels = tuple(dict.fromkeys([level, *include_levels]))
    reference_paths, bbox_map, geometry_map, geonames_map = load_reference_maps(
        lookup_levels, repo_root
    )

    with input_csv.open("r", encoding="utf-8-sig", newline="") as in_handle:
        reader = csv.DictReader(in_handle)
        if not reader.fieldnames:
            raise ValueError(f"{input_csv} is missing a header row.")

        fieldnames = list(reader.fieldnames)
        spatial_field = resolve_spatial_column(fieldnames, spatial_column)

        for column in OUTPUT_COLUMNS:
            if column not in fieldnames:
                fieldnames.append(column)

        row_count = 0
        matched_rows = 0
        unmatched_names = Counter()

        with output_csv.open("w", encoding="utf-8", newline="") as out_handle:
            writer = csv.DictWriter(out_handle, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                row_count += 1
                spatial_value = row.get(spatial_field, "")

                bbox_values = lookup_spatial_values(spatial_value, bbox_map, unmatched_names)
                derived_bbox = combine_bounding_boxes(bbox_values)
                geometry_values = lookup_spatial_values(spatial_value, geometry_map, Counter())
                derived_geometry = combine_geometries(geometry_values)
                derived_geonames = lookup_spatial_values(spatial_value, geonames_map, Counter())

                row["Bounding Box"] = prefer_derived_value(derived_bbox, row.get("Bounding Box", ""))
                row["Geometry"] = prefer_derived_value(derived_geometry, row.get("Geometry", ""))
                row["GeoNames"] = prefer_derived_value(derived_geonames, row.get("GeoNames", ""))

                if derived_bbox:
                    matched_rows += 1

                writer.writerow(row)

    print(f"Input CSV: {input_csv}")
    print(f"Output CSV: {output_csv}")
    print(f"Spatial column: {spatial_field}")
    print(f"Reference levels: {', '.join(lookup_levels)}")
    print(f"Reference CSVs: {', '.join(str(path) for path in reference_paths)}")
    print(f"Rows processed: {row_count}")
    print(f"Rows with bounding box matches: {matched_rows}")

    if unmatched_names:
        most_common = ", ".join(
            f"{name} ({count})" for name, count in unmatched_names.most_common(10) if name
        )
        if most_common:
            print(f"Most common unmatched place names: {most_common}")


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Match values in a CSV's Spatial Coverage column to a reference spatial table "
            "and write Bounding Box, Geometry, and GeoNames columns."
        )
    )
    parser.add_argument("input_csv", type=Path, help="Path to the source CSV.")
    parser.add_argument(
        "-o",
        "--output-csv",
        type=Path,
        help="Path to the output CSV. Defaults to <input>_<level>_matched.csv.",
    )
    parser.add_argument(
        "--level",
        required=True,
        choices=sorted(REFERENCE_CONFIG.keys()),
        help="Reference geography to use for matching.",
    )
    parser.add_argument(
        "--include-level",
        action="append",
        choices=sorted(REFERENCE_CONFIG.keys()),
        default=[],
        help=(
            "Additional reference geography to merge into the lookup. "
            "Repeat to include more than one."
        ),
    )
    parser.add_argument(
        "--spatial-column",
        default="Spatial Coverage",
        help="Name of the spatial coverage column in the input CSV.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    input_csv = args.input_csv.resolve()
    output_csv = args.output_csv.resolve() if args.output_csv else default_output_path(input_csv, args.level)

    if not input_csv.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_csv}")

    process_csv(
        input_csv=input_csv,
        output_csv=output_csv,
        level=args.level,
        spatial_column=args.spatial_column,
        include_levels=args.include_level,
    )


if __name__ == "__main__":
    main()
