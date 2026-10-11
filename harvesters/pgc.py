from __future__ import annotations

import argparse
from collections import Counter
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
import yaml
from urllib3.util.retry import Retry

from harvesters.base import BaseHarvester
from utils.dataframe_cleaner import dataframe_cleaning
from utils.distribution_writer import generate_secondary_table
from utils.geometry_repair import (
    antimeridian_multipolygon_from_bbox_parts,
    bbox_crosses_antimeridian,
    normalize_bbox,
)


class PgcHarvester(BaseHarvester):
    """Harvest public Polar Geospatial Center map-catalog metadata."""

    def __init__(self, config: dict[str, Any]):
        config = dict(config)
        config.setdefault("build_uploads", True)
        config.setdefault("legacy_primary_csv", "inputs/05d-06/primary.csv")
        config.setdefault(
            "legacy_distributions_csv",
            "inputs/05d-06/distributions.csv",
        )
        config.setdefault(
            "inferred_bounding_boxes_yaml",
            "config/pgc-inferred-bounding-boxes.yaml",
        )
        config.setdefault("output_primary_csv", "outputs/pgc_primary.csv")
        config.setdefault(
            "output_distributions_csv",
            "outputs/pgc_distributions.csv",
        )
        config.setdefault("catalog_base_url", "https://maps.apps.pgc.umn.edu")
        config.setdefault("page_size", 1000)
        config.setdefault("timeout", 60)
        config.setdefault("user_agent", "GeoBTAA harvest-operations PGC harvester")
        super().__init__(config)

        self.legacy_id_by_map_id: dict[str, str] = {}
        self.legacy_metadata_by_id: dict[str, dict[str, str]] = {}
        self.inferred_bbox_by_id: dict[str, dict[str, str]] = {}
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.config["user_agent"]})
        retry = Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET"}),
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

    def load_reference_data(self):
        super().load_reference_data()
        (
            self.legacy_id_by_map_id,
            self.legacy_metadata_by_id,
        ) = load_pgc_legacy_metadata(
            Path(self.config["legacy_primary_csv"]),
            Path(self.config["legacy_distributions_csv"]),
        )
        self.inferred_bbox_by_id = load_pgc_bbox_inferences(
            Path(self.config["inferred_bounding_boxes_yaml"])
        )
        print(
            "[PGC] Loaded "
            f"{len(self.legacy_id_by_map_id)} legacy map-ID crosswalk entries."
        )
        print(
            "[PGC] Loaded "
            f"{len(self.inferred_bbox_by_id)} approximate bounding-box decisions."
        )

    def fetch(self):
        records: list[dict[str, Any]] = []
        collections = self.config.get("collections", {})
        if not collections:
            raise ValueError(
                "PGC config must define at least one OGC collection endpoint."
            )

        for region_slug, endpoint in collections.items():
            next_url = str(endpoint)
            params: dict[str, Any] | None = {
                "filter": "visibility = 1",
                "limit": int(self.config["page_size"]),
            }
            collection_records: list[dict[str, Any]] = []
            expected_count: int | None = None

            while next_url:
                response = self.session.get(
                    next_url,
                    params=params,
                    timeout=int(self.config["timeout"]),
                )
                response.raise_for_status()
                payload = response.json()
                if payload.get("type") != "FeatureCollection":
                    raise ValueError(
                        f"PGC endpoint did not return a FeatureCollection: {next_url}"
                    )

                if expected_count is None and payload.get("numberMatched") is not None:
                    expected_count = int(payload["numberMatched"])

                features = payload.get("features")
                if not isinstance(features, list):
                    raise ValueError(
                        f"PGC response is missing a features list: {next_url}"
                    )

                for feature in features:
                    properties = feature.get("properties") or {}
                    if properties.get("visibility") != 1:
                        continue
                    collection_records.append(
                        {
                            **feature,
                            "_pgc_region_slug": str(region_slug),
                        }
                    )

                next_url = next(
                    (
                        str(link["href"])
                        for link in payload.get("links", [])
                        if link.get("rel") == "next" and link.get("href")
                    ),
                    "",
                )
                params = None

            if expected_count is not None and len(collection_records) != expected_count:
                raise ValueError(
                    f"PGC {region_slug} count mismatch: expected {expected_count}, "
                    f"received {len(collection_records)} public records."
                )
            print(
                f"[PGC] Retrieved {len(collection_records)} public {region_slug} maps."
            )
            records.extend(collection_records)

        return records

    def flatten(self, harvested_metadata):
        flattened = []
        for feature in harvested_metadata:
            properties = dict(feature.get("properties") or {})
            properties["_pgc_region_slug"] = feature.get("_pgc_region_slug", "")
            flattened.append(properties)
        return flattened

    def build_dataframe(self, parsed_or_flattened_data):
        records = list(parsed_or_flattened_data)
        reference_counts = Counter(
            normalize_pgc_reference_id(record.get("reference_id"))
            for record in records
            if normalize_pgc_reference_id(record.get("reference_id"))
        )
        rows = [
            pgc_record_to_schema(
                record,
                legacy_id_by_map_id=self.legacy_id_by_map_id,
                reference_counts=reference_counts,
                catalog_base_url=self.config["catalog_base_url"],
            )
            for record in records
        ]
        frame = pd.DataFrame(rows)
        if frame["ID"].eq("").any() or frame["ID"].duplicated().any():
            duplicates = sorted(frame.loc[frame["ID"].duplicated(False), "ID"].unique())
            raise ValueError(
                f"PGC output IDs must be nonblank and unique: {duplicates}"
            )
        print(f"[PGC] Mapped {len(frame)} records to the GeoBTAA schema.")
        return frame

    def derive_fields(self, df):
        df = df.pipe(
            apply_pgc_legacy_enrichments,
            legacy_metadata_by_id=self.legacy_metadata_by_id,
        )
        df = df.pipe(
            apply_pgc_bbox_inferences,
            inferred_bbox_by_id=self.inferred_bbox_by_id,
        )
        return super().derive_fields(df)

    def add_defaults(self, df):
        df = super().add_defaults(df)
        df["Provider"] = "University of Minnesota"
        df["Resource Class"] = "Maps"
        df["Language"] = df.get("Language", "").replace("", "eng")
        df["Member Of"] = "b0153110-e455-4ced-9114-9b13250a7093"
        df["Is Part Of"] = "05d-06"
        df["Code"] = "05d-06"
        df["Is Harvested"] = "true"
        return df

    def add_provenance(self, df):
        df = super().add_provenance(df)
        today = time.strftime("%Y-%m-%d")
        df["Date Accessioned"] = df.apply(
            lambda row: self.legacy_metadata_by_id.get(row["ID"], {}).get(
                "Date Accessioned", today
            )
            or today,
            axis=1,
        )
        df["Website Platform"] = "Custom data portal"
        df["Endpoint URL"] = "|".join(self.config["collections"].values())
        df["Endpoint Description"] = "OGC API Features"
        df["Last Harvested"] = today
        df["Accrual Method"] = "Automated retrieval"
        df["Accrual Periodicity"] = "Irregular"
        df["Harvest Workflow"] = "py_pgc"
        df["Provenance"] = (
            "The metadata for this resource was last retrieved from the Polar "
            f"Geospatial Center on {today}."
        )
        return df

    def clean(self, df):
        df = dataframe_cleaning(df)
        return df.pipe(clean_pgc_spatial_fields)

    def write_outputs(self, primary_df, distributions_df=None):
        distributions_df = generate_secondary_table(
            primary_df.copy(),
            self.distribution_types,
        )
        return super().write_outputs(primary_df, distributions_df)

    def build_uploads(self, results: dict) -> dict | None:
        return super().build_uploads(results)


# Custom functions for this harvester


LEGACY_ENRICHMENT_FIELDS = (
    "Creator",
    "Creator ID",
    "Language",
    "Spatial Coverage",
    "Keyword",
    "Tags",
    "Date Accessioned",
    "Bounding Box",
    "Geometry",
    "Centroid",
)

MAPSERVER_NULL = "_MAPSERVER_NULL_"


def load_pgc_legacy_metadata(
    primary_path: Path,
    distributions_path: Path,
) -> tuple[dict[str, str], dict[str, dict[str, str]]]:
    """Load the manual PGC map-ID crosswalk and sparse curated enrichments."""
    primary = pd.read_csv(primary_path, dtype=str, keep_default_na=False)
    distributions = pd.read_csv(
        distributions_path,
        dtype=str,
        keep_default_na=False,
    )
    required_primary = {"ID", *LEGACY_ENRICHMENT_FIELDS}
    required_distributions = {
        "friendlier_id",
        "reference_type",
        "distribution_url",
    }
    if missing := required_primary - set(primary.columns):
        raise ValueError(f"{primary_path}: missing columns {sorted(missing)}")
    if missing := required_distributions - set(distributions.columns):
        raise ValueError(f"{distributions_path}: missing columns {sorted(missing)}")

    primary_ids = set(primary["ID"].astype(str).str.strip())
    legacy_metadata = {
        str(row["ID"]).strip(): {
            field: clean_pgc_value(row.get(field)) for field in LEGACY_ENRICHMENT_FIELDS
        }
        for row in primary.to_dict("records")
        if str(row.get("ID", "")).strip() and str(row.get("ID", "")).strip() != "05d-06"
    }

    crosswalk: dict[str, str] = {}
    documentation = distributions.loc[
        distributions["reference_type"].eq("documentation_external")
    ]
    for row in documentation.to_dict("records"):
        record_id = clean_pgc_value(row.get("friendlier_id"))
        if record_id not in primary_ids or record_id == "05d-06":
            continue
        map_id = legacy_map_id_from_url(row.get("distribution_url"))
        if not map_id:
            continue
        if map_id in crosswalk and crosswalk[map_id] != record_id:
            raise ValueError(f"Legacy PGC map ID {map_id} maps to multiple record IDs.")
        crosswalk[map_id] = record_id

    return crosswalk, legacy_metadata


def legacy_map_id_from_url(value: Any) -> str:
    path = urlparse(clean_pgc_value(value)).path.rstrip("/")
    match = re.search(r"/id/(\d+)$", path)
    return match.group(1) if match else ""


def load_pgc_bbox_inferences(path: Path) -> dict[str, dict[str, str]]:
    """Load reviewed approximate extents for records lacking source coordinates."""
    with path.open(encoding="utf-8") as stream:
        payload = yaml.safe_load(stream) or {}
    records = payload.get("records", {})
    if not isinstance(records, dict):
        raise ValueError(f"{path}: 'records' must be a mapping")

    decisions: dict[str, dict[str, str]] = {}
    for record_id, decision in records.items():
        if not isinstance(decision, dict):
            raise ValueError(f"{path}: decision for {record_id} must be a mapping")
        bbox = format_pgc_bounding_box(decision.get("bounding_box"))
        note = clean_pgc_value(decision.get("note"))
        parts, bbox_error = normalize_bbox(bbox)
        if not parts:
            raise ValueError(f"{path}: invalid bounding box for {record_id}: {bbox_error}")
        if not note:
            raise ValueError(f"{path}: missing note for {record_id}")
        decisions[str(record_id)] = {"bounding_box": bbox, "note": note}
    return decisions


def apply_pgc_bbox_inferences(
    df: pd.DataFrame,
    inferred_bbox_by_id: dict[str, dict[str, str]],
) -> pd.DataFrame:
    """Fill only blank spatial fields from reviewed title/coverage decisions."""
    frame = df.copy()
    for index, row in frame.iterrows():
        if clean_pgc_value(row.get("Bounding Box")):
            continue
        decision = inferred_bbox_by_id.get(clean_pgc_value(row.get("ID")))
        if not decision:
            continue
        bbox = decision["bounding_box"]
        frame.at[index, "Bounding Box"] = bbox
        frame.at[index, "Geometry"] = ""
        frame.at[index, "Centroid"] = pgc_centroid(bbox)
        existing_note = clean_pgc_value(row.get("Admin Note"))
        frame.at[index, "Admin Note"] = " ".join(
            value for value in (existing_note, decision["note"]) if value
        )
    return frame


def normalize_pgc_reference_id(value: Any) -> str:
    return re.sub(r"\s+", "-", clean_pgc_value(value))


def pgc_record_to_schema(
    record: dict[str, Any],
    legacy_id_by_map_id: dict[str, str],
    reference_counts: Counter,
    catalog_base_url: str,
) -> dict[str, Any]:
    map_id = pgc_integer_text(record.get("map_id"))
    reference_id = normalize_pgc_reference_id(record.get("reference_id"))
    if map_id in legacy_id_by_map_id:
        record_id = legacy_id_by_map_id[map_id]
    elif reference_id and reference_counts[reference_id] == 1:
        record_id = reference_id
    elif reference_id:
        record_id = f"{reference_id}-{map_id}"
    else:
        record_id = f"PGC-{map_id}"

    region_slug = clean_pgc_value(record.get("_pgc_region_slug")).lower()
    region_name = clean_pgc_value(record.get("region_name"))
    title = pgc_title(record.get("title"), region_name)
    year = pgc_year(record.get("auth_year"))
    bbox = pgc_bounding_box(record)
    downloads = pgc_downloads(record)
    keywords = pgc_pipe_values(
        record.get("area_name"),
        record.get("series_name"),
        record.get("theme_name"),
        record.get("purpose_name"),
        record.get("type_name"),
    )
    organization = clean_pgc_value(record.get("org_name"))
    status = pgc_integer_text(record.get("status"))

    return {
        "Title": title,
        "Alternative Title": clean_pgc_value(record.get("orig_map_name")),
        "Description": pgc_description(record),
        "Creator": clean_pgc_value(record.get("auth_person")),
        "Publisher": organization,
        "Resource Type": pgc_resource_type(record.get("type_name")),
        "Keyword": keywords,
        "Local Collection": clean_pgc_value(record.get("series_name")),
        "Temporal Coverage": year,
        "Date Issued": year,
        "Index Year": year,
        "Date Range": f"{year}-{year}" if year else "",
        "Spatial Coverage": pgc_spatial_coverage(
            record.get("area_name"),
            region_name,
        ),
        "Bounding Box": bbox,
        "Geometry": "",
        "Centroid": pgc_centroid(bbox),
        "Coordinate Reference System": clean_pgc_value(record.get("projection")),
        "Spatial Resolution as Text": pgc_scale(record.get("scale")),
        "Georeferenced": "true" if record.get("is_geo") == 1 else "false",
        "B1G Image": pgc_preview(record),
        "Format": pgc_format(record),
        "Identifier": f"PGC Map ID {map_id}",
        "Source": "",
        "Rights": pgc_rights(organization),
        "Rights Holder": "",
        "Admin Note": (
            "PGC source status is 0; retained because the map is public in the "
            "current catalog."
            if status == "0"
            else ""
        ),
        "ID": record_id,
        "information": (
            f"{catalog_base_url.rstrip('/')}/mapinfo/{region_slug}/{map_id}"
        ),
        "download": downloads,
    }


def apply_pgc_legacy_enrichments(
    df: pd.DataFrame,
    legacy_metadata_by_id: dict[str, dict[str, str]],
) -> pd.DataFrame:
    """Overlay nonblank curated legacy values without erasing current metadata."""
    frame = df.copy()
    for index, row in frame.iterrows():
        legacy = legacy_metadata_by_id.get(str(row.get("ID", "")), {})
        if not legacy:
            continue
        for field in ("Creator", "Creator ID", "Language", "Spatial Coverage", "Tags"):
            value = clean_pgc_value(legacy.get(field))
            if value:
                frame.at[index, field] = value
        legacy_keywords = clean_pgc_value(legacy.get("Keyword"))
        if legacy_keywords:
            frame.at[index, "Keyword"] = pgc_pipe_values(
                legacy_keywords,
                row.get("Keyword"),
            )
        for field in ("Bounding Box", "Geometry", "Centroid"):
            value = clean_pgc_value(legacy.get(field))
            if value:
                frame.at[index, field] = value
    return frame


def pgc_downloads(record: dict[str, Any]) -> list[dict[str, str]]:
    base_url = clean_pgc_value(record.get("url_base"))
    filename = clean_pgc_value(record.get("filename"))
    if not base_url or not filename:
        return []

    encoded_filename = quote(filename, safe="")
    downloads = []
    if record.get("is_crop") == 1:
        downloads.append(
            {
                "url": f"{base_url.rstrip('/')}/crop/{encoded_filename}.zip",
                "label": "Cropped GeoTIFF",
            }
        )
    if record.get("is_geo") == 1:
        downloads.append(
            {
                "url": f"{base_url.rstrip('/')}/geo/{encoded_filename}.zip",
                "label": "GeoTIFF",
            }
        )
    if record.get("is_tif") == 1:
        downloads.append(
            {
                "url": f"{base_url.rstrip('/')}/tif/{encoded_filename}.tif",
                "label": "TIFF",
            }
        )
    if record.get("is_pdf") == 1:
        downloads.append(
            {
                "url": f"{base_url.rstrip('/')}/pdf/{encoded_filename}.pdf",
                "label": "PDF",
            }
        )
    return downloads


def pgc_preview(record: dict[str, Any]) -> str:
    base_url = clean_pgc_value(record.get("url_base"))
    filename = clean_pgc_value(record.get("filename"))
    if not base_url or not filename or record.get("is_preview") != 1:
        return ""
    encoded_filename = quote(filename, safe="")
    return f"{base_url.rstrip('/')}/preview/{encoded_filename}.jpg"


def pgc_bounding_box(record: dict[str, Any]) -> str:
    try:
        west = float(record["lon_lower_left"])
        east = float(record["lon_upper_right"])
        latitude_one = float(record["lat_lower_left"])
        latitude_two = float(record["lat_upper_right"])
    except (KeyError, TypeError, ValueError):
        return ""
    if (
        clean_pgc_value(record.get("_pgc_region_slug")).casefold() == "antarctica"
        and latitude_one > 0
        and latitude_two > 0
    ):
        latitude_one *= -1
        latitude_two *= -1
    south = min(latitude_one, latitude_two)
    north = max(latitude_one, latitude_two)
    if not all((-180 <= west <= 180, -180 <= east <= 180, -90 <= south <= north <= 90)):
        return ""
    return ",".join(pgc_coordinate(value) for value in (west, south, east, north))


def format_pgc_bounding_box(value: Any) -> str:
    text = clean_pgc_value(value)
    if not text:
        return ""
    try:
        coordinates = [round(float(part.strip()), 3) for part in text.split(",")]
    except ValueError:
        return text
    if len(coordinates) != 4:
        return text
    if all(coordinate.is_integer() for coordinate in coordinates):
        return ",".join(str(int(coordinate)) for coordinate in coordinates)
    return ",".join(f"{coordinate:.3f}" for coordinate in coordinates)


def clean_pgc_spatial_fields(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    if "Bounding Box" not in frame.columns:
        return frame
    frame["Bounding Box"] = frame["Bounding Box"].map(format_pgc_bounding_box)
    if "Geometry" not in frame.columns:
        frame["Geometry"] = ""

    def geometry(bbox: str) -> str:
        if not bbox:
            return ""
        parts = tuple(bbox.split(","))
        if bbox_crosses_antimeridian(parts):
            return antimeridian_multipolygon_from_bbox_parts(parts)
        west, south, east, north = parts
        return f"ENVELOPE({west},{east},{north},{south})"

    frame["Geometry"] = frame["Bounding Box"].map(geometry)
    return frame


def pgc_centroid(bbox: str) -> str:
    if not bbox:
        return ""
    west, south, east, north = (float(part) for part in bbox.split(","))
    if west > east and west - east > 180:
        longitude = (west + east + 360) / 2
        if longitude > 180:
            longitude -= 360
    else:
        longitude = (west + east) / 2
    latitude = (south + north) / 2
    return f"{pgc_coordinate(latitude)},{pgc_coordinate(longitude)}"


def pgc_description(record: dict[str, Any]) -> str:
    description = clean_pgc_value(record.get("map_description"))
    return description.rstrip(".;")


def pgc_title(value: Any, region_name: str) -> str:
    title = clean_pgc_value(value) or "Untitled map"
    if region_name and region_name.casefold() not in title.casefold():
        return f"{title}: {region_name}"
    return title


def pgc_spatial_coverage(area_name: Any, region_name: str) -> str:
    area = clean_pgc_value(area_name)
    if area and region_name and area.casefold() != region_name.casefold():
        return f"{region_name}--{area}|{region_name}"
    return region_name or area


def pgc_resource_type(value: Any) -> str:
    source_type = clean_pgc_value(value)
    mapping = {
        "Aerial Photography": "Aerial photographs",
        "Line Drawing": "Outline maps",
        "Physical": "Physical maps",
        "Satellite": "Satellite maps",
        "Sketch": "Sketch maps",
        "Topographic": "Topographic maps",
    }
    return mapping.get(source_type, "Maps")


def pgc_format(record: dict[str, Any]) -> str:
    if record.get("is_geo") == 1:
        return "GeoTIFF"
    if record.get("is_tif") == 1:
        return "TIFF"
    if record.get("is_pdf") == 1:
        return "PDF"
    return "Digital map"


def pgc_scale(value: Any) -> str:
    number = pgc_integer_text(value)
    return f"Scale 1:{int(number):,}" if number else ""


def pgc_rights(organization: str) -> str:
    source = f" ({organization})" if organization else ""
    return (
        "Users must acknowledge the Polar Geospatial Center and its sponsorship "
        "by the National Science Foundation and attribute the original source"
        f"{source}. See the PGC Acknowledgement Policy: "
        "https://www.pgc.umn.edu/guides/user-services/acknowledgement-policy/. "
        "Copyright status may vary by original source."
    )


def pgc_pipe_values(*values: Any) -> str:
    result = []
    seen = set()
    for value in values:
        for part in clean_pgc_value(value).split("|"):
            cleaned = part.strip()
            key = cleaned.casefold()
            if cleaned and key not in seen:
                result.append(cleaned)
                seen.add(key)
    return "|".join(result)


def pgc_year(value: Any) -> str:
    text = pgc_integer_text(value)
    return text if re.fullmatch(r"(?:18|19|20)\d{2}", text) else ""


def pgc_integer_text(value: Any) -> str:
    cleaned = clean_pgc_value(value)
    if not cleaned:
        return ""
    try:
        number = float(cleaned)
    except ValueError:
        return ""
    return str(int(number)) if number.is_integer() else ""


def pgc_coordinate(value: float) -> str:
    text = f"{value:.6f}".rstrip("0").rstrip(".")
    return "0" if text == "-0" else text


def clean_pgc_value(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    return "" if text == MAPSERVER_NULL else text


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Harvest public Polar Geospatial Center map metadata."
    )
    parser.add_argument(
        "--config",
        default="config/pgc.yaml",
        help="Path to the PGC YAML configuration file.",
    )
    args = parser.parse_args()
    with Path(args.config).open(encoding="utf-8") as handle:
        config = yaml.safe_load(handle) or {}
    PgcHarvester(config).harvest_pipeline()


if __name__ == "__main__":
    main()
