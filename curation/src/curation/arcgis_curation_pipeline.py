"""Staged ArcGIS Hub curation pipeline.

The metadata stage reuses the ArcGIS harvester's dataframe transformations.  The
remaining stages intentionally stop behind a recorded manual-review checkpoint.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import re
import secrets
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import urlsplit, urlunsplit

import fiona
import pandas as pd
import requests
import yaml
from rasterio.warp import transform_bounds


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from harvesters.arcgis import (  # noqa: E402
    ArcGISHarvester,
    arcgis_extract_distributions,
    arcgis_harvest_identifier_and_id,
    arcgis_map_to_schema,
)
from harvesters.base import BaseHarvester  # noqa: E402
from utils.field_order import PRIMARY_FIELD_ORDER  # noqa: E402
from utils.harvester_helpers import read_csv_rows  # noqa: E402

from curation.embed_qgis_metadata import (  # noqa: E402
    embed_metadata_directory,
    get_default_template_path,
)
from curation.thumbnails import create_vector_thumbnail  # noqa: E402
from curation.zip_geopackages import zip_one_geopackage  # noqa: E402


LOGGER = logging.getLogger(__name__)
RUN_RECORDS_ROOT = REPO_ROOT / "curation" / "run_records"
DEFAULT_REQUIRED_REVIEW_FIELDS = (
    "filename",
    "ID",
    "Title",
    "Description",
    "Creator",
    "Publisher",
    "Provider",
    "Resource Class",
    "Rights",
    "Access Rights",
)
GEOMETRY_RESOURCE_TYPES = {
    "esrigeometrypolygon": "Polygon data",
    "esrigeometrypolyline": "Line data",
    "esrigeometrypoint": "Point data",
    "esrigeometrymultipoint": "Point data",
}
GEOPACKAGE_GEOMETRY_RESOURCE_TYPES = {
    "polygon": "Polygon data",
    "multipolygon": "Polygon data",
    "linestring": "Line data",
    "multilinestring": "Line data",
    "point": "Point data",
    "multipoint": "Point data",
}
NANOID_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
NANOID_LENGTH = 12
DICTIONARY_COLUMNS = (
    "friendlier_id",
    "field_name",
    "field_type",
    "values",
    "definition",
    "definition_source",
    "parent_field_name",
    "position",
)
SNAPSHOT_REQUIRED_STAGES = (
    "metadata",
    "download",
    "enrich",
    "dictionaries",
    "embed",
    "thumbnails",
    "derivatives",
    "zip",
)
RESOURCE_ARTIFACT_ROLES = {
    ".gpkg": "geopackage",
    ".fgb": "flatgeobuf",
    ".pmtiles": "pmtiles",
    ".png": "thumbnail",
    ".csv": "data_dictionary",
    ".zip": "geopackage_archive",
}


class CurationConfigError(ValueError):
    """Raised when a curation YAML file does not satisfy the input contract."""


@dataclass(frozen=True)
class RecordSpec:
    """One selected ArcGIS item/sublayer and its curated output filename."""

    source_id: str
    filename_stem: str
    basic_theme: str = ""
    temporal_year: str = ""
    source_type: str = "dcat"
    service_url: str = ""
    portal_url: str = "https://www.arcgis.com"
    item_id: str = ""
    metadata_overrides: dict[str, Any] | None = None

    @property
    def filename(self) -> str:
        return f"{self.filename_stem}.gpkg"


@dataclass(frozen=True)
class JobConfig:
    """Validated ArcGIS curation job configuration."""

    config_path: Path
    job_id: str
    work_dir: Path
    hub_name: str
    hub_landing_page: str
    dcat_api: str
    website_reference_id: str
    websites_csv: Path
    crs_authority: str
    crs_uri: str
    provider: str
    code: str
    member_of: str
    export_date: date
    records: tuple[RecordSpec, ...]
    allowed_resource_types: tuple[str, ...] = (
        "Polygon data",
        "Line data",
        "Point data",
    )
    required_review_fields: tuple[str, ...] = DEFAULT_REQUIRED_REVIEW_FIELDS
    pmtiles_config: Path | None = None

    @property
    def metadata_path(self) -> Path:
        return self.work_dir / "metadata" / "metadata.csv"

    @property
    def manifest_path(self) -> Path:
        return self.work_dir / "manifest.json"

    def resource_dir(self, filename: str) -> Path:
        return self.work_dir / Path(filename).stem

    def gpkg_path(self, filename: str) -> Path:
        return self.resource_dir(filename) / filename

    def dictionary_path(self, filename: str) -> Path:
        stem = Path(filename).stem
        return self.resource_dir(filename) / f"{stem}.csv"

    def thumbnail_path(self, filename: str) -> Path:
        stem = Path(filename).stem
        return self.resource_dir(filename) / f"{stem}.png"

    @property
    def report_dir(self) -> Path:
        return self.work_dir / "reports"


JsonRequester = Callable[[str, dict[str, Any] | None, str], dict[str, Any]]
DIRECT_REST_SOURCE_TYPE = "arcgis_rest"
DIRECT_REST_METADATA_OVERRIDE_KEYS = {
    "title",
    "description",
    "creator",
    "rights",
    "keywords",
    "landing_page",
}
ARCGIS_LAYER_URL_RE = re.compile(
    r"/(?:FeatureServer|MapServer)/(?P<layer_id>\d+)/?$",
    re.IGNORECASE,
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise CurationConfigError(f"{label} must be a mapping")
    return value


def _required_text(mapping: dict[str, Any], key: str, label: str) -> str:
    value = str(mapping.get(key, "")).strip()
    if not value:
        raise CurationConfigError(f"Missing required value: {label}.{key}")
    return value


def _required_string(mapping: dict[str, Any], key: str, label: str) -> str:
    value = mapping.get(key)
    if not isinstance(value, str) or not value.strip():
        raise CurationConfigError(
            f"{label}.{key} must be a quoted, non-empty YAML string"
        )
    return value.strip()


def _resolve_path(value: str, config_path: Path) -> Path:
    path = Path(value).expanduser()
    return path.resolve() if path.is_absolute() else (config_path.parent / path).resolve()


def normalize_arcgis_layer_url(value: str, label: str) -> str:
    """Validate and normalize a public ArcGIS vector layer URL."""
    parsed = urlsplit(value.strip())
    if (
        parsed.scheme.casefold() not in {"http", "https"}
        or not parsed.netloc
        or parsed.query
        or parsed.fragment
        or not ARCGIS_LAYER_URL_RE.search(parsed.path)
    ):
        raise CurationConfigError(
            f"{label} must be an HTTP(S) layer URL ending in "
            "FeatureServer/<layer-id> or MapServer/<layer-id>"
        )
    normalized_path = parsed.path.rstrip("/")
    return urlunsplit((parsed.scheme, parsed.netloc, normalized_path, "", ""))


def normalize_portal_url(value: str, label: str) -> str:
    parsed = urlsplit(value.strip())
    if (
        parsed.scheme.casefold() not in {"http", "https"}
        or not parsed.netloc
        or parsed.query
        or parsed.fragment
    ):
        raise CurationConfigError(f"{label} must be an HTTP(S) ArcGIS portal URL")
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", ""))


def load_job_config(config_path: Path | str) -> JobConfig:
    """Load and validate a curation YAML file."""
    path = Path(config_path).expanduser().resolve()
    with path.open(encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}

    if raw.get("version") != 1:
        raise CurationConfigError("version must be 1")

    job_raw = _mapping(raw.get("job"), "job")
    hub_raw = _mapping(raw.get("hub"), "hub")
    crs_raw = _mapping(raw.get("coordinate_reference_system"), "coordinate_reference_system")
    metadata_raw = _mapping(raw.get("metadata"), "metadata")
    selection_raw = _mapping(raw.get("selection_criteria", {}), "selection_criteria")
    review_raw = _mapping(raw.get("manual_review", {}), "manual_review")
    derivatives_raw = _mapping(raw.get("derivatives", {}), "derivatives")

    records_raw = raw.get("records")
    if not isinstance(records_raw, list) or not records_raw:
        raise CurationConfigError("records must be a non-empty list")

    records: list[RecordSpec] = []
    seen_ids: set[str] = set()
    seen_filenames: set[str] = set()
    for index, record_value in enumerate(records_raw, start=1):
        record_raw = _mapping(record_value, f"records[{index}]")
        source_id = _required_text(record_raw, "id", f"records[{index}]")
        filename = _required_text(record_raw, "filename", f"records[{index}]")
        filename_path = Path(filename)
        if filename_path.name != filename or filename in {".", ".."}:
            raise CurationConfigError(
                f"records[{index}].filename must be a filename, not a path"
            )
        filename_stem = filename_path.stem if filename_path.suffix.lower() == ".gpkg" else filename
        basic_theme = str(record_raw.get("basic_theme", "")).strip()
        temporal_year = str(record_raw.get("temporal_year", "")).strip()
        if temporal_year and not re.fullmatch(r"(?:19|20)\d{2}", temporal_year):
            raise CurationConfigError(
                f"records[{index}].temporal_year must be a four-digit year"
            )

        source_value = record_raw.get("source")
        source_raw = (
            _mapping(source_value, f"records[{index}].source")
            if source_value is not None
            else {}
        )
        unknown_source_keys = sorted(
            set(source_raw) - {"type", "service_url", "portal_url", "item_id"}
        )
        if unknown_source_keys:
            raise CurationConfigError(
                f"Unsupported records[{index}].source keys: "
                + ", ".join(unknown_source_keys)
            )
        source_type = str(source_raw.get("type", "dcat")).strip().casefold()
        if source_type not in {"dcat", DIRECT_REST_SOURCE_TYPE}:
            raise CurationConfigError(
                f"records[{index}].source.type must be 'dcat' or "
                f"'{DIRECT_REST_SOURCE_TYPE}'"
            )
        service_url = ""
        portal_url = "https://www.arcgis.com"
        item_id = str(source_raw.get("item_id", "")).strip()
        if source_type == DIRECT_REST_SOURCE_TYPE:
            service_url = normalize_arcgis_layer_url(
                _required_text(
                    source_raw,
                    "service_url",
                    f"records[{index}].source",
                ),
                f"records[{index}].source.service_url",
            )
            portal_url = normalize_portal_url(
                str(source_raw.get("portal_url", portal_url)),
                f"records[{index}].source.portal_url",
            )
        elif any(key in source_raw for key in ("service_url", "portal_url", "item_id")):
            raise CurationConfigError(
                f"records[{index}].source REST settings require "
                f"type: {DIRECT_REST_SOURCE_TYPE}"
            )

        overrides_value = record_raw.get("metadata_overrides", {})
        metadata_overrides = _mapping(
            overrides_value,
            f"records[{index}].metadata_overrides",
        )
        unknown_override_keys = sorted(
            set(metadata_overrides) - DIRECT_REST_METADATA_OVERRIDE_KEYS
        )
        if unknown_override_keys:
            raise CurationConfigError(
                f"Unsupported records[{index}].metadata_overrides keys: "
                + ", ".join(unknown_override_keys)
            )
        if metadata_overrides and source_type != DIRECT_REST_SOURCE_TYPE:
            raise CurationConfigError(
                f"records[{index}].metadata_overrides requires "
                f"source.type: {DIRECT_REST_SOURCE_TYPE}"
            )
        keywords_override = metadata_overrides.get("keywords")
        if keywords_override is not None and not isinstance(
            keywords_override,
            (str, list),
        ):
            raise CurationConfigError(
                f"records[{index}].metadata_overrides.keywords must be a string or list"
            )
        for override_key in DIRECT_REST_METADATA_OVERRIDE_KEYS - {"keywords"}:
            if override_key in metadata_overrides and not isinstance(
                metadata_overrides[override_key],
                str,
            ):
                raise CurationConfigError(
                    f"records[{index}].metadata_overrides.{override_key} "
                    "must be a string"
                )
        if not filename_stem:
            raise CurationConfigError(f"records[{index}].filename is empty")
        if source_id in seen_ids:
            raise CurationConfigError(f"Duplicate record id: {source_id}")
        if filename_stem.casefold() in seen_filenames:
            raise CurationConfigError(f"Duplicate output filename: {filename_stem}.gpkg")
        seen_ids.add(source_id)
        seen_filenames.add(filename_stem.casefold())
        records.append(
            RecordSpec(
                source_id=source_id,
                filename_stem=filename_stem,
                basic_theme=basic_theme,
                temporal_year=temporal_year,
                source_type=source_type,
                service_url=service_url,
                portal_url=portal_url,
                item_id=item_id,
                metadata_overrides=dict(metadata_overrides),
            )
        )

    allowed_types_value = selection_raw.get(
        "allowed_resource_types",
        ["Polygon data", "Line data", "Point data"],
    )
    known_vector_types = set(GEOMETRY_RESOURCE_TYPES.values())
    if not isinstance(allowed_types_value, list) or not allowed_types_value:
        raise CurationConfigError(
            "selection_criteria.allowed_resource_types must be a non-empty list"
        )
    allowed_resource_types = tuple(str(value).strip() for value in allowed_types_value)
    invalid_types = sorted(set(allowed_resource_types) - known_vector_types)
    if invalid_types:
        raise CurationConfigError(
            "Unsupported allowed_resource_types: " + ", ".join(invalid_types)
        )

    required_fields_value = review_raw.get("required_fields", DEFAULT_REQUIRED_REVIEW_FIELDS)
    if not isinstance(required_fields_value, list) or not all(
        isinstance(value, str) and value.strip() for value in required_fields_value
    ):
        raise CurationConfigError("manual_review.required_fields must be a list of field names")

    websites_value = str(
        hub_raw.get("websites_csv", REPO_ROOT / "reference_data" / "websites.csv")
    )
    websites_csv = _resolve_path(websites_value, path)
    if not websites_csv.is_file():
        raise CurationConfigError(f"websites_csv does not exist: {websites_csv}")

    pmtiles_value = str(derivatives_raw.get("pmtiles_config", "")).strip()
    pmtiles_config = _resolve_path(pmtiles_value, path) if pmtiles_value else None
    if pmtiles_config is not None and not pmtiles_config.is_file():
        raise CurationConfigError(f"PMTiles config does not exist: {pmtiles_config}")

    export_date_text = _required_text(metadata_raw, "export_date", "metadata")
    try:
        export_date = date.fromisoformat(export_date_text)
    except ValueError as exc:
        raise CurationConfigError("metadata.export_date must use YYYY-MM-DD") from exc

    job_id = _required_text(job_raw, "id", "job")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", job_id):
        raise CurationConfigError(
            "job.id must start with a letter or number and contain only letters, "
            "numbers, periods, underscores, or hyphens"
        )

    return JobConfig(
        config_path=path,
        job_id=job_id,
        work_dir=_resolve_path(_required_text(job_raw, "work_directory", "job"), path),
        hub_name=_required_text(hub_raw, "name", "hub"),
        hub_landing_page=_required_text(hub_raw, "landing_page", "hub"),
        dcat_api=_required_text(hub_raw, "dcat_api", "hub"),
        website_reference_id=_required_text(hub_raw, "website_reference_id", "hub"),
        websites_csv=websites_csv,
        crs_authority=_required_text(crs_raw, "authority", "coordinate_reference_system"),
        crs_uri=_required_text(crs_raw, "uri", "coordinate_reference_system"),
        provider=str(raw.get("provider", "BTAA-GIN")).strip() or "BTAA-GIN",
        code=_required_string(metadata_raw, "code", "metadata"),
        member_of=_required_text(metadata_raw, "member_of", "metadata"),
        export_date=export_date,
        records=tuple(records),
        allowed_resource_types=allowed_resource_types,
        required_review_fields=tuple(value.strip() for value in required_fields_value),
        pmtiles_config=pmtiles_config,
    )


def default_request_json(
    url: str,
    params: dict[str, Any] | None = None,
    method: str = "GET",
    *,
    timeout: int = 120,
) -> dict[str, Any]:
    """Fetch a JSON object and surface ArcGIS error payloads as exceptions."""
    headers = {"User-Agent": "BTAA-GIN ArcGIS curation pipeline/1.0"}
    if method.upper() == "POST":
        response = requests.post(url, data=params or {}, headers=headers, timeout=timeout)
    else:
        response = requests.get(url, params=params or {}, headers=headers, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected a JSON object from {url}")
    if payload.get("error"):
        raise RuntimeError(f"ArcGIS error from {url}: {payload['error']}")
    return payload


def _first_text(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        cleaned = str(value).strip()
        if cleaned:
            return cleaned
    return ""


def _arcgis_timestamp_date(value: Any) -> str:
    try:
        timestamp = int(value) / 1000
    except (TypeError, ValueError):
        return ""
    return datetime.fromtimestamp(timestamp, timezone.utc).date().isoformat()


def _direct_rest_keywords(value: Any, geometry_type: str) -> list[str]:
    if isinstance(value, str):
        keywords = [part.strip() for part in value.split("|")]
    elif isinstance(value, list):
        keywords = [str(part).strip() for part in value]
    else:
        keywords = []
    geometry_keyword = {
        "esrigeometrypolygon": "polygon",
        "esrigeometrypolyline": "line",
        "esrigeometrypoint": "point",
        "esrigeometrymultipoint": "point",
    }.get(geometry_type.casefold(), "")
    if geometry_keyword:
        keywords.append(geometry_keyword)
    return list(dict.fromkeys(keyword for keyword in keywords if keyword))


def _item_spatial_extent(value: Any) -> str:
    try:
        lower_left, upper_right = value
        west, south = (float(coordinate) for coordinate in lower_left)
        east, north = (float(coordinate) for coordinate in upper_right)
    except (TypeError, ValueError):
        return ""
    return f"{west},{south},{east},{north}"


def _arcgis_service_root(layer_url: str) -> str:
    return ARCGIS_LAYER_URL_RE.sub(
        lambda match: match.group(0).rsplit("/", 1)[0],
        layer_url,
    )


def build_direct_rest_resource(
    record: RecordSpec,
    requester: JsonRequester,
) -> dict[str, Any]:
    """Build a DCAT-shaped resource from a configured ArcGIS REST layer."""
    layer_url = record.service_url
    layer_metadata = requester(layer_url, {"f": "pjson"}, "GET")
    geometry_type = str(layer_metadata.get("geometryType", "")).casefold()
    resource_type = GEOMETRY_RESOURCE_TYPES.get(geometry_type)
    if not resource_type:
        raise RuntimeError(
            f"Direct ArcGIS REST source is not a supported vector layer: {layer_url}"
        )
    capabilities = str(layer_metadata.get("capabilities", "")).casefold()
    if capabilities and "query" not in capabilities:
        raise RuntimeError(f"Direct ArcGIS REST layer does not support Query: {layer_url}")

    service_url = _arcgis_service_root(layer_url)
    service_metadata = requester(service_url, {"f": "pjson"}, "GET")
    item_id = _first_text(
        record.item_id,
        layer_metadata.get("serviceItemId"),
        service_metadata.get("serviceItemId"),
    )
    item_metadata: dict[str, Any] = {}
    item_api_url = ""
    if item_id:
        item_api_url = (
            f"{record.portal_url}/sharing/rest/content/items/{item_id}"
        )
        try:
            item_metadata = requester(item_api_url, {"f": "pjson"}, "GET")
        except (requests.RequestException, RuntimeError, ValueError) as exc:
            LOGGER.warning(
                "Could not load ArcGIS item metadata for %s; using REST layer metadata: %s",
                layer_url,
                exc,
            )

    overrides = record.metadata_overrides or {}
    item_page = (
        f"{record.portal_url}/home/item.html?id={item_id}"
        if item_id
        else ""
    )
    layer_match = ARCGIS_LAYER_URL_RE.search(layer_url)
    layer_id = layer_match.group("layer_id") if layer_match else ""
    identifier = (
        f"{item_page}&sublayer={layer_id}"
        if item_page and layer_id
        else layer_url
    )
    keywords_value = (
        overrides["keywords"]
        if "keywords" in overrides
        else item_metadata.get("tags", [])
    )
    creator = _first_text(
        overrides.get("creator"),
        item_metadata.get("accessInformation"),
        item_metadata.get("owner"),
    )
    description = _first_text(
        overrides.get("description"),
        item_metadata.get("description"),
        item_metadata.get("snippet"),
        layer_metadata.get("description"),
        service_metadata.get("description"),
        service_metadata.get("serviceDescription"),
    )
    rights = _first_text(
        overrides.get("rights"),
        item_metadata.get("licenseInfo"),
    )
    title = _first_text(
        overrides.get("title"),
        item_metadata.get("title"),
        layer_metadata.get("name"),
    )
    landing_page = _first_text(
        overrides.get("landing_page"),
        item_page,
        layer_url,
    )

    return {
        "identifier": identifier,
        "landingPage": landing_page,
        "title": title,
        "description": description,
        "publisher": {"name": creator},
        "keyword": _direct_rest_keywords(keywords_value, geometry_type),
        "issued": _arcgis_timestamp_date(item_metadata.get("created")),
        "modified": _arcgis_timestamp_date(
            item_metadata.get("modified")
            or (layer_metadata.get("editingInfo") or {}).get("dataLastEditDate")
        ),
        "license": rights,
        "spatial": _item_spatial_extent(item_metadata.get("extent")),
        "distribution": [
            {
                "title": "ArcGIS GeoService",
                "accessURL": layer_url,
            }
        ],
        "_curation_metadata_source": (
            "arcgis_rest_item" if item_metadata else "arcgis_rest_layer"
        ),
        "_curation_item_id": item_id,
        "_curation_item_url": item_page,
    }


def normalized_catalog_id(resource: dict[str, Any]) -> str:
    """Return the ArcGIS item ID with an optional sublayer suffix."""
    _, resource_id = arcgis_harvest_identifier_and_id(str(resource.get("identifier", "")))
    return str(resource_id).strip()


def select_catalog_records(
    catalog: dict[str, Any], records: Iterable[RecordSpec]
) -> list[tuple[RecordSpec, dict[str, Any]]]:
    """Match configured IDs to DCAT datasets in configuration order."""
    datasets = catalog.get("dataset", [])
    if not isinstance(datasets, list):
        raise RuntimeError("DCAT catalog does not contain a dataset list")
    by_id = {
        normalized_catalog_id(resource): resource
        for resource in datasets
        if isinstance(resource, dict) and normalized_catalog_id(resource)
    }
    selected: list[tuple[RecordSpec, dict[str, Any]]] = []
    missing: list[str] = []
    for record in records:
        resource = by_id.get(record.source_id)
        if resource is None:
            missing.append(record.source_id)
        else:
            selected.append((record, resource))
    if missing:
        raise RuntimeError(f"Selected ArcGIS IDs not found in DCAT catalog: {', '.join(missing)}")
    return selected


def resolve_selected_records(
    job: JobConfig,
    *,
    requester: JsonRequester,
    catalog: dict[str, Any] | None = None,
) -> list[tuple[RecordSpec, dict[str, Any]]]:
    """Resolve configured records from DCAT or explicit ArcGIS REST layers."""
    dcat_records = [
        record for record in job.records if record.source_type == "dcat"
    ]
    selected_dcat: dict[str, dict[str, Any]] = {}
    if dcat_records:
        catalog_payload = (
            catalog
            if catalog is not None
            else requester(job.dcat_api, None, "GET")
        )
        selected_dcat = {
            record.source_id: resource
            for record, resource in select_catalog_records(
                catalog_payload,
                dcat_records,
            )
        }

    selected: list[tuple[RecordSpec, dict[str, Any]]] = []
    for record in job.records:
        if record.source_type == DIRECT_REST_SOURCE_TYPE:
            resource = build_direct_rest_resource(record, requester)
        else:
            resource = selected_dcat[record.source_id]
        selected.append((record, resource))
    return selected


def arcgis_service_url(resource: dict[str, Any]) -> str:
    """Extract the FeatureServer/MapServer/ImageServer URL from DCAT distributions."""
    for distribution in resource.get("distribution", []) or []:
        if not isinstance(distribution, dict):
            continue
        if str(distribution.get("title", "")) != "ArcGIS GeoService":
            continue
        url = str(distribution.get("accessURL", "")).strip()
        if url:
            if "/FeatureServer/" not in url and "/MapServer/" not in url:
                raise RuntimeError(
                    "The initial curation pipeline supports vector FeatureServer/MapServer "
                    f"layers only, not {url}"
                )
            return url
    raise RuntimeError(
        f"No ArcGIS GeoService distribution found for {normalized_catalog_id(resource)}"
    )


def load_website_defaults(job: JobConfig) -> dict[str, str]:
    """Load the selected website row from shared reference data."""
    for row in read_csv_rows(str(job.websites_csv)):
        candidates = {str(row.get(key, "")).strip() for key in ("ID", "Code", "Identifier")}
        if job.website_reference_id in candidates:
            return row
    raise RuntimeError(
        f"Website reference {job.website_reference_id!r} was not found in {job.websites_csv}"
    )


def load_theme_map(path: Path) -> dict[str, str]:
    dataframe = pd.read_csv(path, dtype=str).fillna("")
    theme_map: dict[str, str] = {}
    for _, row in dataframe.iterrows():
        for keyword in str(row.get("Keyword", "")).split("|"):
            normalized = keyword.strip().lower()
            if normalized:
                theme_map[normalized] = str(row.get("Theme", ""))
    return theme_map


def archive_display_note(job: JobConfig) -> str:
    return (
        "Warning: This dataset is an archived copy held by the BTAA-GIN. "
        f"For the most current layer, consult {job.hub_name} at {job.hub_landing_page}"
    )


def generate_curated_id(existing_ids: set[str]) -> str:
    """Generate a unique, URL-safe Nano ID with the GeoBTAA prefix."""
    while True:
        suffix = "".join(secrets.choice(NANOID_ALPHABET) for _ in range(NANOID_LENGTH))
        candidate = f"b1g_{suffix}"
        if candidate not in existing_ids:
            existing_ids.add(candidate)
            return candidate


def existing_curated_ids(job: JobConfig) -> dict[str, str]:
    """Reuse IDs from saved run records, preferring the current work manifest."""
    reused: dict[str, str] = {}
    run_record_dir = RUN_RECORDS_ROOT / job.job_id
    saved_manifests = sorted(run_record_dir.glob("*/manifest.json"), reverse=True)
    for manifest_path in saved_manifests:
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if manifest.get("job_id") != job.job_id:
            continue
        for source_id, curated_id in curated_ids_from_manifest(manifest).items():
            reused.setdefault(source_id, curated_id)

    if job.manifest_path.is_file():
        try:
            manifest = json.loads(job.manifest_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            manifest = {}
        if manifest.get("job_id") == job.job_id:
            reused.update(curated_ids_from_manifest(manifest))
    return reused


def curated_ids_from_manifest(manifest: dict[str, Any]) -> dict[str, str]:
    return {
        str(record.get("source_id", "")): str(record.get("curated_id", ""))
        for record in manifest.get("records", [])
        if str(record.get("source_id", "")).strip()
        and str(record.get("curated_id", "")).startswith("b1g_")
    }


def assign_curated_ids(job: JobConfig) -> dict[str, str]:
    reused = existing_curated_ids(job)
    used_ids = set(reused.values())
    return {
        record.source_id: reused.get(record.source_id) or generate_curated_id(used_ids)
        for record in job.records
    }


def formatted_export_date(value: date) -> str:
    return f"{value.strftime('%B')} {value.day}, {value.year}"


def temporal_coverage_year(value: Any) -> str:
    """Extract the first four-digit year from a Temporal Coverage value."""
    match = re.search(r"\b(?:19|20)\d{2}\b", str(value or ""))
    return match.group(0) if match else ""


def humanize_spatial_coverage(value: Any) -> str:
    """Convert an Aardvark hierarchical place into a readable phrase."""
    controlled_place = str(value or "").split("|")[0].strip()
    parts = [part.strip() for part in controlled_place.split("--") if part.strip()]
    return ", ".join(reversed(parts)) if parts else controlled_place


def apply_historical_title_and_description(
    dataframe: pd.DataFrame,
    selected: list[tuple[RecordSpec, dict[str, Any]]],
    source_ids: pd.Series,
) -> pd.DataFrame:
    """Apply the curated historical title and description convention."""
    basic_themes = {
        record.source_id: record.basic_theme
        or str(resource.get("title", "")).strip()
        for record, resource in selected
    }
    configured_years = {
        record.source_id: record.temporal_year
        for record, _ in selected
        if record.temporal_year
    }

    for index in dataframe.index:
        source_id = str(source_ids.loc[index])
        basic_theme = basic_themes.get(source_id, "").strip()
        controlled_place = str(dataframe.loc[index, "Spatial Coverage"]).split("|")[0].strip()
        description_place = humanize_spatial_coverage(controlled_place)
        year = configured_years.get(source_id) or temporal_coverage_year(
            dataframe.loc[index, "Temporal Coverage"]
        )
        if not basic_theme or not controlled_place or not year:
            raise RuntimeError(
                "Historical title inputs are incomplete for "
                f"{source_id}: basic_theme={basic_theme!r}, place={controlled_place!r}, "
                f"temporal_year={year!r}"
            )

        dataframe.loc[index, "Title"] = (
            f"{basic_theme} [{controlled_place}] {{{year}}}"
        )
        prefix = (
            f"Historical dataset of {basic_theme} in {description_place} as of {year}."
        )
        existing_description = str(dataframe.loc[index, "Description"] or "").strip()
        dataframe.loc[index, "Description"] = (
            f"{prefix} {existing_description}" if existing_description else prefix
        )
        if source_id in configured_years:
            dataframe.loc[index, "Temporal Coverage"] = year
            dataframe.loc[index, "Date Range"] = f"{year}-{year}"

    return dataframe


def build_metadata_dataframe(
    job: JobConfig,
    selected: list[tuple[RecordSpec, dict[str, Any]]],
    curated_ids: dict[str, str],
) -> pd.DataFrame:
    """Apply ArcGIS harvester rules, followed by curation-specific exceptions."""
    website_defaults = load_website_defaults(job)
    dcat_workflow = {
        "Endpoint URL": job.dcat_api,
        "Website Platform": "ArcGIS Hub",
        "Endpoint Description": "DCAT-US 1.1",
        "Accrual Method": "Manual curation",
        "Harvest Workflow": "Manual curation",
    }
    harvester = ArcGISHarvester(
        {
            "input_csv": str(job.config_path),
            "hub_metadata_csv": str(job.websites_csv),
            "output_primary_csv": "unused.csv",
            "output_distributions_csv": "unused.csv",
            "output_report_csv": "unused.csv",
            "themes_csv": str(REPO_ROOT / "reference_data" / "themes.csv"),
            "build_uploads": False,
        }
    )
    harvester.theme_map = load_theme_map(REPO_ROOT / "reference_data" / "themes.csv")

    mapped_records: list[pd.DataFrame] = []
    rejected: list[str] = []
    for record, resource in selected:
        workflow = (
            {
                **dcat_workflow,
                "Endpoint URL": record.service_url,
                "Website Platform": "ArcGIS REST",
                "Endpoint Description": "ArcGIS REST layer",
            }
            if record.source_type == DIRECT_REST_SOURCE_TYPE
            else dcat_workflow
        )
        flattened_record = pd.DataFrame(
            [
                {
                    "workflow": workflow,
                    "hub_defaults": website_defaults,
                    "resource": resource,
                }
            ]
        )
        if record.source_type == DIRECT_REST_SOURCE_TYPE:
            mapped = (
                flattened_record.pipe(arcgis_map_to_schema)
                .pipe(arcgis_extract_distributions)
            )
        else:
            mapped = harvester.build_dataframe(flattened_record)
        if mapped.empty:
            rejected.append(record.source_id)
        else:
            mapped_records.append(mapped)
    if rejected:
        raise RuntimeError(
            "Selected records did not pass the ArcGIS harvester's distribution filter: "
            + ", ".join(rejected)
        )
    dataframe = pd.concat(mapped_records, ignore_index=True)

    dataframe = harvester.derive_fields(dataframe)
    dataframe = harvester.add_defaults(dataframe)
    dataframe = BaseHarvester.add_provenance(harvester, dataframe)
    source_ids = pd.Series(
        [record.source_id for record, _ in selected],
        index=dataframe.index,
        dtype=str,
    )
    service_urls = {
        record.source_id: arcgis_service_url(resource)
        for record, resource in selected
    }
    dataframe["Provenance"] = source_ids.map(
        {
            source_id: (
                f"Exported from {service_url} as GeoPackage on "
                f"{formatted_export_date(job.export_date)}."
            )
            for source_id, service_url in service_urls.items()
        }
    ).fillna("")
    dataframe["ID"] = source_ids.map(curated_ids).fillna("")
    dataframe["Code"] = job.code
    dataframe["Member Of"] = job.member_of
    dataframe["Is Part Of"] = ""
    dataframe["Provider"] = job.provider
    dataframe["Display Note"] = archive_display_note(job)
    dataframe["Resource Class"] = "Datasets"
    dataframe["Publication State"] = "draft"
    dataframe["Coordinate Reference System"] = job.crs_uri
    dataframe["Format"] = "GeoPackage"
    dataframe["Source"] = ""
    dataframe["Harvest Workflow"] = "curation_datasets"
    dataframe = apply_historical_title_and_description(dataframe, selected, source_ids)
    dataframe = harvester.clean(dataframe)
    harvester.validate(dataframe)

    filenames = {record.source_id: record.filename for record, _ in selected}
    dataframe.insert(0, "filename", source_ids.map(filenames).fillna(""))
    return dataframe.reindex(columns=["filename", *PRIMARY_FIELD_ORDER], fill_value="")


def write_metadata_csv(dataframe: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(path, index=False, encoding="utf-8")


def build_manifest(
    job: JobConfig,
    selected: list[tuple[RecordSpec, dict[str, Any]]],
    curated_ids: dict[str, str],
) -> dict[str, Any]:
    completed_at = utc_now()
    return {
        "version": 1,
        "job_id": job.job_id,
        "config_path": str(job.config_path),
        "work_directory": str(job.work_dir),
        "metadata_path": str(job.metadata_path),
        "config_sha256": file_sha256(job.config_path),
        "created_at": utc_now(),
        "updated_at": utc_now(),
        "manual_review": {"status": "pending"},
        "stages": {
            "validate": {"status": "completed", "completed_at": completed_at},
            "metadata": {"status": "completed", "completed_at": completed_at},
        },
        "records": [
            {
                "source_id": record.source_id,
                "curated_id": curated_ids[record.source_id],
                "filename": record.filename,
                "landing_page": str(resource.get("landingPage", "")).strip(),
                "service_url": arcgis_service_url(resource),
                "source_type": record.source_type,
                "metadata_source": str(
                    resource.get("_curation_metadata_source", "dcat")
                ),
                **(
                    {
                        "item_id": str(
                            resource.get("_curation_item_id", "")
                        ),
                        "item_url": str(
                            resource.get("_curation_item_url", "")
                        ),
                    }
                    if record.source_type == DIRECT_REST_SOURCE_TYPE
                    else {}
                ),
            }
            for record, resource in selected
        ],
    }


def write_manifest(job: JobConfig, manifest: dict[str, Any]) -> None:
    job.work_dir.mkdir(parents=True, exist_ok=True)
    manifest["updated_at"] = utc_now()
    portable_manifest = portable_manifest_value(job, manifest)
    manifest.clear()
    manifest.update(portable_manifest)
    temporary_path = job.manifest_path.with_suffix(".json.tmp")
    temporary_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    temporary_path.replace(job.manifest_path)


def portable_manifest_value(job: JobConfig, value: Any) -> Any:
    """Recursively convert local paths to portable POSIX paths."""
    if isinstance(value, dict):
        return {key: portable_manifest_value(job, item) for key, item in value.items()}
    if isinstance(value, list):
        return [portable_manifest_value(job, item) for item in value]
    if not isinstance(value, str):
        return value

    path = Path(value)
    if not path.is_absolute():
        return value
    if path.resolve() == job.work_dir.resolve():
        try:
            return path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
        except ValueError:
            return value
    try:
        return path.resolve().relative_to(job.work_dir.resolve()).as_posix()
    except ValueError:
        pass
    try:
        return path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
    except ValueError:
        return value


def load_manifest(job: JobConfig) -> dict[str, Any]:
    if not job.manifest_path.is_file():
        raise RuntimeError(f"Run the metadata stage first; manifest not found: {job.manifest_path}")
    return json.loads(job.manifest_path.read_text(encoding="utf-8"))


def mark_stage(
    job: JobConfig,
    stage: str,
    *,
    details: dict[str, Any] | None = None,
) -> None:
    manifest = load_manifest(job)
    stage_value = {"status": "completed", "completed_at": utc_now()}
    if details:
        stage_value.update(details)
    manifest.setdefault("stages", {})[stage] = stage_value
    write_manifest(job, manifest)


def mark_validation_stage(job: JobConfig) -> None:
    """Persist successful YAML validation even before metadata is harvested."""
    completed_at = utc_now()
    if job.manifest_path.is_file():
        try:
            manifest = load_manifest(job)
        except (json.JSONDecodeError, OSError, RuntimeError):
            manifest = {}
    else:
        manifest = {}

    current_config_sha256 = file_sha256(job.config_path)
    if manifest.get("job_id") != job.job_id:
        manifest = {
            "version": 1,
            "job_id": job.job_id,
            "config_path": str(job.config_path),
            "created_at": completed_at,
            "manual_review": {"status": "pending"},
            "stages": {},
            "records": [],
        }
    elif (
        manifest.get("config_sha256")
        and manifest.get("config_sha256") != current_config_sha256
    ):
        manifest["manual_review"] = {"status": "pending"}
        manifest["stages"] = {}
    manifest["config_sha256"] = current_config_sha256
    manifest.setdefault("stages", {})["validate"] = {
        "status": "completed",
        "completed_at": completed_at,
    }
    write_manifest(job, manifest)


def run_metadata_stage(
    job: JobConfig,
    *,
    requester: JsonRequester = default_request_json,
    catalog: dict[str, Any] | None = None,
) -> Path:
    """Resolve selected ArcGIS records and write the manual-review metadata CSV."""
    selected = resolve_selected_records(
        job,
        requester=requester,
        catalog=catalog,
    )
    curated_ids = assign_curated_ids(job)
    dataframe = build_metadata_dataframe(job, selected, curated_ids)
    write_metadata_csv(dataframe, job.metadata_path)
    write_manifest(job, build_manifest(job, selected, curated_ids))
    return job.metadata_path


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def collect_artifact_records(job: JobConfig) -> list[dict[str, Any]]:
    """Describe generated artifacts without copying them into the run record."""
    artifacts: list[dict[str, Any]] = []
    artifact_locations: list[tuple[Path, str]] = []
    for record in job.records:
        directory = job.resource_dir(record.filename)
        if not directory.is_dir():
            continue
        for artifact_path in sorted(path for path in directory.rglob("*") if path.is_file()):
            if artifact_path.name == ".DS_Store":
                continue
            role = RESOURCE_ARTIFACT_ROLES.get(artifact_path.suffix.casefold())
            if role:
                artifact_locations.append((artifact_path, role))
    if job.report_dir.is_dir():
        artifact_locations.extend(
            (path, "report")
            for path in sorted(job.report_dir.rglob("*"))
            if path.is_file() and path.name != ".DS_Store"
        )

    for artifact_path, role in artifact_locations:
        relative_path = artifact_path.relative_to(job.work_dir).as_posix()
        size_bytes = artifact_path.stat().st_size
        LOGGER.info(
            "Recording %s artifact (%s bytes): %s",
            role,
            size_bytes,
            relative_path,
        )
        artifacts.append(
            {
                "role": role,
                "path": relative_path,
                "size_bytes": size_bytes,
                "sha256": file_sha256(artifact_path),
            }
        )
    return artifacts


def git_run_information() -> dict[str, Any]:
    """Record the root repository revision and whether local changes were present."""
    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    status = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=normal"],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    return {
        "revision": revision.stdout.strip() if revision.returncode == 0 else "",
        "dirty": bool(status.stdout.strip()) if status.returncode == 0 else None,
    }


def next_run_record_id(job: JobConfig) -> str:
    base_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    job_dir = RUN_RECORDS_ROOT / job.job_id
    candidate = base_id
    suffix = 2
    while (job_dir / candidate).exists():
        candidate = f"{base_id}-{suffix:02d}"
        suffix += 1
    return candidate


def save_run_record(job: JobConfig) -> Path:
    """Save an immutable, Git-friendly record of a completed curation run."""
    manifest = require_confirmed_review(job)
    # Backfill portable location fields for manifests created by older pipeline versions.
    manifest["config_path"] = str(job.config_path)
    manifest["work_directory"] = str(job.work_dir)
    manifest["metadata_path"] = str(job.metadata_path)
    stages = manifest.get("stages", {})
    missing_stages = [
        stage
        for stage in SNAPSHOT_REQUIRED_STAGES
        if stages.get(stage, {}).get("status") != "completed"
    ]
    if missing_stages:
        raise RuntimeError(
            "Complete all postprocess stages before saving a run record; missing: "
            + ", ".join(missing_stages)
        )
    if not job.metadata_path.is_file():
        raise RuntimeError(f"Metadata CSV not found: {job.metadata_path}")

    run_id = next_run_record_id(job)
    saved_at = utc_now()
    run_record_parent = RUN_RECORDS_ROOT / job.job_id
    run_record_parent.mkdir(parents=True, exist_ok=True)
    run_record_path = run_record_parent / run_id
    temporary_path = Path(
        tempfile.mkdtemp(prefix=f".{run_id}-", dir=run_record_parent)
    )

    try:
        snapshot = portable_manifest_value(job, json.loads(json.dumps(manifest)))
        snapshot["run_record"] = {
            "run_id": run_id,
            "saved_at": saved_at,
            "record_path": run_record_path.relative_to(REPO_ROOT).as_posix(),
            "metadata_csv": "metadata.csv",
            "job_config": "job.yaml",
            "git": git_run_information(),
        }
        snapshot["artifacts"] = collect_artifact_records(job)
        snapshot.setdefault("stages", {})["snapshot"] = {
            "status": "completed",
            "completed_at": saved_at,
        }
        shutil.copy2(job.metadata_path, temporary_path / "metadata.csv")
        shutil.copy2(job.config_path, temporary_path / "job.yaml")
        (temporary_path / "manifest.json").write_text(
            json.dumps(snapshot, indent=2) + "\n",
            encoding="utf-8",
        )
        temporary_path.replace(run_record_path)
    except Exception:
        if temporary_path.exists():
            shutil.rmtree(temporary_path)
        raise

    manifest.setdefault("stages", {})["snapshot"] = {
        "status": "completed",
        "completed_at": saved_at,
        "run_id": run_id,
        "record_path": str(run_record_path),
    }
    manifest["latest_run_record"] = {
        "run_id": run_id,
        "record_path": str(run_record_path),
    }
    write_manifest(job, manifest)
    return run_record_path


def validate_reviewed_metadata(job: JobConfig) -> pd.DataFrame:
    """Validate row identity and the fields required at the manual checkpoint."""
    if not job.metadata_path.is_file():
        raise RuntimeError(f"Metadata CSV not found: {job.metadata_path}")
    dataframe = pd.read_csv(job.metadata_path, dtype=str, keep_default_na=False).fillna("")
    missing_columns = [
        column for column in job.required_review_fields if column not in dataframe.columns
    ]
    if missing_columns:
        raise RuntimeError(f"Metadata CSV is missing review columns: {', '.join(missing_columns)}")

    expected_filenames = {record.filename for record in job.records}
    actual_filenames = set(dataframe["filename"].astype(str).str.strip())
    if actual_filenames != expected_filenames:
        raise RuntimeError(
            "Metadata filenames must exactly match the YAML records. "
            f"Expected {sorted(expected_filenames)}; found {sorted(actual_filenames)}"
        )
    if dataframe["filename"].duplicated().any():
        raise RuntimeError("Metadata CSV contains duplicate filename values")

    blank_messages: list[str] = []
    for _, row in dataframe.iterrows():
        blank_fields = [
            field_name
            for field_name in job.required_review_fields
            if not str(row.get(field_name, "")).strip()
        ]
        if blank_fields:
            blank_messages.append(f"{row['filename']}: {', '.join(blank_fields)}")
    if blank_messages:
        raise RuntimeError("Manual review fields are blank: " + " | ".join(blank_messages))
    return dataframe


def validate_enriched_metadata(job: JobConfig) -> pd.DataFrame:
    """Require the fields added from the downloaded/service data before embedding."""
    dataframe = validate_reviewed_metadata(job)
    missing = []
    for _, row in dataframe.iterrows():
        blank_fields = [
            field_name
            for field_name in ("Resource Type", "Bounding Box")
            if not str(row.get(field_name, "")).strip()
        ]
        if blank_fields:
            missing.append(f"{row['filename']}: {', '.join(blank_fields)}")
    if missing:
        raise RuntimeError(
            "Run the enrich stage before embedding; derived fields are blank: "
            + " | ".join(missing)
        )
    return dataframe


def confirm_manual_review(job: JobConfig, *, confirmed: bool) -> None:
    if not confirmed:
        raise RuntimeError("Manual review was not confirmed; pass --confirm after editing the CSV")
    manifest = load_manifest(job)
    recorded_config_sha256 = manifest.get("config_sha256")
    if (
        recorded_config_sha256
        and file_sha256(job.config_path) != recorded_config_sha256
    ):
        raise RuntimeError(
            "The YAML job changed after metadata was harvested; run the metadata "
            "stage again before confirming review."
        )
    validate_reviewed_metadata(job)
    manifest["manual_review"] = {
        "status": "confirmed",
        "confirmed_at": utc_now(),
        "metadata_sha256": file_sha256(job.metadata_path),
    }
    write_manifest(job, manifest)


def require_confirmed_review(job: JobConfig) -> dict[str, Any]:
    manifest = load_manifest(job)
    recorded_config_sha256 = manifest.get("config_sha256")
    if (
        recorded_config_sha256
        and file_sha256(job.config_path) != recorded_config_sha256
    ):
        raise RuntimeError(
            "The YAML job changed after metadata was harvested; run the metadata "
            "stage again before confirming review."
        )
    review = manifest.get("manual_review", {})
    if review.get("status") != "confirmed":
        raise RuntimeError(
            "Manual review is pending. Edit metadata/metadata.csv, then run the review command."
        )
    actual_hash = file_sha256(job.metadata_path)
    if actual_hash != review.get("metadata_sha256"):
        raise RuntimeError(
            "Metadata CSV changed after review confirmation; review and confirm it again."
        )
    return manifest


def refresh_review_checksum(job: JobConfig) -> None:
    manifest = load_manifest(job)
    if manifest.get("manual_review", {}).get("status") == "confirmed":
        manifest["manual_review"]["metadata_sha256"] = file_sha256(job.metadata_path)
        manifest["manual_review"]["pipeline_updated_at"] = utc_now()
        write_manifest(job, manifest)


def _chunks(values: list[Any], size: int) -> Iterable[list[Any]]:
    for index in range(0, len(values), size):
        yield values[index : index + size]


def _run_command(command: list[str]) -> None:
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    if completed.returncode != 0:
        raise RuntimeError(
            f"Command failed ({completed.returncode}): {' '.join(command)}\n"
            f"{completed.stderr.strip()}"
        )


def download_service_geopackage(
    service_url: str,
    output_path: Path,
    layer_name: str,
    output_crs: str,
    *,
    requester: JsonRequester = default_request_json,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Page a FeatureServer layer through GeoJSON and assemble a projected GeoPackage."""
    ogr2ogr = shutil.which("ogr2ogr")
    if not ogr2ogr:
        raise RuntimeError("ogr2ogr is required to download GeoPackages")
    if output_path.exists() and not overwrite:
        raise FileExistsError(f"GeoPackage already exists (use --overwrite): {output_path}")

    layer_metadata = requester(service_url, {"f": "pjson"}, "GET")
    ids_payload = requester(
        f"{service_url.rstrip('/')}/query",
        {"where": "1=1", "returnIdsOnly": "true", "f": "json"},
        "POST",
    )
    object_ids = ids_payload.get("objectIds") or []
    if not isinstance(object_ids, list) or not object_ids:
        raise RuntimeError(f"ArcGIS layer contains no downloadable features: {service_url}")

    page_size = max(1, min(int(layer_metadata.get("maxRecordCount") or 1000), 2000))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    partial_path = output_path.with_suffix(".partial.gpkg")
    if partial_path.exists():
        partial_path.unlink()
    if output_path.exists() and overwrite:
        output_path.unlink()

    try:
        with tempfile.TemporaryDirectory(prefix="arcgis-pages-", dir=output_path.parent) as temp_dir:
            for page_number, object_id_page in enumerate(
                _chunks(object_ids, page_size), start=1
            ):
                payload = requester(
                    f"{service_url.rstrip('/')}/query",
                    {
                        "objectIds": ",".join(str(value) for value in object_id_page),
                        "outFields": "*",
                        "returnGeometry": "true",
                        "outSR": "4326",
                        "f": "geojson",
                    },
                    "POST",
                )
                features = payload.get("features")
                if not isinstance(features, list):
                    raise RuntimeError(f"ArcGIS query did not return GeoJSON features: {service_url}")
                page_path = Path(temp_dir) / f"page-{page_number:05d}.geojson"
                page_path.write_text(json.dumps(payload), encoding="utf-8")

                if page_number == 1:
                    command = [
                        ogr2ogr,
                        "-f",
                        "GPKG",
                        str(partial_path),
                        str(page_path),
                        "-nln",
                        layer_name,
                        "-t_srs",
                        output_crs,
                        "-nlt",
                        "PROMOTE_TO_MULTI",
                        "-lco",
                        "SPATIAL_INDEX=YES",
                    ]
                else:
                    command = [
                        ogr2ogr,
                        "-f",
                        "GPKG",
                        "-update",
                        "-append",
                        str(partial_path),
                        str(page_path),
                        "-nln",
                        layer_name,
                        "-t_srs",
                        output_crs,
                        "-nlt",
                        "PROMOTE_TO_MULTI",
                    ]
                _run_command(command)
        partial_path.replace(output_path)
    except Exception:
        if partial_path.exists():
            partial_path.unlink()
        raise

    return {
        "feature_count": len(object_ids),
        "geometry_type": str(layer_metadata.get("geometryType", "")),
        "output": str(output_path),
    }


def run_download_stage(
    job: JobConfig,
    *,
    requester: JsonRequester = default_request_json,
    overwrite: bool = False,
) -> list[dict[str, Any]]:
    manifest = require_confirmed_review(job)
    results: list[dict[str, Any]] = []
    for record in manifest["records"]:
        output_path = job.gpkg_path(record["filename"])
        if output_path.is_file() and not overwrite:
            LOGGER.info("Skipping existing GeoPackage: %s", output_path)
            results.append(
                {
                    "status": "skipped_existing",
                    "output": str(output_path),
                }
            )
            continue
        try:
            result = download_service_geopackage(
                record["service_url"],
                output_path,
                Path(record["filename"]).stem,
                job.crs_authority,
                requester=requester,
                overwrite=overwrite,
            )
        except Exception as exc:
            LOGGER.error(
                "Skipping failed ArcGIS download for %s (%s): %s",
                record["filename"],
                record["service_url"],
                exc,
            )
            results.append(
                {
                    "status": "failed",
                    "source_id": record["source_id"],
                    "filename": record["filename"],
                    "service_url": record["service_url"],
                    "output": str(output_path),
                    "error": str(exc),
                }
            )
            continue
        result["status"] = "downloaded"
        results.append(result)
    failed_count = sum(result["status"] == "failed" for result in results)
    stage_status = "completed_with_errors" if failed_count else "completed"
    mark_stage(
        job,
        "download",
        details={
            "status": stage_status,
            "failed_count": failed_count,
            "outputs": results,
        },
    )
    if failed_count:
        LOGGER.warning(
            "Skipped %s ArcGIS dataset(s) that could not be downloaded; "
            "place their GeoPackages at the recorded output paths and rerun the stage",
            failed_count,
        )
    return results


def _format_bbox(values: tuple[float, float, float, float]) -> str:
    return ",".join(f"{value:.4f}" for value in values)


def _bbox_geometry(values: tuple[float, float, float, float]) -> str:
    west, south, east, north = values
    return (
        f"POLYGON(({west:.4f} {north:.4f}, {east:.4f} {north:.4f}, "
        f"{east:.4f} {south:.4f}, {west:.4f} {south:.4f}, "
        f"{west:.4f} {north:.4f}))"
    )


def inspect_geopackage(path: Path) -> dict[str, Any]:
    """Read geometry, WGS84 bounds, and field schema from a local GeoPackage."""
    with fiona.open(path) as collection:
        feature_count = len(collection)
        geometry_type = str(collection.schema.get("geometry", ""))
        fields = [
            {"name": name, "type": field_type}
            for name, field_type in collection.schema.get("properties", {}).items()
        ]
        source_crs = collection.crs_wkt or collection.crs
        bounds = tuple(float(value) for value in collection.bounds)
        normalized_geometry = geometry_type.removeprefix("3D ").casefold()
        resource_type = GEOPACKAGE_GEOMETRY_RESOURCE_TYPES.get(normalized_geometry)
        if not resource_type:
            feature_geometry_types = {
                str(feature["geometry"].get("type", ""))
                for feature in collection
                if feature.get("geometry")
            }
            inferred_resource_types = {
                GEOPACKAGE_GEOMETRY_RESOURCE_TYPES.get(
                    value.removeprefix("3D ").casefold()
                )
                for value in feature_geometry_types
            }
            if None not in inferred_resource_types and len(inferred_resource_types) == 1:
                resource_type = next(iter(inferred_resource_types))
                geometry_type = ", ".join(sorted(feature_geometry_types))
    if feature_count < 1:
        raise RuntimeError(f"GeoPackage contains no features: {path}")
    if not source_crs:
        raise RuntimeError(f"GeoPackage has no coordinate reference system: {path}")
    if not resource_type:
        raise RuntimeError(f"Unsupported GeoPackage geometry type {geometry_type!r}: {path}")
    wgs84_bounds = transform_bounds(source_crs, "EPSG:4326", *bounds, densify_pts=21)
    return {
        "feature_count": feature_count,
        "geometry_type": geometry_type,
        "resource_type": resource_type,
        "bounds": tuple(float(value) for value in wgs84_bounds),
        "fields": fields,
    }


def run_enrich_stage(
    job: JobConfig,
    *,
    requester: JsonRequester = default_request_json,
) -> None:
    manifest = require_confirmed_review(job)
    dataframe = validate_reviewed_metadata(job)
    dataframe = dataframe.set_index("filename", drop=False)
    details = []
    for record in manifest["records"]:
        gpkg_path = job.gpkg_path(record["filename"])
        if not gpkg_path.is_file():
            raise RuntimeError(f"GeoPackage is missing; run download first: {gpkg_path}")
        inspection = inspect_geopackage(gpkg_path)
        bbox = inspection["bounds"]
        resource_type = inspection["resource_type"]
        if resource_type not in job.allowed_resource_types:
            raise RuntimeError(
                f"Derived resource type {resource_type!r} is excluded by YAML selection criteria"
            )
        dataframe.loc[record["filename"], "Resource Type"] = resource_type
        dataframe.loc[record["filename"], "Bounding Box"] = _format_bbox(bbox)
        dataframe.loc[record["filename"], "Geometry"] = _bbox_geometry(bbox)
        dataframe.loc[record["filename"], "Centroid"] = (
            f"{(bbox[1] + bbox[3]) / 2:.4f},{(bbox[0] + bbox[2]) / 2:.4f}"
        )
        details.append(
            {
                "filename": record["filename"],
                "metadata_source": "geopackage",
                "feature_count": inspection["feature_count"],
                "resource_type": resource_type,
                "bounding_box": _format_bbox(bbox),
            }
        )
    write_metadata_csv(dataframe.reset_index(drop=True), job.metadata_path)
    refresh_review_checksum(job)
    mark_stage(job, "enrich", details={"records": details})


def _domain_values(field_value: dict[str, Any]) -> str:
    domain = field_value.get("domain") or {}
    coded_values = domain.get("codedValues") if isinstance(domain, dict) else None
    if not isinstance(coded_values, list):
        return ""
    values = []
    for coded_value in coded_values:
        if isinstance(coded_value, dict):
            values.append(f"{coded_value.get('code', '')}: {coded_value.get('name', '')}".strip())
    return "|".join(value for value in values if value)


def run_dictionary_stage(
    job: JobConfig,
    *,
    requester: JsonRequester = default_request_json,
) -> None:
    manifest = require_confirmed_review(job)
    metadata = validate_reviewed_metadata(job).set_index("filename")
    outputs: list[str] = []
    details: list[dict[str, Any]] = []
    for record in manifest["records"]:
        metadata_source = "arcgis_rest"
        fallback_error = ""
        try:
            layer_metadata = requester(record["service_url"], {"f": "pjson"}, "GET")
            fields = layer_metadata.get("fields") or []
            if not isinstance(fields, list) or not fields:
                raise RuntimeError(
                    f"ArcGIS fields are missing or invalid: {record['service_url']}"
                )
        except Exception as exc:
            gpkg_path = job.gpkg_path(record["filename"])
            if not gpkg_path.is_file():
                raise RuntimeError(
                    "Could not load the ArcGIS field definition and no local "
                    f"GeoPackage is available: {record['filename']}"
                ) from exc
            inspection = inspect_geopackage(gpkg_path)
            fields = inspection["fields"]
            metadata_source = "geopackage_schema"
            fallback_error = str(exc)
            LOGGER.warning(
                "Could not load ArcGIS field definitions for %s; using the local "
                "GeoPackage schema without aliases or coded-value domains: %s",
                record["filename"],
                exc,
            )
        output_path = job.dictionary_path(record["filename"])
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=DICTIONARY_COLUMNS)
            writer.writeheader()
            for position, field_value in enumerate(fields, start=1):
                if not isinstance(field_value, dict):
                    continue
                writer.writerow(
                    {
                        "friendlier_id": metadata.loc[record["filename"], "ID"],
                        "field_name": field_value.get("name", ""),
                        "field_type": field_value.get("type", ""),
                        "values": _domain_values(field_value),
                        "definition": field_value.get("alias", ""),
                        "definition_source": (
                            record["service_url"]
                            if metadata_source == "arcgis_rest"
                            else ""
                        ),
                        "parent_field_name": "",
                        "position": position,
                    }
                )
        outputs.append(str(output_path))
        details.append(
            {
                "filename": record["filename"],
                "output": str(output_path),
                "metadata_source": metadata_source,
                **({"fallback_error": fallback_error} if fallback_error else {}),
            }
        )
    fallback_count = sum(
        detail["metadata_source"] == "geopackage_schema" for detail in details
    )
    mark_stage(
        job,
        "dictionaries",
        details={
            "fallback_count": fallback_count,
            "outputs": outputs,
            "records": details,
        },
    )


def run_embed_stage(job: JobConfig) -> None:
    require_confirmed_review(job)
    validate_enriched_metadata(job)
    expected = {record.filename for record in job.records}
    missing = sorted(
        record.filename
        for record in job.records
        if not job.gpkg_path(record.filename).is_file()
    )
    if missing:
        raise RuntimeError(f"GeoPackages are missing before metadata embedding: {', '.join(missing)}")
    summary = embed_metadata_directory(
        job.work_dir,
        job.metadata_path,
        get_default_template_path(),
        match_column="filename",
    )
    processed = set(summary.processed_files)
    if not expected.issubset(processed):
        raise RuntimeError(
            f"Metadata was not embedded in every selected GeoPackage: "
            f"{sorted(expected - processed)}"
        )
    outputs = [
        str(job.gpkg_path(record.filename))
        for record in job.records
    ]
    mark_stage(job, "embed", details={"outputs": outputs})


def run_thumbnail_stage(job: JobConfig) -> None:
    require_confirmed_review(job)
    outputs = []
    for record in job.records:
        gpkg_path = job.gpkg_path(record.filename)
        if not gpkg_path.is_file():
            raise RuntimeError(f"GeoPackage is missing before thumbnail creation: {gpkg_path}")
        thumbnail_path = job.thumbnail_path(record.filename)
        thumbnail_path.parent.mkdir(parents=True, exist_ok=True)
        create_vector_thumbnail(gpkg_path, thumbnail_path)
        outputs.append(str(thumbnail_path))
    mark_stage(job, "thumbnails", details={"outputs": outputs})


def run_derivatives_stage(job: JobConfig, *, overwrite: bool = False) -> None:
    require_confirmed_review(job)
    script_path = REPO_ROOT / "curation" / "scripts" / "build_pmtiles_from_gpkg.py"
    report_path = job.report_dir / "pmtiles_build_report.csv"
    command = [
        sys.executable,
        str(script_path),
        "--input-dir",
        str(job.work_dir),
        "--fgb-dir",
        str(job.work_dir),
        "--pmtiles-dir",
        str(job.work_dir),
        "--report",
        str(report_path),
        "--resource-layout",
    ]
    if job.pmtiles_config:
        command.extend(["--config", str(job.pmtiles_config)])
    command.append("--overwrite" if overwrite else "--skip-existing")
    _run_command(command)
    mark_stage(job, "derivatives", details={"report": str(report_path)})


def run_zip_stage(job: JobConfig, *, overwrite: bool = False) -> None:
    """Create one upload-ready ZIP archive for each selected GeoPackage."""
    require_confirmed_review(job)
    outputs: list[str] = []
    for record in job.records:
        gpkg_path = job.gpkg_path(record.filename)
        if not gpkg_path.is_file():
            raise RuntimeError(f"GeoPackage is missing before ZIP creation: {gpkg_path}")
        result = zip_one_geopackage(
            gpkg_path,
            gpkg_path.parent,
            gpkg_path.parent,
            overwrite=overwrite,
            delete_original=False,
        )
        archive_path = gpkg_path.with_name(f"{gpkg_path.name}.zip")
        if not archive_path.is_file():
            raise RuntimeError(f"GeoPackage ZIP was not created: {archive_path}")
        outputs.append(str(archive_path))
        if result is None:
            LOGGER.info("Using existing GeoPackage ZIP: %s", archive_path)
    mark_stage(job, "zip", details={"outputs": outputs})


def run_postprocess(
    job: JobConfig,
    *,
    requester: JsonRequester = default_request_json,
    overwrite: bool = False,
) -> None:
    """Run all automated stages after the manual metadata review checkpoint."""
    require_confirmed_review(job)
    LOGGER.info("Postprocess 1/7: downloading GeoPackages")
    download_results = run_download_stage(
        job,
        requester=requester,
        overwrite=overwrite,
    )
    failed_downloads = [
        result for result in download_results if result["status"] == "failed"
    ]
    if failed_downloads:
        failed_filenames = ", ".join(
            result["filename"] for result in failed_downloads
        )
        raise RuntimeError(
            "Postprocess paused after attempting every download. Add the skipped "
            f"GeoPackages manually, then rerun postprocess: {failed_filenames}"
        )
    LOGGER.info("Postprocess 2/7: enriching metadata")
    run_enrich_stage(job, requester=requester)
    LOGGER.info("Postprocess 3/7: creating data dictionaries")
    run_dictionary_stage(job, requester=requester)
    LOGGER.info("Postprocess 4/7: embedding GeoPackage metadata")
    run_embed_stage(job)
    LOGGER.info("Postprocess 5/7: creating thumbnails")
    run_thumbnail_stage(job)
    LOGGER.info("Postprocess 6/7: creating FlatGeoBuf and PMTiles derivatives")
    run_derivatives_stage(job, overwrite=overwrite)
    LOGGER.info("Postprocess 7/7: zipping GeoPackages for upload")
    run_zip_stage(job, overwrite=overwrite)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path, help="ArcGIS curation job YAML")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("validate", help="Validate YAML inputs only")
    subparsers.add_parser("metadata", help="Harvest selected DCAT metadata and pause")
    review_parser = subparsers.add_parser("review", help="Record completion of manual CSV review")
    review_parser.add_argument("--confirm", action="store_true")
    for command_name in ("download", "postprocess", "derivatives", "zip"):
        command_parser = subparsers.add_parser(command_name)
        command_parser.add_argument("--overwrite", action="store_true")
    subparsers.add_parser("enrich")
    subparsers.add_parser("dictionaries")
    subparsers.add_parser("embed")
    subparsers.add_parser("thumbnails")
    subparsers.add_parser(
        "snapshot",
        help="Save a portable run record with metadata and artifact checksums",
    )
    subparsers.add_parser("status")
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = build_parser().parse_args(argv)
    try:
        job = load_job_config(args.config)

        if args.command == "validate":
            mark_validation_stage(job)
            LOGGER.info(
                "Valid curation job %s with %s record(s)", job.job_id, len(job.records)
            )
        elif args.command == "metadata":
            path = run_metadata_stage(job)
            LOGGER.info("Metadata is ready for manual review: %s", path)
        elif args.command == "review":
            confirm_manual_review(job, confirmed=args.confirm)
            LOGGER.info("Manual review recorded for %s", job.metadata_path)
        elif args.command == "download":
            run_download_stage(job, overwrite=args.overwrite)
        elif args.command == "enrich":
            run_enrich_stage(job)
        elif args.command == "dictionaries":
            run_dictionary_stage(job)
        elif args.command == "embed":
            run_embed_stage(job)
        elif args.command == "thumbnails":
            run_thumbnail_stage(job)
        elif args.command == "derivatives":
            run_derivatives_stage(job, overwrite=args.overwrite)
        elif args.command == "zip":
            run_zip_stage(job, overwrite=args.overwrite)
        elif args.command == "postprocess":
            run_postprocess(job, overwrite=args.overwrite)
        elif args.command == "snapshot":
            path = save_run_record(job)
            LOGGER.info("Saved portable run record: %s", path)
        elif args.command == "status":
            print(json.dumps(load_manifest(job), indent=2))
    except (CurationConfigError, RuntimeError, OSError) as exc:
        LOGGER.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
