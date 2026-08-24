"""Staged Socrata curation pipeline for selected geospatial datasets.

The metadata stage reuses the Socrata harvester's dataframe transformations.
Data is downloaded through deterministic, explicitly sized SODA2 GeoJSON
pages so datasets larger than Socrata's default 1,000-row response are complete.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import date
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

from harvesters.base import BaseHarvester  # noqa: E402
from harvesters.socrata import SocrataHarvester  # noqa: E402
from utils.field_order import PRIMARY_FIELD_ORDER  # noqa: E402

from curation.arcgis_curation_pipeline import (  # noqa: E402
    CurationConfigError,
    DEFAULT_REQUIRED_REVIEW_FIELDS,
    DICTIONARY_COLUMNS,
    apply_historical_title_and_description,
    archive_display_note,
    assign_curated_ids,
    confirm_manual_review,
    file_sha256,
    formatted_export_date,
    load_manifest,
    load_theme_map,
    load_website_defaults,
    mark_stage,
    mark_validation_stage,
    refresh_review_checksum,
    require_confirmed_review,
    run_derivatives_stage,
    run_embed_stage,
    run_thumbnail_stage,
    run_zip_stage,
    save_run_record,
    utc_now,
    validate_reviewed_metadata,
    write_manifest,
    write_metadata_csv,
)


LOGGER = logging.getLogger(__name__)
SOCRATA_ID_RE = re.compile(r"^[a-z0-9]{4}-[a-z0-9]{4}$", re.IGNORECASE)
SOCRATA_GEOMETRY_RESOURCE_TYPES = {
    "point": "Point data",
    "multipoint": "Point data",
    "location": "Point data",
    "line": "Line data",
    "linestring": "Line data",
    "multiline": "Line data",
    "multilinestring": "Line data",
    "polygon": "Polygon data",
    "multipolygon": "Polygon data",
}
DEFAULT_PAGE_SIZE = 10_000
MAX_SODA2_PAGE_SIZE = 50_000


@dataclass(frozen=True)
class RecordSpec:
    """One selected Socrata dataset and its curated output filename."""

    source_id: str
    filename_theme: str
    filename_stem: str
    basic_theme: str = ""
    temporal_year: str = ""

    @property
    def filename(self) -> str:
        return f"{self.filename_stem}.gpkg"


@dataclass(frozen=True)
class JobConfig:
    """Validated Socrata curation job configuration."""

    config_path: Path
    job_id: str
    work_dir: Path
    hub_name: str
    hub_landing_page: str
    dcat_api: str
    soda_api_base: str
    website_reference_id: str
    websites_csv: Path
    crs_authority: str
    crs_uri: str
    provider: str
    code: str
    member_of: str
    export_date: date
    city_abbreviation: str
    download_year: str
    records: tuple[RecordSpec, ...]
    page_size: int = DEFAULT_PAGE_SIZE
    app_token_environment: str = ""
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

    @property
    def request_headers(self) -> dict[str, str]:
        if not self.app_token_environment:
            return {}
        token = os.environ.get(self.app_token_environment, "").strip()
        return {"X-App-Token": token} if token else {}

    def metadata_url(self, source_id: str) -> str:
        return f"{self.soda_api_base}/api/views/{source_id}"

    def geojson_url(self, source_id: str) -> str:
        return f"{self.soda_api_base}/resource/{source_id}.geojson"


JsonRequester = Callable[
    [str, dict[str, Any] | None, dict[str, str] | None],
    Any,
]


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


def _http_url(value: str, label: str) -> str:
    parsed = urlsplit(value.strip())
    if (
        parsed.scheme.casefold() not in {"http", "https"}
        or not parsed.netloc
        or parsed.fragment
    ):
        raise CurationConfigError(f"{label} must be an HTTP(S) URL")
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), parsed.query, ""))


def _http_origin(value: str, label: str) -> str:
    parsed = urlsplit(value.strip())
    if (
        parsed.scheme.casefold() not in {"http", "https"}
        or not parsed.netloc
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
    ):
        raise CurationConfigError(f"{label} must be an HTTP(S) origin without a path")
    return urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))


def load_job_config(config_path: Path | str) -> JobConfig:
    """Load and validate a Socrata curation YAML file."""
    path = Path(config_path).expanduser().resolve()
    with path.open(encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}

    if raw.get("version") != 1:
        raise CurationConfigError("version must be 1")

    job_raw = _mapping(raw.get("job"), "job")
    hub_raw = _mapping(raw.get("hub"), "hub")
    crs_raw = _mapping(raw.get("coordinate_reference_system"), "coordinate_reference_system")
    metadata_raw = _mapping(raw.get("metadata"), "metadata")
    file_naming_raw = _mapping(raw.get("file_naming"), "file_naming")
    selection_raw = _mapping(raw.get("selection_criteria", {}), "selection_criteria")
    review_raw = _mapping(raw.get("manual_review", {}), "manual_review")
    derivatives_raw = _mapping(raw.get("derivatives", {}), "derivatives")
    download_raw = _mapping(raw.get("download", {}), "download")

    export_date_text = _required_text(metadata_raw, "export_date", "metadata")
    try:
        export_date = date.fromisoformat(export_date_text)
    except ValueError as exc:
        raise CurationConfigError("metadata.export_date must use YYYY-MM-DD") from exc

    city_abbreviation = _required_text(
        file_naming_raw,
        "city_abbreviation",
        "file_naming",
    )
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]*", city_abbreviation):
        raise CurationConfigError(
            "file_naming.city_abbreviation must contain only letters, numbers, or hyphens"
        )
    download_year = _required_string(
        file_naming_raw,
        "download_year",
        "file_naming",
    )
    if not re.fullmatch(r"(?:19|20)\d{2}", download_year):
        raise CurationConfigError(
            "file_naming.download_year must be a quoted four-digit year"
        )
    if download_year != str(export_date.year):
        raise CurationConfigError(
            "file_naming.download_year must match the year in metadata.export_date"
        )

    records_raw = raw.get("records")
    if not isinstance(records_raw, list) or not records_raw:
        raise CurationConfigError("records must be a non-empty list")

    records: list[RecordSpec] = []
    seen_ids: set[str] = set()
    seen_filenames: set[str] = set()
    for index, record_value in enumerate(records_raw, start=1):
        record_raw = _mapping(record_value, f"records[{index}]")
        source_id = _required_text(record_raw, "id", f"records[{index}]").casefold()
        if not SOCRATA_ID_RE.fullmatch(source_id):
            raise CurationConfigError(
                f"records[{index}].id must be a Socrata identifier such as abcd-1234"
            )
        filename_theme = _required_text(
            record_raw,
            "filename_theme",
            f"records[{index}]",
        )
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", filename_theme):
            raise CurationConfigError(
                f"records[{index}].filename_theme must contain only letters, "
                "numbers, underscores, or hyphens"
            )
        filename_stem = f"{city_abbreviation}_{filename_theme}_{download_year}"
        basic_theme = str(record_raw.get("basic_theme", "")).strip()
        temporal_year = str(record_raw.get("temporal_year", "")).strip()
        if temporal_year and not re.fullmatch(r"(?:19|20)\d{2}", temporal_year):
            raise CurationConfigError(
                f"records[{index}].temporal_year must be a four-digit year"
            )
        if source_id in seen_ids:
            raise CurationConfigError(f"Duplicate record id: {source_id}")
        if filename_stem.casefold() in seen_filenames:
            raise CurationConfigError(f"Duplicate output filename: {filename_stem}.gpkg")
        seen_ids.add(source_id)
        seen_filenames.add(filename_stem.casefold())
        records.append(
            RecordSpec(
                source_id=source_id,
                filename_theme=filename_theme,
                filename_stem=filename_stem,
                basic_theme=basic_theme,
                temporal_year=temporal_year,
            )
        )

    allowed_types_value = selection_raw.get(
        "allowed_resource_types",
        ["Polygon data", "Line data", "Point data"],
    )
    known_vector_types = set(SOCRATA_GEOMETRY_RESOURCE_TYPES.values())
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

    job_id = _required_text(job_raw, "id", "job")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", job_id):
        raise CurationConfigError(
            "job.id must start with a letter or number and contain only letters, "
            "numbers, periods, underscores, or hyphens"
        )

    try:
        page_size = int(download_raw.get("page_size", DEFAULT_PAGE_SIZE))
    except (TypeError, ValueError) as exc:
        raise CurationConfigError("download.page_size must be an integer") from exc
    if not 1 <= page_size <= MAX_SODA2_PAGE_SIZE:
        raise CurationConfigError(
            f"download.page_size must be between 1 and {MAX_SODA2_PAGE_SIZE}"
        )
    app_token_environment = str(
        download_raw.get("app_token_environment", "")
    ).strip()
    if app_token_environment and not re.fullmatch(
        r"[A-Za-z_][A-Za-z0-9_]*",
        app_token_environment,
    ):
        raise CurationConfigError(
            "download.app_token_environment must be an environment-variable name"
        )

    landing_page = _http_url(
        _required_text(hub_raw, "landing_page", "hub"),
        "hub.landing_page",
    )
    dcat_api = _http_url(
        _required_text(hub_raw, "dcat_api", "hub"),
        "hub.dcat_api",
    )
    default_soda_api_base = urlunsplit(
        (urlsplit(dcat_api).scheme, urlsplit(dcat_api).netloc, "", "", "")
    )
    soda_api_base = _http_origin(
        str(hub_raw.get("soda_api_base", default_soda_api_base)),
        "hub.soda_api_base",
    )

    return JobConfig(
        config_path=path,
        job_id=job_id,
        work_dir=_resolve_path(_required_text(job_raw, "work_directory", "job"), path),
        hub_name=_required_text(hub_raw, "name", "hub"),
        hub_landing_page=landing_page,
        dcat_api=dcat_api,
        soda_api_base=soda_api_base,
        website_reference_id=_required_text(hub_raw, "website_reference_id", "hub"),
        websites_csv=websites_csv,
        crs_authority=_required_text(crs_raw, "authority", "coordinate_reference_system"),
        crs_uri=_required_text(crs_raw, "uri", "coordinate_reference_system"),
        provider=str(raw.get("provider", "BTAA-GIN")).strip() or "BTAA-GIN",
        code=_required_string(metadata_raw, "code", "metadata"),
        member_of=_required_text(metadata_raw, "member_of", "metadata"),
        export_date=export_date,
        city_abbreviation=city_abbreviation,
        download_year=download_year,
        records=tuple(records),
        page_size=page_size,
        app_token_environment=app_token_environment,
        allowed_resource_types=allowed_resource_types,
        required_review_fields=tuple(value.strip() for value in required_fields_value),
        pmtiles_config=pmtiles_config,
    )


def default_request_json(
    url: str,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    *,
    timeout: int = 120,
) -> Any:
    """Fetch a Socrata JSON response with an identifying user agent."""
    request_headers = {"User-Agent": "BTAA-GIN Socrata curation pipeline/1.0"}
    request_headers.update(headers or {})
    response = requests.get(
        url,
        params=params or {},
        headers=request_headers,
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def normalized_catalog_id(resource: dict[str, Any]) -> str:
    """Extract a four-by-four Socrata identifier from a DCAT dataset."""
    identifier = str(resource.get("identifier", "")).strip()
    parsed_path = urlsplit(identifier).path
    for marker in ("/views/", "/d/"):
        if marker in parsed_path:
            candidate = parsed_path.split(marker, 1)[1].split("/", 1)[0]
            return candidate.casefold() if SOCRATA_ID_RE.fullmatch(candidate) else ""
    candidate = identifier.rstrip("/").rsplit("/", 1)[-1]
    return candidate.casefold() if SOCRATA_ID_RE.fullmatch(candidate) else ""


def select_catalog_records(
    catalog: dict[str, Any],
    records: Iterable[RecordSpec],
) -> list[tuple[RecordSpec, dict[str, Any]]]:
    """Match configured Socrata IDs to DCAT datasets in configuration order."""
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
        raise RuntimeError(
            "Selected Socrata IDs not found in DCAT catalog: " + ", ".join(missing)
        )
    return selected


def socrata_geometry_columns(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    columns = metadata.get("columns", [])
    if not isinstance(columns, list):
        raise RuntimeError("Socrata view metadata does not contain a columns list")
    return [
        column
        for column in columns
        if isinstance(column, dict)
        and str(column.get("dataTypeName", "")).casefold()
        in SOCRATA_GEOMETRY_RESOURCE_TYPES
    ]


def validate_socrata_metadata(source_id: str, metadata: Any, metadata_url: str) -> dict[str, Any]:
    if not isinstance(metadata, dict):
        raise RuntimeError(f"Expected a metadata object from {metadata_url}")
    returned_id = str(metadata.get("id", "")).casefold()
    if returned_id and returned_id != source_id:
        raise RuntimeError(
            f"Socrata metadata ID mismatch: expected {source_id}; found {returned_id}"
        )
    geometry_columns = socrata_geometry_columns(metadata)
    if not geometry_columns:
        raise RuntimeError(
            f"Selected Socrata dataset has no supported geometry column: {source_id}"
        )
    resource_types = {
        SOCRATA_GEOMETRY_RESOURCE_TYPES[
            str(column.get("dataTypeName", "")).casefold()
        ]
        for column in geometry_columns
    }
    if len(resource_types) != 1:
        raise RuntimeError(
            f"Selected Socrata dataset has mixed geometry types: {source_id}"
        )
    return metadata


def count_socrata_rows(
    api_base: str,
    source_id: str,
    *,
    requester: JsonRequester = default_request_json,
    headers: dict[str, str] | None = None,
) -> int:
    """Return the exact row count through a small SODA2 aggregate query."""
    payload = requester(
        f"{api_base.rstrip('/')}/resource/{source_id}.json",
        {"$select": "count(*)"},
        headers,
    )
    try:
        value = payload[0]["count"]
        count = int(value)
    except (IndexError, KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(f"Socrata count query failed for {source_id}: {payload!r}") from exc
    if count < 1:
        raise RuntimeError(f"Selected Socrata dataset contains no rows: {source_id}")
    return count


def resolve_selected_records(
    job: JobConfig,
    *,
    requester: JsonRequester = default_request_json,
    catalog: dict[str, Any] | None = None,
) -> list[tuple[RecordSpec, dict[str, Any], dict[str, Any]]]:
    """Resolve selected DCAT records and verify their Socrata schemas."""
    catalog_payload = (
        catalog
        if catalog is not None
        else requester(job.dcat_api, None, job.request_headers)
    )
    if not isinstance(catalog_payload, dict):
        raise RuntimeError(f"Expected a DCAT object from {job.dcat_api}")
    selected = select_catalog_records(catalog_payload, job.records)
    resolved: list[tuple[RecordSpec, dict[str, Any], dict[str, Any]]] = []
    for record, resource in selected:
        metadata_url = job.metadata_url(record.source_id)
        metadata = validate_socrata_metadata(
            record.source_id,
            requester(metadata_url, None, job.request_headers),
            metadata_url,
        )
        metadata["_curation_row_count"] = count_socrata_rows(
            job.soda_api_base,
            record.source_id,
            requester=requester,
            headers=job.request_headers,
        )
        resolved.append((record, resource, metadata))
    return resolved


def build_metadata_dataframe(
    job: JobConfig,
    selected: list[tuple[RecordSpec, dict[str, Any], dict[str, Any]]],
    curated_ids: dict[str, str],
) -> pd.DataFrame:
    """Apply Socrata harvester rules, followed by curation-specific exceptions."""
    website_defaults = load_website_defaults(job)
    workflow = {
        "Endpoint URL": job.dcat_api,
        "Website Platform": "Socrata",
        "Endpoint Description": "DCAT API",
        "Accrual Method": "Manual curation",
        "Harvest Workflow": "Manual curation",
    }
    harvester = SocrataHarvester(
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
    for record, resource, _ in selected:
        flattened_record = pd.DataFrame(
            [
                {
                    "workflow": workflow,
                    "hub_defaults": website_defaults,
                    "resource": resource,
                }
            ]
        )
        mapped = harvester.build_dataframe(flattened_record)
        if mapped.empty:
            rejected.append(record.source_id)
        else:
            mapped_records.append(mapped)
    if rejected:
        raise RuntimeError(
            "Selected records did not pass the Socrata harvester's GIS filter: "
            + ", ".join(rejected)
        )
    dataframe = pd.concat(mapped_records, ignore_index=True)

    dataframe = harvester.derive_fields(dataframe)
    dataframe = harvester.add_defaults(dataframe)
    dataframe = BaseHarvester.add_provenance(harvester, dataframe)
    source_ids = pd.Series(
        [record.source_id for record, _, _ in selected],
        index=dataframe.index,
        dtype=str,
    )
    row_counts = {
        record.source_id: int(metadata["_curation_row_count"])
        for record, _, metadata in selected
    }
    geojson_urls = {
        record.source_id: job.geojson_url(record.source_id)
        for record, _, _ in selected
    }
    dataframe["Provenance"] = source_ids.map(
        {
            source_id: (
                f"Exported from {geojson_url} as GeoPackage on "
                f"{formatted_export_date(job.export_date)}. The source reported "
                f"{row_counts[source_id]} rows before download."
            )
            for source_id, geojson_url in geojson_urls.items()
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
    dataframe = apply_historical_title_and_description(
        dataframe,
        [(record, resource) for record, resource, _ in selected],
        source_ids,
    )
    dataframe = harvester.clean(dataframe)
    harvester.validate(dataframe)

    filenames = {record.source_id: record.filename for record, _, _ in selected}
    dataframe.insert(0, "filename", source_ids.map(filenames).fillna(""))
    return dataframe.reindex(columns=["filename", *PRIMARY_FIELD_ORDER], fill_value="")


def build_manifest(
    job: JobConfig,
    selected: list[tuple[RecordSpec, dict[str, Any], dict[str, Any]]],
    curated_ids: dict[str, str],
) -> dict[str, Any]:
    completed_at = utc_now()
    return {
        "version": 1,
        "source_platform": "socrata",
        "job_id": job.job_id,
        "config_path": str(job.config_path),
        "work_directory": str(job.work_dir),
        "metadata_path": str(job.metadata_path),
        "file_naming": {
            "city_abbreviation": job.city_abbreviation,
            "download_year": job.download_year,
        },
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
                "filename_theme": record.filename_theme,
                "curated_id": curated_ids[record.source_id],
                "filename": record.filename,
                "landing_page": str(resource.get("landingPage", "")).strip(),
                "metadata_url": job.metadata_url(record.source_id),
                "geojson_url": job.geojson_url(record.source_id),
                "row_count": int(metadata["_curation_row_count"]),
                "source_revision": metadata.get("rowsUpdatedAt"),
                "geometry_columns": [
                    {
                        "field_name": column.get("fieldName", ""),
                        "field_type": column.get("dataTypeName", ""),
                    }
                    for column in socrata_geometry_columns(metadata)
                ],
            }
            for record, resource, metadata in selected
        ],
    }


def run_metadata_stage(
    job: JobConfig,
    *,
    requester: JsonRequester = default_request_json,
    catalog: dict[str, Any] | None = None,
) -> Path:
    """Resolve selected Socrata records and write the review metadata CSV."""
    selected = resolve_selected_records(job, requester=requester, catalog=catalog)
    curated_ids = assign_curated_ids(job)
    dataframe = build_metadata_dataframe(job, selected, curated_ids)
    write_metadata_csv(dataframe, job.metadata_path)
    write_manifest(job, build_manifest(job, selected, curated_ids))
    return job.metadata_path


def _run_command(command: list[str]) -> None:
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"Command failed ({completed.returncode}): {' '.join(command)}\n"
            f"{completed.stderr.strip()}"
        )


def _source_revision(
    metadata_url: str,
    *,
    requester: JsonRequester,
    headers: dict[str, str] | None,
) -> Any:
    payload = requester(metadata_url, None, headers)
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected a metadata object from {metadata_url}")
    return payload.get("rowsUpdatedAt")


def download_socrata_geopackage(
    api_base: str,
    source_id: str,
    output_path: Path,
    layer_name: str,
    output_crs: str,
    *,
    page_size: int = DEFAULT_PAGE_SIZE,
    expected_count: int | None = None,
    metadata_url: str = "",
    expected_revision: Any = None,
    requester: JsonRequester = default_request_json,
    headers: dict[str, str] | None = None,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Download ordered SODA2 GeoJSON pages and assemble a projected GeoPackage."""
    ogr2ogr = shutil.which("ogr2ogr")
    if not ogr2ogr:
        raise RuntimeError("ogr2ogr is required to download GeoPackages")
    if output_path.exists() and not overwrite:
        raise FileExistsError(f"GeoPackage already exists (use --overwrite): {output_path}")
    if not 1 <= page_size <= MAX_SODA2_PAGE_SIZE:
        raise ValueError(f"page_size must be between 1 and {MAX_SODA2_PAGE_SIZE}")

    if metadata_url and expected_revision is not None:
        current_revision = _source_revision(
            metadata_url,
            requester=requester,
            headers=headers,
        )
        if current_revision != expected_revision:
            raise RuntimeError(
                f"Socrata dataset {source_id} changed after the metadata stage; "
                "run metadata and review again before downloading"
            )

    initial_count = count_socrata_rows(
        api_base,
        source_id,
        requester=requester,
        headers=headers,
    )
    if expected_count is not None and initial_count != expected_count:
        raise RuntimeError(
            f"Socrata row count changed after metadata for {source_id}: "
            f"expected {expected_count}; found {initial_count}"
        )

    geojson_url = f"{api_base.rstrip('/')}/resource/{source_id}.geojson"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    partial_path = output_path.with_suffix(".partial.gpkg")
    if partial_path.exists():
        partial_path.unlink()
    if output_path.exists() and overwrite:
        output_path.unlink()

    downloaded = 0
    page_count = 0
    try:
        with tempfile.TemporaryDirectory(
            prefix="socrata-pages-",
            dir=output_path.parent,
        ) as temp_dir:
            while downloaded < initial_count:
                requested_count = min(page_size, initial_count - downloaded)
                payload = requester(
                    geojson_url,
                    {
                        "$limit": requested_count,
                        "$offset": downloaded,
                        "$order": ":id",
                    },
                    headers,
                )
                if not isinstance(payload, dict) or payload.get("type") != "FeatureCollection":
                    raise RuntimeError(
                        f"Socrata page is not a GeoJSON FeatureCollection: {geojson_url}"
                    )
                features = payload.get("features")
                if not isinstance(features, list):
                    raise RuntimeError(f"Socrata GeoJSON page has no features list: {geojson_url}")
                if len(features) != requested_count:
                    raise RuntimeError(
                        f"Socrata page for {source_id} returned {len(features)} rows at "
                        f"offset {downloaded}; expected {requested_count}"
                    )

                page_count += 1
                page_path = Path(temp_dir) / f"page-{page_count:05d}.geojson"
                page_path.write_text(json.dumps(payload), encoding="utf-8")
                if page_count == 1:
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
                        "-addfields",
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
                downloaded += len(features)
                LOGGER.info(
                    "Downloaded Socrata %s page %s: %s/%s rows",
                    source_id,
                    page_count,
                    downloaded,
                    initial_count,
                )

        final_count = count_socrata_rows(
            api_base,
            source_id,
            requester=requester,
            headers=headers,
        )
        if final_count != initial_count:
            raise RuntimeError(
                f"Socrata row count changed during download for {source_id}: "
                f"started at {initial_count}; ended at {final_count}"
            )
        if metadata_url and expected_revision is not None:
            final_revision = _source_revision(
                metadata_url,
                requester=requester,
                headers=headers,
            )
            if final_revision != expected_revision:
                raise RuntimeError(
                    f"Socrata dataset {source_id} changed during download; retry the "
                    "metadata, review, and download stages"
                )
        partial_path.replace(output_path)
    except Exception:
        if partial_path.exists():
            partial_path.unlink()
        raise

    return {
        "feature_count": downloaded,
        "page_count": page_count,
        "page_size": page_size,
        "source_url": geojson_url,
        "output": str(output_path),
    }


def run_download_stage(
    job: JobConfig,
    *,
    requester: JsonRequester = default_request_json,
    overwrite: bool = False,
) -> None:
    manifest = require_confirmed_review(job)
    results = []
    for record in manifest["records"]:
        output_path = job.gpkg_path(record["filename"])
        if output_path.is_file() and not overwrite:
            LOGGER.info("Skipping existing GeoPackage: %s", output_path)
            results.append({"status": "skipped_existing", "output": str(output_path)})
            continue
        result = download_socrata_geopackage(
            job.soda_api_base,
            record["source_id"],
            output_path,
            Path(record["filename"]).stem,
            job.crs_authority,
            page_size=job.page_size,
            expected_count=int(record["row_count"]),
            metadata_url=record["metadata_url"],
            expected_revision=record.get("source_revision"),
            requester=requester,
            headers=job.request_headers,
            overwrite=overwrite,
        )
        result["status"] = "downloaded"
        results.append(result)
    mark_stage(job, "download", details={"outputs": results})


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
    """Read feature count, geometry type, and WGS84 bounds from a GeoPackage."""
    with fiona.open(path) as collection:
        feature_count = len(collection)
        geometry_type = str(collection.schema.get("geometry", ""))
        source_crs = collection.crs_wkt or collection.crs
        bounds = tuple(float(value) for value in collection.bounds)
    if feature_count < 1:
        raise RuntimeError(f"GeoPackage contains no features: {path}")
    if not source_crs:
        raise RuntimeError(f"GeoPackage has no coordinate reference system: {path}")
    normalized_geometry = geometry_type.removeprefix("3D ").casefold()
    resource_type = SOCRATA_GEOMETRY_RESOURCE_TYPES.get(normalized_geometry)
    if not resource_type:
        raise RuntimeError(f"Unsupported GeoPackage geometry type {geometry_type!r}: {path}")
    wgs84_bounds = transform_bounds(source_crs, "EPSG:4326", *bounds, densify_pts=21)
    return {
        "feature_count": feature_count,
        "geometry_type": geometry_type,
        "resource_type": resource_type,
        "bounds": tuple(float(value) for value in wgs84_bounds),
    }


def run_enrich_stage(job: JobConfig) -> None:
    manifest = require_confirmed_review(job)
    dataframe = validate_reviewed_metadata(job).set_index("filename", drop=False)
    details = []
    for record in manifest["records"]:
        gpkg_path = job.gpkg_path(record["filename"])
        if not gpkg_path.is_file():
            raise RuntimeError(f"GeoPackage is missing; run download first: {gpkg_path}")
        inspection = inspect_geopackage(gpkg_path)
        if inspection["feature_count"] != int(record["row_count"]):
            raise RuntimeError(
                f"GeoPackage feature count differs from the source count for "
                f"{record['source_id']}: {inspection['feature_count']} != {record['row_count']}"
            )
        resource_type = inspection["resource_type"]
        if resource_type not in job.allowed_resource_types:
            raise RuntimeError(
                f"Derived resource type {resource_type!r} is excluded by YAML selection criteria"
            )
        bbox = inspection["bounds"]
        dataframe.loc[record["filename"], "Resource Type"] = resource_type
        dataframe.loc[record["filename"], "Bounding Box"] = _format_bbox(bbox)
        dataframe.loc[record["filename"], "Geometry"] = _bbox_geometry(bbox)
        dataframe.loc[record["filename"], "Centroid"] = (
            f"{(bbox[1] + bbox[3]) / 2:.4f},{(bbox[0] + bbox[2]) / 2:.4f}"
        )
        details.append(
            {
                "filename": record["filename"],
                "feature_count": inspection["feature_count"],
                "resource_type": resource_type,
                "bounding_box": _format_bbox(bbox),
            }
        )
    write_metadata_csv(dataframe.reset_index(drop=True), job.metadata_path)
    refresh_review_checksum(job)
    mark_stage(job, "enrich", details={"records": details})


def _column_position(column: dict[str, Any]) -> tuple[int, str]:
    try:
        position = int(column.get("position"))
    except (TypeError, ValueError):
        position = 1_000_000
    return position, str(column.get("fieldName", ""))


def _documented_values(column: dict[str, Any]) -> str:
    format_value = column.get("format")
    if not isinstance(format_value, dict):
        return ""
    for key in ("dropDownList", "possibleValues"):
        values = format_value.get(key)
        if isinstance(values, list):
            return "|".join(str(value).strip() for value in values if str(value).strip())
    return ""


def _column_definition(column: dict[str, Any]) -> str:
    label = str(column.get("name", "")).strip()
    description = str(column.get("description", "")).strip()
    if label and description and label.casefold() != description.casefold():
        return f"{label}: {description}"
    return description or label


def run_dictionary_stage(
    job: JobConfig,
    *,
    requester: JsonRequester = default_request_json,
) -> None:
    manifest = require_confirmed_review(job)
    metadata = validate_reviewed_metadata(job).set_index("filename")
    outputs: list[str] = []
    for record in manifest["records"]:
        source_metadata = validate_socrata_metadata(
            record["source_id"],
            requester(record["metadata_url"], None, job.request_headers),
            record["metadata_url"],
        )
        columns = source_metadata.get("columns", [])
        output_path = job.dictionary_path(record["filename"])
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=DICTIONARY_COLUMNS)
            writer.writeheader()
            for position, column in enumerate(
                sorted(
                    (value for value in columns if isinstance(value, dict)),
                    key=_column_position,
                ),
                start=1,
            ):
                writer.writerow(
                    {
                        "friendlier_id": metadata.loc[record["filename"], "ID"],
                        "field_name": column.get("fieldName") or column.get("name", ""),
                        "field_type": column.get("dataTypeName", ""),
                        "values": _documented_values(column),
                        "definition": _column_definition(column),
                        "definition_source": record["metadata_url"],
                        "parent_field_name": "",
                        "position": position,
                    }
                )
        outputs.append(str(output_path))
    mark_stage(job, "dictionaries", details={"outputs": outputs})


def run_postprocess(
    job: JobConfig,
    *,
    requester: JsonRequester = default_request_json,
    overwrite: bool = False,
) -> None:
    """Run all automated stages after the manual metadata review checkpoint."""
    require_confirmed_review(job)
    LOGGER.info("Postprocess 1/7: downloading paged Socrata GeoJSON as GeoPackages")
    run_download_stage(job, requester=requester, overwrite=overwrite)
    LOGGER.info("Postprocess 2/7: enriching metadata from downloaded GeoPackages")
    run_enrich_stage(job)
    LOGGER.info("Postprocess 3/7: harvesting Socrata data dictionaries")
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
    parser.add_argument("config", type=Path, help="Socrata curation job YAML")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("validate", help="Validate YAML inputs only")
    subparsers.add_parser("metadata", help="Harvest selected metadata and pause")
    review_parser = subparsers.add_parser("review", help="Record completion of CSV review")
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
                "Valid Socrata curation job %s with %s record(s)",
                job.job_id,
                len(job.records),
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
    except (CurationConfigError, RuntimeError, OSError, ValueError, requests.RequestException) as exc:
        LOGGER.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
