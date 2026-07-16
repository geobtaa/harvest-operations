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

import pandas as pd
import requests
import yaml


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from harvesters.arcgis import (  # noqa: E402
    ArcGISHarvester,
    arcgis_harvest_identifier_and_id,
)
from harvesters.base import BaseHarvester  # noqa: E402
from utils.field_order import PRIMARY_FIELD_ORDER  # noqa: E402
from utils.harvester_helpers import read_csv_rows  # noqa: E402

from curation.embed_qgis_metadata import (  # noqa: E402
    embed_metadata_directory,
    get_default_template_path,
)
from curation.thumbnails import create_vector_thumbnail  # noqa: E402


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
)
RESOURCE_ARTIFACT_ROLES = {
    ".gpkg": "geopackage",
    ".fgb": "flatgeobuf",
    ".pmtiles": "pmtiles",
    ".png": "thumbnail",
    ".csv": "data_dictionary",
}
LEGACY_ARTIFACT_DIRECTORIES = (
    "gpkg",
    "fgb",
    "pmtiles",
    "thumbnails",
    "data_dictionaries",
)


class CurationConfigError(ValueError):
    """Raised when a curation YAML file does not satisfy the input contract."""


@dataclass(frozen=True)
class RecordSpec:
    """One selected ArcGIS item/sublayer and its curated output filename."""

    source_id: str
    filename_stem: str
    basic_theme: str = ""

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

    def resource_dir(self, filename_or_stem: str) -> Path:
        return self.work_dir / Path(filename_or_stem).stem

    def resource_asset_path(self, filename_or_stem: str, suffix: str) -> Path:
        stem = Path(filename_or_stem).stem
        return self.resource_dir(stem) / f"{stem}{suffix}"

    def gpkg_path(self, filename_or_stem: str) -> Path:
        return self.resource_asset_path(filename_or_stem, ".gpkg")

    def dictionary_path(self, filename_or_stem: str) -> Path:
        return self.resource_asset_path(filename_or_stem, ".csv")

    def thumbnail_path(self, filename_or_stem: str) -> Path:
        return self.resource_asset_path(filename_or_stem, ".png")

    def fgb_path(self, filename_or_stem: str) -> Path:
        return self.resource_asset_path(filename_or_stem, ".fgb")

    def pmtiles_path(self, filename_or_stem: str) -> Path:
        return self.resource_asset_path(filename_or_stem, ".pmtiles")

    @property
    def report_dir(self) -> Path:
        return self.work_dir / "reports"


JsonRequester = Callable[[str, dict[str, Any] | None, str], dict[str, Any]]


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

    for index in dataframe.index:
        source_id = str(source_ids.loc[index])
        basic_theme = basic_themes.get(source_id, "").strip()
        controlled_place = str(dataframe.loc[index, "Spatial Coverage"]).split("|")[0].strip()
        description_place = humanize_spatial_coverage(controlled_place)
        year = temporal_coverage_year(dataframe.loc[index, "Temporal Coverage"])
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

    return dataframe


def build_metadata_dataframe(
    job: JobConfig,
    selected: list[tuple[RecordSpec, dict[str, Any]]],
    curated_ids: dict[str, str],
) -> pd.DataFrame:
    """Apply ArcGIS harvester rules, followed by curation-specific exceptions."""
    website_defaults = load_website_defaults(job)
    workflow = {
        "Endpoint URL": job.dcat_api,
        "Website Platform": "ArcGIS Hub",
        "Endpoint Description": "DCAT-US 1.1",
        "Accrual Method": "Manual curation",
        "Harvest Workflow": "Manual curation",
    }
    flattened = [
        {"workflow": workflow, "hub_defaults": website_defaults, "resource": resource}
        for _, resource in selected
    ]

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

    dataframe = pd.DataFrame(flattened)
    dataframe = harvester.build_dataframe(dataframe)
    if len(dataframe) != len(selected):
        retained_ids = set(dataframe.get("identifier_raw", pd.Series(dtype=str)).astype(str))
        rejected = [
            record.source_id
            for record, resource in selected
            if str(resource.get("identifier", "")) not in retained_ids
        ]
        raise RuntimeError(
            "Selected records did not pass the ArcGIS harvester's distribution filter: "
            + ", ".join(rejected)
        )

    dataframe = harvester.derive_fields(dataframe)
    dataframe = harvester.add_defaults(dataframe)
    dataframe = BaseHarvester.add_provenance(harvester, dataframe)
    source_ids = dataframe["ID"].copy()
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
    """Fetch selected DCAT records and write the manual-review metadata CSV."""
    catalog_payload = catalog if catalog is not None else requester(job.dcat_api, None, "GET")
    selected = select_catalog_records(catalog_payload, job.records)
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
    artifact_locations = [
        (job.resource_dir(record.filename_stem), RESOURCE_ARTIFACT_ROLES)
        for record in job.records
    ]
    artifact_locations.append((job.report_dir, {}))
    for directory, roles in artifact_locations:
        if not directory.is_dir():
            continue
        for artifact_path in sorted(path for path in directory.rglob("*") if path.is_file()):
            if artifact_path.name == ".DS_Store":
                continue
            role = roles.get(artifact_path.suffix.casefold(), "resource_artifact")
            if directory == job.report_dir:
                role = "report"
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
) -> None:
    manifest = require_confirmed_review(job)
    results = []
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
        result = download_service_geopackage(
            record["service_url"],
            output_path,
            Path(record["filename"]).stem,
            job.crs_authority,
            requester=requester,
            overwrite=overwrite,
        )
        result["status"] = "downloaded"
        results.append(result)
    mark_stage(job, "download", details={"outputs": results})


def _bbox_values(extent_payload: dict[str, Any]) -> tuple[float, float, float, float]:
    extent = extent_payload.get("extent", extent_payload)
    try:
        values = tuple(float(extent[key]) for key in ("xmin", "ymin", "xmax", "ymax"))
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError(f"ArcGIS extent payload is incomplete: {extent_payload}") from exc
    return values  # type: ignore[return-value]


def _format_bbox(values: tuple[float, float, float, float]) -> str:
    return ",".join(f"{value:.4f}" for value in values)


def _bbox_geometry(values: tuple[float, float, float, float]) -> str:
    west, south, east, north = values
    return (
        f"POLYGON(({west:.4f} {north:.4f}, {east:.4f} {north:.4f}, "
        f"{east:.4f} {south:.4f}, {west:.4f} {south:.4f}, "
        f"{west:.4f} {north:.4f}))"
    )


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
        service_url = record["service_url"]
        layer_metadata = requester(service_url, {"f": "pjson"}, "GET")
        extent_payload = requester(
            f"{service_url.rstrip('/')}/query",
            {
                "where": "1=1",
                "returnExtentOnly": "true",
                "outSR": "4326",
                "f": "json",
            },
            "POST",
        )
        bbox = _bbox_values(extent_payload)
        geometry_type = str(layer_metadata.get("geometryType", "")).casefold()
        resource_type = GEOMETRY_RESOURCE_TYPES.get(geometry_type)
        if not resource_type:
            raise RuntimeError(f"Unsupported ArcGIS geometry type {geometry_type!r}: {service_url}")
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
    for record in manifest["records"]:
        layer_metadata = requester(record["service_url"], {"f": "pjson"}, "GET")
        fields = layer_metadata.get("fields") or []
        if not isinstance(fields, list):
            raise RuntimeError(f"ArcGIS fields are not a list: {record['service_url']}")
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
                        "definition_source": record["service_url"],
                        "parent_field_name": "",
                        "position": position,
                    }
                )
        outputs.append(str(output_path))
    mark_stage(job, "dictionaries", details={"outputs": outputs})


def run_embed_stage(job: JobConfig) -> None:
    require_confirmed_review(job)
    validate_enriched_metadata(job)
    expected = {record.filename for record in job.records}
    missing = sorted(filename for filename in expected if not job.gpkg_path(filename).is_file())
    if missing:
        raise RuntimeError(f"GeoPackages are missing before metadata embedding: {', '.join(missing)}")
    processed: set[str] = set()
    for record in job.records:
        summary = embed_metadata_directory(
            job.resource_dir(record.filename_stem),
            job.metadata_path,
            get_default_template_path(),
            match_column="filename",
        )
        processed.update(summary.processed_files)
    if not expected.issubset(processed):
        raise RuntimeError(
            f"Metadata was not embedded in every selected GeoPackage: {sorted(expected - processed)}"
        )
    mark_stage(job, "embed", details={"outputs": summary.processed_files})


def run_thumbnail_stage(job: JobConfig) -> None:
    require_confirmed_review(job)
    outputs = []
    for record in job.records:
        gpkg_path = job.gpkg_path(record.filename)
        if not gpkg_path.is_file():
            raise RuntimeError(f"GeoPackage is missing before thumbnail creation: {gpkg_path}")
        thumbnail_path = job.thumbnail_path(record.filename_stem)
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
        "--outputs-next-to-input",
        "--report",
        str(report_path),
    ]
    if job.pmtiles_config:
        command.extend(["--config", str(job.pmtiles_config)])
    command.append("--overwrite" if overwrite else "--skip-existing")
    _run_command(command)
    mark_stage(job, "derivatives", details={"report": str(report_path)})


def _replace_path_strings(value: Any, replacements: dict[str, str]) -> Any:
    if isinstance(value, dict):
        return {
            key: _replace_path_strings(item, replacements)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_replace_path_strings(item, replacements) for item in value]
    if not isinstance(value, str):
        return value
    return replacements.get(value, value)


def migrate_resource_layout(job: JobConfig) -> list[tuple[Path, Path]]:
    """Move artifacts from legacy format folders into per-resource folders."""
    suffix_directories = {
        ".gpkg": "gpkg",
        ".fgb": "fgb",
        ".pmtiles": "pmtiles",
        ".png": "thumbnails",
        ".csv": "data_dictionaries",
    }
    moves: list[tuple[Path, Path]] = []
    replacements: dict[str, str] = {}

    for record in job.records:
        for suffix, directory_name in suffix_directories.items():
            legacy_dir = job.work_dir / directory_name
            if not legacy_dir.is_dir():
                continue
            expected_name = f"{record.filename_stem}{suffix}"
            source = legacy_dir / expected_name
            if not source.is_file():
                matches = [
                    path
                    for path in legacy_dir.glob(f"*{suffix}")
                    if path.stem.casefold() == record.filename_stem.casefold()
                ]
                if len(matches) != 1:
                    continue
                source = matches[0]
            target = job.resource_asset_path(record.filename_stem, suffix)
            if target.exists():
                if file_sha256(source) != file_sha256(target):
                    raise RuntimeError(
                        f"Cannot migrate {source}; a different target already exists: {target}"
                    )
                source.unlink()
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                source.replace(target)
            moves.append((source, target))
            replacements[source.relative_to(job.work_dir).as_posix()] = (
                target.relative_to(job.work_dir).as_posix()
            )
            replacements[str(source)] = str(target)

    for directory_name in LEGACY_ARTIFACT_DIRECTORIES:
        directory = job.work_dir / directory_name
        ds_store = directory / ".DS_Store"
        if ds_store.is_file():
            ds_store.unlink()
        if directory.is_dir() and not any(directory.iterdir()):
            directory.rmdir()

    if job.manifest_path.is_file():
        manifest = _replace_path_strings(load_manifest(job), replacements)
        manifest.get("stages", {}).pop("snapshot", None)
        manifest.pop("latest_run_record", None)
        write_manifest(job, manifest)

    report_paths = job.report_dir.glob("*") if job.report_dir.is_dir() else ()
    for report_path in report_paths:
        if not report_path.is_file():
            continue
        try:
            content = report_path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        updated = content
        for old_path, new_path in replacements.items():
            updated = updated.replace(old_path, new_path)
        if updated != content:
            report_path.write_text(updated, encoding="utf-8")

    return moves


def run_postprocess(
    job: JobConfig,
    *,
    requester: JsonRequester = default_request_json,
    overwrite: bool = False,
) -> None:
    """Run all automated stages after the manual metadata review checkpoint."""
    require_confirmed_review(job)
    LOGGER.info("Postprocess 1/6: downloading GeoPackages")
    run_download_stage(job, requester=requester, overwrite=overwrite)
    LOGGER.info("Postprocess 2/6: enriching metadata")
    run_enrich_stage(job, requester=requester)
    LOGGER.info("Postprocess 3/6: creating data dictionaries")
    run_dictionary_stage(job, requester=requester)
    LOGGER.info("Postprocess 4/6: embedding GeoPackage metadata")
    run_embed_stage(job)
    LOGGER.info("Postprocess 5/6: creating thumbnails")
    run_thumbnail_stage(job)
    LOGGER.info("Postprocess 6/6: creating FlatGeoBuf and PMTiles derivatives")
    run_derivatives_stage(job, overwrite=overwrite)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path, help="ArcGIS curation job YAML")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("validate", help="Validate YAML inputs only")
    subparsers.add_parser("metadata", help="Harvest selected DCAT metadata and pause")
    review_parser = subparsers.add_parser("review", help="Record completion of manual CSV review")
    review_parser.add_argument("--confirm", action="store_true")
    for command_name in ("download", "postprocess", "derivatives"):
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
    subparsers.add_parser(
        "migrate-layout",
        help="Move legacy format-folder outputs into per-resource folders",
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
        elif args.command == "postprocess":
            run_postprocess(job, overwrite=args.overwrite)
        elif args.command == "snapshot":
            path = save_run_record(job)
            LOGGER.info("Saved portable run record: %s", path)
        elif args.command == "migrate-layout":
            moves = migrate_resource_layout(job)
            LOGGER.info("Migrated %s artifact(s) into resource folders", len(moves))
        elif args.command == "status":
            print(json.dumps(load_manifest(job), indent=2))
    except (CurationConfigError, RuntimeError, OSError) as exc:
        LOGGER.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
