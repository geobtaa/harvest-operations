"""Staged CKAN curation pipeline for selected geospatial datasets.

The metadata stage reuses the repository CKAN harvester. Selected GeoJSON
resources are downloaded from CKAN and converted to projected GeoPackages
before the shared curation derivative stages run.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable
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
from harvesters.ckan import CkanHarvester  # noqa: E402
from utils.field_order import PRIMARY_FIELD_ORDER  # noqa: E402
from utils.geometry_repair import repair_geometry_fields  # noqa: E402

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
CKAN_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$", re.IGNORECASE)
CKAN_GEOMETRY_RESOURCE_TYPES = {
    "point": "Point data",
    "multipoint": "Point data",
    "linestring": "Line data",
    "multilinestring": "Line data",
    "polygon": "Polygon data",
    "multipolygon": "Polygon data",
}


@dataclass(frozen=True)
class RecordSpec:
    """One selected CKAN package and its curated output filename."""

    source_id: str
    filename_theme: str
    filename_stem: str
    basic_theme: str = ""
    temporal_year: str = ""
    resource_id: str = ""

    @property
    def filename(self) -> str:
        return f"{self.filename_stem}.gpkg"


@dataclass(frozen=True)
class JobConfig:
    """Validated CKAN curation job configuration."""

    config_path: Path
    job_id: str
    work_dir: Path
    hub_name: str
    hub_landing_page: str
    api_base: str
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

    def package_url(self, source_id: str) -> str:
        return f"{self.api_base}/api/3/action/package_show?id={source_id}"


JsonRequester = Callable[[str, dict[str, Any] | None], Any]


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


def _http_url(value: str, label: str) -> str:
    parsed = urlsplit(value.strip())
    if parsed.scheme.casefold() not in {"http", "https"} or not parsed.netloc:
        raise CurationConfigError(f"{label} must be an HTTP(S) URL")
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, parsed.query, ""))


def load_job_config(config_path: Path | str) -> JobConfig:
    """Load and validate a CKAN curation YAML file."""
    path = Path(config_path).expanduser().resolve()
    with path.open(encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    if raw.get("version") != 1:
        raise CurationConfigError("version must be 1")

    job_raw = _mapping(raw.get("job"), "job")
    hub_raw = _mapping(raw.get("hub"), "hub")
    crs_raw = _mapping(raw.get("coordinate_reference_system"), "coordinate_reference_system")
    metadata_raw = _mapping(raw.get("metadata"), "metadata")
    naming_raw = _mapping(raw.get("file_naming"), "file_naming")
    selection_raw = _mapping(raw.get("selection_criteria", {}), "selection_criteria")
    review_raw = _mapping(raw.get("manual_review", {}), "manual_review")
    derivatives_raw = _mapping(raw.get("derivatives", {}), "derivatives")

    export_date_text = _required_text(metadata_raw, "export_date", "metadata")
    try:
        export_date = date.fromisoformat(export_date_text)
    except ValueError as exc:
        raise CurationConfigError("metadata.export_date must use YYYY-MM-DD") from exc

    city_abbreviation = _required_text(naming_raw, "city_abbreviation", "file_naming")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9-]*", city_abbreviation):
        raise CurationConfigError(
            "file_naming.city_abbreviation must contain only letters, numbers, or hyphens"
        )
    download_year = _required_string(naming_raw, "download_year", "file_naming")
    if not re.fullmatch(r"(?:19|20)\d{2}", download_year):
        raise CurationConfigError("file_naming.download_year must be a quoted four-digit year")
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
        if not CKAN_NAME_RE.fullmatch(source_id):
            raise CurationConfigError(f"records[{index}].id must be a CKAN package name")
        filename_theme = _required_text(record_raw, "filename_theme", f"records[{index}]")
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", filename_theme):
            raise CurationConfigError(
                f"records[{index}].filename_theme must contain only letters, numbers, underscores, or hyphens"
            )
        filename_stem = f"{city_abbreviation}_{filename_theme}_{download_year}"
        temporal_year = str(record_raw.get("temporal_year", "")).strip()
        if temporal_year and not re.fullmatch(r"(?:19|20)\d{2}", temporal_year):
            raise CurationConfigError(
                f"records[{index}].temporal_year must be a four-digit year"
            )
        resource_id = str(record_raw.get("resource_id", "")).strip()
        if resource_id and not re.fullmatch(r"[0-9a-fA-F-]{36}", resource_id):
            raise CurationConfigError(
                f"records[{index}].resource_id must be a CKAN resource UUID"
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
                basic_theme=str(record_raw.get("basic_theme", "")).strip(),
                temporal_year=temporal_year,
                resource_id=resource_id,
            )
        )

    allowed_value = selection_raw.get(
        "allowed_resource_types", ["Polygon data", "Line data", "Point data"]
    )
    if not isinstance(allowed_value, list) or not allowed_value:
        raise CurationConfigError(
            "selection_criteria.allowed_resource_types must be a non-empty list"
        )
    allowed_types = tuple(str(value).strip() for value in allowed_value)
    invalid_types = sorted(set(allowed_types) - set(CKAN_GEOMETRY_RESOURCE_TYPES.values()))
    if invalid_types:
        raise CurationConfigError(
            "Unsupported allowed_resource_types: " + ", ".join(invalid_types)
        )

    required_fields = review_raw.get("required_fields", DEFAULT_REQUIRED_REVIEW_FIELDS)
    if not isinstance(required_fields, list) or not all(
        isinstance(value, str) and value.strip() for value in required_fields
    ):
        raise CurationConfigError("manual_review.required_fields must be a list of field names")

    websites_csv = _resolve_path(
        str(hub_raw.get("websites_csv", REPO_ROOT / "reference_data" / "websites.csv")),
        path,
    )
    if not websites_csv.is_file():
        raise CurationConfigError(f"websites_csv does not exist: {websites_csv}")
    pmtiles_value = str(derivatives_raw.get("pmtiles_config", "")).strip()
    pmtiles_config = _resolve_path(pmtiles_value, path) if pmtiles_value else None
    if pmtiles_config is not None and not pmtiles_config.is_file():
        raise CurationConfigError(f"PMTiles config does not exist: {pmtiles_config}")

    job_id = _required_text(job_raw, "id", "job")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", job_id):
        raise CurationConfigError(
            "job.id must start with a letter or number and contain only letters, numbers, periods, underscores, or hyphens"
        )

    return JobConfig(
        config_path=path,
        job_id=job_id,
        work_dir=_resolve_path(_required_text(job_raw, "work_directory", "job"), path),
        hub_name=_required_text(hub_raw, "name", "hub"),
        hub_landing_page=_http_url(
            _required_text(hub_raw, "landing_page", "hub"), "hub.landing_page"
        ),
        api_base=_http_origin(_required_text(hub_raw, "api_base", "hub"), "hub.api_base"),
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
        allowed_resource_types=allowed_types,
        required_review_fields=tuple(value.strip() for value in required_fields),
        pmtiles_config=pmtiles_config,
    )


def default_request_json(url: str, params: dict[str, Any] | None = None) -> Any:
    response = requests.get(
        url,
        params=params or {},
        headers={"User-Agent": "BTAA-GIN CKAN curation pipeline/1.0"},
        timeout=120,
    )
    response.raise_for_status()
    return response.json()


def validate_package_payload(payload: Any, source_id: str, url: str) -> dict[str, Any]:
    if not isinstance(payload, dict) or payload.get("success") is not True:
        raise RuntimeError(f"CKAN returned an unsuccessful response from {url}")
    package = payload.get("result")
    if not isinstance(package, dict):
        raise RuntimeError(f"CKAN response has no package result: {url}")
    if str(package.get("name", "")).casefold() != source_id.casefold():
        raise RuntimeError(
            f"CKAN package name mismatch: expected {source_id}; found {package.get('name', '')}"
        )
    return package


def select_geojson_resource(package: dict[str, Any], record: RecordSpec) -> dict[str, Any]:
    resources = package.get("resources")
    if not isinstance(resources, list):
        raise RuntimeError(f"CKAN package has no resource list: {record.source_id}")
    if record.resource_id:
        candidates = [item for item in resources if str(item.get("id", "")) == record.resource_id]
    else:
        candidates = [
            item
            for item in resources
            if isinstance(item, dict)
            and (
                str(item.get("format", "")).casefold() in {"geojson", "json"}
                or str(item.get("url", "")).casefold().endswith((".geojson", ".json"))
            )
        ]
        geojson_candidates = [
            item
            for item in candidates
            if str(item.get("format", "")).casefold() == "geojson"
            or str(item.get("url", "")).casefold().endswith(".geojson")
        ]
        candidates = geojson_candidates or candidates
    if len(candidates) != 1:
        detail = "not found" if not candidates else "ambiguous"
        raise RuntimeError(
            f"GeoJSON resource is {detail} for CKAN package {record.source_id}; configure resource_id"
        )
    resource = candidates[0]
    if not str(resource.get("url", "")).strip():
        raise RuntimeError(f"Selected CKAN resource has no URL: {record.source_id}")
    return resource


def package_landing_page(job: JobConfig, package: dict[str, Any]) -> str:
    organization = package.get("organization")
    organization_name = (
        str(organization.get("name", "")).strip()
        if isinstance(organization, dict)
        else ""
    )
    package_name = str(package.get("name", "")).strip()
    if organization_name and package_name:
        return f"{job.hub_landing_page.rstrip('/')}/{organization_name}/{package_name}"
    if package_name:
        return f"{job.hub_landing_page.rstrip('/')}/dataset/{package_name}"
    return job.hub_landing_page


def resolve_selected_records(
    job: JobConfig,
    *,
    requester: JsonRequester = default_request_json,
) -> list[tuple[RecordSpec, dict[str, Any], dict[str, Any]]]:
    selected = []
    for record in job.records:
        url = job.package_url(record.source_id)
        package = validate_package_payload(requester(url, None), record.source_id, url)
        selected.append((record, package, select_geojson_resource(package, record)))
    return selected


def build_metadata_dataframe(
    job: JobConfig,
    selected: list[tuple[RecordSpec, dict[str, Any], dict[str, Any]]],
    curated_ids: dict[str, str],
) -> pd.DataFrame:
    """Apply CKAN harvester rules, followed by curation-specific exceptions."""
    website_defaults = load_website_defaults(job)
    endpoint_url = f"{job.api_base}/api/3/action/package_search"
    source = {
        "workflow": {
            "Endpoint URL": endpoint_url,
            "Website Platform": "CKAN / PortalJS",
            "Endpoint Description": "CKAN API (package_show)",
            "Accrual Method": "Manual curation",
            "Harvest Workflow": "Manual curation",
        },
        "hub_defaults": website_defaults,
        "base_url": job.hub_landing_page,
        "endpoint_url": endpoint_url,
        "site_title": job.hub_name,
    }
    harvester = CkanHarvester(
        {
            "base_url": job.hub_landing_page,
            "endpoint_url": endpoint_url,
            "output_primary_csv": "unused.csv",
            "output_distributions_csv": "unused.csv",
            "output_report_csv": "unused.csv",
            "themes_csv": str(REPO_ROOT / "reference_data" / "themes.csv"),
            "build_uploads": False,
        }
    )
    harvester.theme_map = load_theme_map(REPO_ROOT / "reference_data" / "themes.csv")
    raw_records = []
    for _, package, _ in selected:
        value = dict(package)
        value["_ckan_source"] = source
        raw_records.append(value)
    dataframe = harvester.build_dataframe(pd.DataFrame(raw_records))
    dataframe = harvester.derive_fields(dataframe)
    dataframe = harvester.add_defaults(dataframe)
    dataframe = BaseHarvester.add_provenance(harvester, dataframe)
    source_ids = pd.Series(
        [record.source_id for record, _, _ in selected], index=dataframe.index, dtype=str
    )
    resource_urls = {
        record.source_id: str(resource.get("url", "")).strip()
        for record, _, resource in selected
    }
    dataframe["Provenance"] = source_ids.map(
        {
            source_id: (
                f"Exported from {resource_url} as GeoPackage on "
                f"{formatted_export_date(job.export_date)}."
            )
            for source_id, resource_url in resource_urls.items()
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
        [(record, package) for record, package, _ in selected],
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
        "source_platform": "ckan",
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
                "package_id": str(package.get("id", "")),
                "filename_theme": record.filename_theme,
                "curated_id": curated_ids[record.source_id],
                "filename": record.filename,
                "landing_page": package_landing_page(job, package),
                "metadata_url": job.package_url(record.source_id),
                "source_revision": package.get("metadata_modified"),
                "resource_id": str(resource.get("id", "")),
                "resource_url": str(resource.get("url", "")),
                "resource_revision": resource.get("last_modified"),
                "resource_size": resource.get("size"),
            }
            for record, package, resource in selected
        ],
    }


def run_metadata_stage(
    job: JobConfig, *, requester: JsonRequester = default_request_json
) -> Path:
    selected = resolve_selected_records(job, requester=requester)
    curated_ids = assign_curated_ids(job)
    dataframe = build_metadata_dataframe(job, selected, curated_ids)
    write_metadata_csv(dataframe, job.metadata_path)
    write_manifest(job, build_manifest(job, selected, curated_ids))
    return job.metadata_path


def _run_command(command: list[str]) -> None:
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    if completed.returncode != 0:
        raise RuntimeError(
            f"Command failed ({completed.returncode}): {' '.join(command)}\n{completed.stderr.strip()}"
        )


def _download_resource(url: str, output_path: Path) -> None:
    with requests.get(
        url,
        headers={"User-Agent": "BTAA-GIN CKAN curation pipeline/1.0"},
        timeout=300,
        stream=True,
    ) as response:
        response.raise_for_status()
        with output_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)


def download_ckan_geopackage(
    resource_url: str,
    output_path: Path,
    layer_name: str,
    output_crs: str,
    *,
    overwrite: bool = False,
) -> dict[str, Any]:
    """Download one CKAN GeoJSON resource and convert it to GeoPackage."""
    ogr2ogr = shutil.which("ogr2ogr")
    if not ogr2ogr:
        raise RuntimeError("ogr2ogr is required to download GeoPackages")
    if output_path.exists() and not overwrite:
        raise FileExistsError(f"GeoPackage already exists (use --overwrite): {output_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    partial_path = output_path.with_suffix(".partial.gpkg")
    if partial_path.exists():
        partial_path.unlink()
    if output_path.exists() and overwrite:
        output_path.unlink()
    try:
        with tempfile.TemporaryDirectory(prefix="ckan-download-", dir=output_path.parent) as temp_dir:
            source_path = Path(temp_dir) / "resource.geojson"
            _download_resource(resource_url, source_path)
            _run_command(
                [
                    ogr2ogr,
                    "-f",
                    "GPKG",
                    str(partial_path),
                    str(source_path),
                    "-nln",
                    layer_name,
                    "-t_srs",
                    output_crs,
                    "-nlt",
                    "PROMOTE_TO_MULTI",
                    "-lco",
                    "SPATIAL_INDEX=YES",
                ]
            )
        partial_path.replace(output_path)
    except Exception:
        if partial_path.exists():
            partial_path.unlink()
        raise
    return {"source_url": resource_url, "output": str(output_path)}


def run_download_stage(
    job: JobConfig,
    *,
    requester: JsonRequester = default_request_json,
    overwrite: bool = False,
) -> None:
    manifest = require_confirmed_review(job)
    results = []
    for record in manifest["records"]:
        package = validate_package_payload(
            requester(record["metadata_url"], None), record["source_id"], record["metadata_url"]
        )
        resources = package.get("resources", [])
        current = next(
            (item for item in resources if str(item.get("id", "")) == record["resource_id"]),
            None,
        )
        if not isinstance(current, dict):
            raise RuntimeError(f"CKAN resource disappeared after metadata: {record['resource_id']}")
        if package.get("metadata_modified") != record.get("source_revision") or current.get(
            "last_modified"
        ) != record.get("resource_revision"):
            raise RuntimeError(
                f"CKAN package {record['source_id']} changed after metadata; run metadata and review again"
            )
        output_path = job.gpkg_path(record["filename"])
        if output_path.is_file() and not overwrite:
            results.append({"status": "skipped_existing", "output": str(output_path)})
            continue
        result = download_ckan_geopackage(
            record["resource_url"],
            output_path,
            Path(record["filename"]).stem,
            job.crs_authority,
            overwrite=overwrite,
        )
        result["status"] = "downloaded"
        results.append(result)
    mark_stage(job, "download", details={"outputs": results})


def _format_bbox(values: tuple[float, float, float, float]) -> str:
    return ",".join(f"{value:.4f}" for value in values)


def inspect_geopackage(path: Path) -> dict[str, Any]:
    """Read feature count, geometry, WGS84 bounds, and fields from a GeoPackage."""
    with fiona.open(path) as collection:
        feature_count = len(collection)
        geometry_type = str(collection.schema.get("geometry", ""))
        source_crs = collection.crs_wkt or collection.crs
        bounds = tuple(float(value) for value in collection.bounds)
        properties = dict(collection.schema.get("properties", {}))
        normalized_geometry = geometry_type.removeprefix("3D ").casefold()
        resource_type = CKAN_GEOMETRY_RESOURCE_TYPES.get(normalized_geometry)
        if not resource_type:
            feature_geometry_types = {
                str(feature["geometry"].get("type", ""))
                for feature in collection
                if feature.get("geometry")
            }
            inferred_resource_types = {
                CKAN_GEOMETRY_RESOURCE_TYPES.get(
                    value.removeprefix("3D ").casefold()
                )
                for value in feature_geometry_types
            }
            if (
                feature_geometry_types
                and None not in inferred_resource_types
                and len(inferred_resource_types) == 1
            ):
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
        "properties": properties,
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
        resource_type = inspection["resource_type"]
        if resource_type not in job.allowed_resource_types:
            raise RuntimeError(
                f"Derived resource type {resource_type!r} is excluded by YAML selection criteria"
            )
        bbox = inspection["bounds"]
        dataframe.loc[record["filename"], "Resource Type"] = resource_type
        dataframe.loc[record["filename"], "Bounding Box"] = _format_bbox(bbox)
        dataframe.loc[record["filename"], "Geometry"] = repair_geometry_fields(
            _format_bbox(bbox), ""
        ).geometry
        dataframe.loc[record["filename"], "Centroid"] = (
            f"{(bbox[1] + bbox[3]) / 2:.4f},{(bbox[0] + bbox[2]) / 2:.4f}"
        )
        record["feature_count"] = inspection["feature_count"]
        details.append(
            {
                "filename": record["filename"],
                "feature_count": inspection["feature_count"],
                "resource_type": resource_type,
                "bounding_box": _format_bbox(bbox),
            }
        )
    write_metadata_csv(dataframe.reset_index(drop=True), job.metadata_path)
    write_manifest(job, manifest)
    refresh_review_checksum(job)
    mark_stage(job, "enrich", details={"records": details})


def run_dictionary_stage(job: JobConfig) -> None:
    manifest = require_confirmed_review(job)
    metadata = validate_reviewed_metadata(job).set_index("filename")
    outputs = []
    for record in manifest["records"]:
        gpkg_path = job.gpkg_path(record["filename"])
        if not gpkg_path.is_file():
            raise RuntimeError(f"GeoPackage is missing; run download first: {gpkg_path}")
        properties = inspect_geopackage(gpkg_path)["properties"]
        output_path = job.dictionary_path(record["filename"])
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=DICTIONARY_COLUMNS)
            writer.writeheader()
            for position, (field_name, field_type) in enumerate(properties.items(), start=1):
                writer.writerow(
                    {
                        "friendlier_id": metadata.loc[record["filename"], "ID"],
                        "field_name": field_name,
                        "field_type": field_type,
                        "values": "",
                        "definition": "",
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
    require_confirmed_review(job)
    LOGGER.info("Postprocess 1/7: downloading CKAN GeoJSON resources as GeoPackages")
    run_download_stage(job, requester=requester, overwrite=overwrite)
    LOGGER.info("Postprocess 2/7: enriching metadata from downloaded GeoPackages")
    run_enrich_stage(job)
    LOGGER.info("Postprocess 3/7: creating GeoPackage field dictionaries")
    run_dictionary_stage(job)
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
    parser.add_argument("config", type=Path, help="CKAN curation job YAML")
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
    subparsers.add_parser("snapshot")
    subparsers.add_parser("status")
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = build_parser().parse_args(argv)
    try:
        job = load_job_config(args.config)
        if args.command == "validate":
            mark_validation_stage(job)
            LOGGER.info("Valid CKAN curation job %s with %s record(s)", job.job_id, len(job.records))
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
    except (
        CurationConfigError,
        RuntimeError,
        OSError,
        ValueError,
        requests.RequestException,
    ) as exc:
        LOGGER.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
