"""Staged curation pipeline for local Minnesota Geospatial Commons snapshots.

The pipeline inventories and validates local source packages, crosswalks FGDC
metadata to a B1G Aardvark review CSV, generates access GeoPackages from the
preservation File Geodatabases, and records an explicit manual-review
checkpoint. Source packages are read-only inputs; all generated state is
written below the job's configured work directory.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import math
import re
import secrets
import shutil
import sqlite3
import subprocess
import sys
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

import yaml


REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from utils.field_order import PRIMARY_FIELD_ORDER  # noqa: E402

from curation.fgdc_metadata import (  # noqa: E402
    FgdcMetadata,
    clean_text,
    local_name,
    normalize_fgdc_date,
    parse_fgdc_metadata,
)
from curation.embed_qgis_metadata import (  # noqa: E402
    build_metadata_xml,
    get_default_template_path,
    load_metadata_lookup,
    write_metadata_to_gpkg,
)
from curation.thumbnails import create_vector_thumbnail  # noqa: E402


LOGGER = logging.getLogger(__name__)
NANOID_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
NANOID_LENGTH = 12
REVIEW_COLUMNS = (
    "series_id",
    "snapshot_year",
    "snapshot_date",
    "source_resource_path",
    "resource_guid",
    "gdrs_current_as_of",
    "fgdc_publication_date",
    "fgdc_calendar_date",
    "fgdc_begin_date",
    "fgdc_end_date",
    "fgdc_metadata_date",
    "proposed_temporal_source",
    "geopackage_status",
    "metadata_review_flags",
)
INVENTORY_COLUMNS = (
    "series_id",
    "snapshot_year",
    "source_resource_path",
    "resource_guid",
    "base_name",
    "geodatabase_path",
    "geodatabase_size_bytes",
    "geodatabase_sha256",
    "geopackage_path",
    "geopackage_size_bytes",
    "geopackage_sha256",
    "geopackage_status",
    "geopackage_embedded_metadata",
    "layer_count",
    "layers",
    "feature_counts",
    "geometry_types",
    "metadata_xml_path",
    "metadata_xml_sha256",
    "metadata_html_path",
    "preview_path",
    "layer_file_count",
    "layer_files",
    "layer_file_sha256",
    "validation_warnings",
    "validation_errors",
)
UPLOAD_MANIFEST_COLUMNS = (
    "source_key",
    "role",
    "path",
    "size_bytes",
    "sha256",
    "contents",
    "status",
)
DEFAULT_REQUIRED_REVIEW_FIELDS = (
    "filename",
    "ID",
    "Title",
    "Description",
    "Creator",
    "Publisher",
    "Provider",
    "Resource Class",
    "Resource Type",
    "Temporal Coverage",
    "Date Issued",
    "Spatial Coverage",
    "Bounding Box",
    "Rights",
    "Access Rights",
    "Member Of",
)
COLLECTION_REQUIRED_REVIEW_FIELDS = (
    "ID",
    "Title",
    "Provider",
    "Resource Class",
    "Temporal Coverage",
    "Spatial Coverage",
    "Access Rights",
)
GEOMETRY_RESOURCE_TYPES = {
    "point": "Point data",
    "multipoint": "Point data",
    "linestring": "Line data",
    "multilinestring": "Line data",
    "circularstring": "Line data",
    "compoundcurve": "Line data",
    "multicurve": "Line data",
    "polygon": "Polygon data",
    "multipolygon": "Polygon data",
    "curvepolygon": "Polygon data",
    "multisurface": "Polygon data",
}


class CurationConfigError(ValueError):
    """Raised when a MnCommons job is invalid."""


@dataclass(frozen=True)
class VersionSpec:
    """One captured resource version within a longitudinal series."""

    series_id: str
    snapshot_year: int
    snapshot_date: str
    temporal_coverage: str
    source_path: Path
    output_stem: str
    description_prefix: str
    metadata_overrides: dict[str, str]

    @property
    def source_key(self) -> str:
        return f"{self.series_id}:{self.snapshot_year}"

    @property
    def filename(self) -> str:
        return f"{self.output_stem}.gpkg"


@dataclass(frozen=True)
class SeriesSpec:
    """Configuration shared by every captured version of a dataset series."""

    series_id: str
    title: str
    catalog_title: str
    resource_guid: str
    spatial_coverage: str
    metadata_overrides: dict[str, str]
    versions: tuple[VersionSpec, ...]


@dataclass(frozen=True)
class CollectionSpec:
    """Collection-level Aardvark record that contains every curated version."""

    collection_id: str
    title: str
    description: str
    creator: str
    spatial_coverage: str
    temporal_coverage: str
    date_issued: str
    rights: str
    metadata_overrides: dict[str, str]


@dataclass(frozen=True)
class MetadataDefaults:
    provider: str
    publisher: str
    code: str
    series_label_prefix: str
    accession_date: str
    access_rights: str
    publication_state: str
    display_note: str


@dataclass(frozen=True)
class JobConfig:
    """Validated MnCommons pilot configuration."""

    config_path: Path
    job_id: str
    source_dir: Path
    work_dir: Path
    collection: CollectionSpec
    metadata: MetadataDefaults
    required_review_fields: tuple[str, ...]
    series: tuple[SeriesSpec, ...]
    pmtiles_config: Path | None = None

    @property
    def manifest_path(self) -> Path:
        return self.work_dir / "manifest.json"

    @property
    def inventory_path(self) -> Path:
        return self.work_dir / "reports" / "source_inventory.csv"

    @property
    def metadata_path(self) -> Path:
        return self.work_dir / "metadata" / "metadata.csv"

    @property
    def collection_metadata_path(self) -> Path:
        return self.work_dir / "metadata" / "collection_metadata.csv"

    @property
    def items_dir(self) -> Path:
        return self.work_dir / "items"

    @property
    def uploads_dir(self) -> Path:
        return self.work_dir / "uploads"

    @property
    def upload_manifest_path(self) -> Path:
        return self.work_dir / "reports" / "upload_manifest.csv"

    @property
    def versions(self) -> tuple[VersionSpec, ...]:
        return tuple(version for series in self.series for version in series.versions)

    def item_dir(self, version: VersionSpec) -> Path:
        return self.items_dir / version.output_stem

    def output_geopackage(self, version: VersionSpec) -> Path:
        return self.item_dir(version) / version.filename

    def thumbnail_path(self, version: VersionSpec) -> Path:
        return self.item_dir(version) / f"{version.output_stem}.png"


@dataclass(frozen=True)
class ResourcePackage:
    """Resolved files belonging to one local GDRS resource package."""

    version: VersionSpec
    data_resource_xml: Path
    metadata_xml: Path
    metadata_html: Path
    preview: Path | None
    geodatabase: Path
    geopackage: Path | None
    layer_files: tuple[Path, ...]


VectorInspector = Callable[[Path], dict[str, Any]]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise CurationConfigError(f"{label} must be a mapping")
    return value


def _required_text(mapping: dict[str, Any], key: str, label: str) -> str:
    value = clean_text(str(mapping.get(key, "")))
    if not value:
        raise CurationConfigError(f"Missing required value: {label}.{key}")
    return value


def _metadata_overrides(mapping: dict[str, Any], label: str) -> dict[str, str]:
    """Read curator-selected Aardvark values from a configuration section."""
    raw = mapping.get("metadata_overrides") or {}
    values = _mapping(raw, f"{label}.metadata_overrides")
    unknown = sorted(set(values) - set(PRIMARY_FIELD_ORDER))
    if unknown:
        raise CurationConfigError(
            f"{label}.metadata_overrides contains unknown Aardvark fields: "
            + ", ".join(unknown)
        )
    return {
        str(field): clean_text(str("" if value is None else value))
        for field, value in values.items()
    }


def _resolve_path(value: str, config_path: Path) -> Path:
    path = Path(value).expanduser()
    return (
        path.resolve() if path.is_absolute() else (config_path.parent / path).resolve()
    )


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.casefold()).strip("_")
    if not slug:
        raise CurationConfigError(f"Cannot create an output name from {value!r}")
    return slug


def normalize_guid(value: str) -> str:
    return clean_text(value).strip("{}").casefold()


def load_job_config(path: Path) -> JobConfig:
    """Load and validate a MnCommons YAML job."""
    config_path = Path(path).expanduser().resolve()
    if not config_path.is_file():
        raise CurationConfigError(f"Job configuration does not exist: {config_path}")
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    root = _mapping(raw, "configuration")
    if root.get("version") != 1:
        raise CurationConfigError("version must be 1")

    job_raw = _mapping(root.get("job"), "job")
    job_id = _required_text(job_raw, "id", "job")
    source_dir = _resolve_path(
        _required_text(job_raw, "source_directory", "job"), config_path
    )
    work_dir = _resolve_path(
        _required_text(job_raw, "work_directory", "job"), config_path
    )
    if source_dir == work_dir or work_dir.is_relative_to(source_dir):
        raise CurationConfigError(
            "job.work_directory must be outside the source directory"
        )

    collection_raw = _mapping(root.get("collection"), "collection")
    collection = CollectionSpec(
        collection_id=_required_text(collection_raw, "id", "collection"),
        title=_required_text(collection_raw, "title", "collection"),
        description=_required_text(collection_raw, "description", "collection"),
        creator=_required_text(collection_raw, "creator", "collection"),
        spatial_coverage=_required_text(
            collection_raw, "spatial_coverage", "collection"
        ),
        temporal_coverage=_required_text(
            collection_raw, "temporal_coverage", "collection"
        ),
        date_issued=clean_text(str(collection_raw.get("date_issued", ""))),
        rights=clean_text(str(collection_raw.get("rights", ""))),
        metadata_overrides=_metadata_overrides(collection_raw, "collection"),
    )
    if not collection.collection_id.startswith("b1g_"):
        raise CurationConfigError("collection.id must be a B1G ID beginning with b1g_")

    metadata_raw = _mapping(root.get("metadata"), "metadata")
    accession_value = metadata_raw.get("accession_date", date.today().isoformat())
    metadata = MetadataDefaults(
        provider=_required_text(metadata_raw, "provider", "metadata"),
        publisher=_required_text(metadata_raw, "publisher", "metadata"),
        code=clean_text(str(metadata_raw.get("code", ""))),
        series_label_prefix=(
            clean_text(str(metadata_raw.get("series_label_prefix", "")))
            or "Minnesota Geospatial Commons series:"
        ),
        accession_date=str(accession_value),
        access_rights=clean_text(str(metadata_raw.get("access_rights", "Public")))
        or "Public",
        publication_state=(
            clean_text(str(metadata_raw.get("publication_state", "draft"))) or "draft"
        ),
        display_note=clean_text(str(metadata_raw.get("display_note", ""))),
    )

    review_raw = root.get("manual_review") or {}
    review_mapping = _mapping(review_raw, "manual_review")
    required_fields_raw = review_mapping.get(
        "required_fields", list(DEFAULT_REQUIRED_REVIEW_FIELDS)
    )
    if not isinstance(required_fields_raw, list) or not all(
        isinstance(field, str) and field.strip() for field in required_fields_raw
    ):
        raise CurationConfigError(
            "manual_review.required_fields must be a list of names"
        )
    required_review_fields = tuple(field.strip() for field in required_fields_raw)

    derivatives_raw = _mapping(root.get("derivatives") or {}, "derivatives")
    pmtiles_value = clean_text(str(derivatives_raw.get("pmtiles_config", "")))
    pmtiles_config = (
        _resolve_path(pmtiles_value, config_path) if pmtiles_value else None
    )
    if pmtiles_config is not None and not pmtiles_config.is_file():
        raise CurationConfigError(
            f"derivatives.pmtiles_config does not exist: {pmtiles_config}"
        )

    series_raw = root.get("series")
    if not isinstance(series_raw, list) or not series_raw:
        raise CurationConfigError("series must be a nonempty list")
    seen_series: set[str] = set()
    seen_source_paths: set[Path] = set()
    seen_output_stems: set[str] = set()
    parsed_series: list[SeriesSpec] = []
    for series_index, series_value in enumerate(series_raw, start=1):
        label = f"series[{series_index}]"
        series_mapping = _mapping(series_value, label)
        series_id = _required_text(series_mapping, "id", label)
        if series_id in seen_series:
            raise CurationConfigError(f"Duplicate series ID: {series_id}")
        seen_series.add(series_id)
        expected_guid = normalize_guid(
            _required_text(series_mapping, "resource_guid", label)
        )
        version_values = series_mapping.get("versions")
        if not isinstance(version_values, list) or not version_values:
            raise CurationConfigError(f"{label}.versions must be a nonempty list")
        parsed_versions: list[VersionSpec] = []
        seen_years: set[int] = set()
        for version_index, version_value in enumerate(version_values, start=1):
            version_label = f"{label}.versions[{version_index}]"
            version_mapping = _mapping(version_value, version_label)
            try:
                snapshot_year = int(version_mapping.get("snapshot_year"))
            except (TypeError, ValueError) as exc:
                raise CurationConfigError(
                    f"{version_label}.snapshot_year must be a four-digit year"
                ) from exc
            if snapshot_year < 1900 or snapshot_year > 2200:
                raise CurationConfigError(
                    f"{version_label}.snapshot_year must be a four-digit year"
                )
            if snapshot_year in seen_years:
                raise CurationConfigError(
                    f"Duplicate snapshot year {snapshot_year} in {series_id}"
                )
            seen_years.add(snapshot_year)
            snapshot_date = clean_text(
                str(version_mapping.get("snapshot_date", snapshot_year))
            )
            if not re.fullmatch(r"\d{4}(?:-(?:0[1-9]|1[0-2]))?", snapshot_date):
                raise CurationConfigError(
                    f"{version_label}.snapshot_date must be YYYY or YYYY-MM"
                )
            if int(snapshot_date[:4]) != snapshot_year:
                raise CurationConfigError(
                    f"{version_label}.snapshot_date must begin with snapshot_year"
                )
            temporal_coverage = clean_text(
                str(version_mapping.get("temporal_coverage", ""))
            )
            relative_path = Path(_required_text(version_mapping, "path", version_label))
            source_path = (source_dir / relative_path).resolve()
            if not source_path.is_relative_to(source_dir):
                raise CurationConfigError(
                    f"{version_label}.path escapes source_directory"
                )
            if source_path in seen_source_paths:
                raise CurationConfigError(f"Duplicate source path: {source_path}")
            seen_source_paths.add(source_path)
            output_stem = clean_text(str(version_mapping.get("output_name", "")))
            output_stem = output_stem or f"mncommons_{_slug(series_id)}_{snapshot_year}"
            if output_stem.casefold().endswith(".gpkg"):
                output_stem = output_stem[:-5]
            if Path(output_stem).name != output_stem:
                raise CurationConfigError(
                    f"{version_label}.output_name cannot contain a path"
                )
            if output_stem in seen_output_stems:
                raise CurationConfigError(f"Duplicate output name: {output_stem}")
            seen_output_stems.add(output_stem)
            parsed_versions.append(
                VersionSpec(
                    series_id=series_id,
                    snapshot_year=snapshot_year,
                    snapshot_date=snapshot_date,
                    temporal_coverage=temporal_coverage,
                    source_path=source_path,
                    output_stem=output_stem,
                    description_prefix=clean_text(
                        str(version_mapping.get("description_prefix", ""))
                    ),
                    metadata_overrides=_metadata_overrides(
                        version_mapping, version_label
                    ),
                )
            )
        parsed_series.append(
            SeriesSpec(
                series_id=series_id,
                title=_required_text(series_mapping, "title", label),
                catalog_title=clean_text(
                    str(series_mapping.get("catalog_title", ""))
                ),
                resource_guid=expected_guid,
                spatial_coverage=_required_text(
                    series_mapping, "spatial_coverage", label
                ),
                metadata_overrides=_metadata_overrides(series_mapping, label),
                versions=tuple(parsed_versions),
            )
        )

    return JobConfig(
        config_path=config_path,
        job_id=job_id,
        source_dir=source_dir,
        work_dir=work_dir,
        collection=collection,
        metadata=metadata,
        required_review_fields=required_review_fields,
        series=tuple(parsed_series),
        pmtiles_config=pmtiles_config,
    )


def _only_path(paths: Iterable[Path], label: str) -> Path:
    values = tuple(sorted(paths))
    if len(values) != 1:
        raise RuntimeError(f"Expected exactly one {label}; found {len(values)}")
    return values[0]


def discover_resource_package(version: VersionSpec) -> ResourcePackage:
    """Resolve and require the preservation inputs for one resource."""
    resource = version.source_path
    if not resource.is_dir():
        raise RuntimeError(f"Resource directory does not exist: {resource}")
    fgdb_dir = resource / "fgdb"
    metadata_dir = resource / "metadata"
    data_resource_xml = resource / "dataResource.xml"
    metadata_xml = metadata_dir / "metadata.xml"
    metadata_html = metadata_dir / "metadata.html"
    preview_path = metadata_dir / "preview.jpg"
    preview = preview_path if preview_path.is_file() else None
    missing = [
        path
        for path in (data_resource_xml, metadata_xml, metadata_html)
        if not path.is_file()
    ]
    if missing:
        raise RuntimeError(
            f"Required source files are missing from {resource}: "
            + ", ".join(str(path.relative_to(resource)) for path in missing)
        )
    geodatabase = _only_path(
        (
            path
            for path in fgdb_dir.iterdir()
            if path.is_dir() and path.suffix.casefold() == ".gdb"
        ),
        f"File Geodatabase in {fgdb_dir}",
    )
    gpkg_dir = fgdb_dir / "gpkg"
    geopackages = (
        tuple(
            path
            for path in gpkg_dir.iterdir()
            if path.is_file() and path.suffix.casefold() == ".gpkg"
        )
        if gpkg_dir.is_dir()
        else ()
    )
    if len(geopackages) > 1:
        raise RuntimeError(
            f"Expected at most one GeoPackage in {gpkg_dir}; found {len(geopackages)}"
        )
    layer_files = tuple(
        sorted(
            path
            for path in fgdb_dir.iterdir()
            if path.is_file() and path.suffix.casefold() in {".lyr", ".lyrx"}
        )
    )
    return ResourcePackage(
        version=version,
        data_resource_xml=data_resource_xml,
        metadata_xml=metadata_xml,
        metadata_html=metadata_html,
        preview=preview,
        geodatabase=geodatabase,
        geopackage=geopackages[0] if geopackages else None,
        layer_files=layer_files,
    )


def parse_data_resource(path: Path) -> dict[str, str]:
    """Read stable identity and currency hints from ``dataResource.xml``."""
    root = ET.parse(path).getroot()
    values: dict[str, str] = {}
    wanted = {
        "resourceguid": "resource_guid",
        "basename": "base_name",
        "currentasofdate": "current_as_of",
        "descriptivename": "descriptive_name",
        "publisher": "publisher",
    }
    for element in root.iter():
        output_name = wanted.get(local_name(element.tag))
        if output_name and output_name not in values:
            value = clean_text(element.text)
            if value:
                values[output_name] = value
    values["resource_guid"] = normalize_guid(values.get("resource_guid", ""))
    values["current_as_of"] = normalize_fgdc_date(values.get("current_as_of", ""))
    return values


def inspect_vector(path: Path) -> dict[str, Any]:
    """Inspect all user-visible layers with GDAL's JSON interface."""
    result = subprocess.run(
        ["ogrinfo", "-ro", "-json", "-so", str(path)],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"ogrinfo could not inspect {path}: {result.stderr.strip()}")
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"ogrinfo returned invalid JSON for {path}") from exc
    layers = payload.get("layers")
    if not isinstance(layers, list) or not layers:
        raise RuntimeError(f"No readable layers found in {path}")
    return payload


def sqlite_integrity(path: Path) -> str:
    """Return SQLite's integrity result for a GeoPackage."""
    try:
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            row = connection.execute("PRAGMA integrity_check").fetchone()
        finally:
            connection.close()
    except sqlite3.Error as exc:
        return f"error: {exc}"
    return str(row[0]) if row else "no result"


def geopackage_has_embedded_metadata(path: Path) -> bool:
    """Return whether both standard GeoPackage metadata tables contain rows."""
    try:
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            tables = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            if not {"gpkg_metadata", "gpkg_metadata_reference"}.issubset(tables):
                return False
            metadata_count = connection.execute(
                "SELECT count(*) FROM gpkg_metadata"
            ).fetchone()[0]
            reference_count = connection.execute(
                "SELECT count(*) FROM gpkg_metadata_reference"
            ).fetchone()[0]
        finally:
            connection.close()
    except sqlite3.Error:
        return False
    return bool(metadata_count and reference_count)


def _layers(inspection: dict[str, Any]) -> list[dict[str, Any]]:
    return [layer for layer in inspection.get("layers", []) if isinstance(layer, dict)]


def _layer_summary(inspection: dict[str, Any]) -> list[dict[str, Any]]:
    summary = []
    for layer in _layers(inspection):
        geometry_fields = layer.get("geometryFields") or []
        geometry = geometry_fields[0] if geometry_fields else {}
        coordinate_system = geometry.get("coordinateSystem") or {}
        projjson = coordinate_system.get("projjson") or {}
        identifier = projjson.get("id") or {}
        summary.append(
            {
                "name": str(layer.get("name", "")),
                "feature_count": layer.get("featureCount"),
                "geometry_type": str(geometry.get("type", "")),
                "extent": geometry.get("extent") or [],
                "crs_name": str(projjson.get("name", "")),
                "crs_authority": str(identifier.get("authority", "")),
                "crs_code": str(identifier.get("code", "")),
                "fields": [
                    {
                        "name": str(field.get("name", "")),
                        "type": str(field.get("type", "")),
                        "nullable": field.get("nullable"),
                    }
                    for field in layer.get("fields") or []
                    if isinstance(field, dict)
                ],
            }
        )
    return summary


def _extents_equal(left: list[Any], right: list[Any]) -> bool:
    if len(left) != len(right):
        return False
    try:
        return all(
            math.isclose(float(a), float(b), rel_tol=1e-11, abs_tol=1e-6)
            for a, b in zip(left, right, strict=True)
        )
    except (TypeError, ValueError):
        return left == right


def compare_vector_datasets(
    source: dict[str, Any],
    candidate: dict[str, Any],
) -> tuple[list[str], list[str]]:
    """Compare preservation-significant structural properties."""
    errors: list[str] = []
    warnings: list[str] = []
    source_layers = _layer_summary(source)
    candidate_layers = _layer_summary(candidate)
    if len(source_layers) != len(candidate_layers):
        errors.append(
            f"layer_count_mismatch:{len(source_layers)}!={len(candidate_layers)}"
        )
        return warnings, errors
    for source_layer, candidate_layer in zip(
        source_layers, candidate_layers, strict=True
    ):
        label = source_layer["name"] or "unnamed_layer"
        for key in ("name", "feature_count", "geometry_type", "crs_name"):
            if source_layer[key] != candidate_layer[key]:
                errors.append(
                    f"{label}:{key}_mismatch:{source_layer[key]}!={candidate_layer[key]}"
                )
        if not _extents_equal(source_layer["extent"], candidate_layer["extent"]):
            errors.append(f"{label}:extent_mismatch")
        source_fields = [
            (field["name"], field["type"]) for field in source_layer["fields"]
        ]
        candidate_fields = [
            (field["name"], field["type"]) for field in candidate_layer["fields"]
        ]
        if source_fields != candidate_fields:
            errors.append(f"{label}:field_schema_mismatch")
        source_nullable = {
            field["name"]: field["nullable"] for field in source_layer["fields"]
        }
        candidate_nullable = {
            field["name"]: field["nullable"] for field in candidate_layer["fields"]
        }
        for field_name in sorted(source_nullable.keys() & candidate_nullable.keys()):
            if source_nullable[field_name] != candidate_nullable[field_name]:
                warnings.append(f"{label}:{field_name}:nullability_changed")
    return warnings, errors


def evaluate_geopackage(
    package: ResourcePackage,
    source_inspection: dict[str, Any],
    inspector: VectorInspector = inspect_vector,
) -> dict[str, Any]:
    """Inventory a supplied GDRS GeoPackage as an ignored derivative."""
    if package.geopackage is None:
        return {
            "status": "not_supplied",
            "embedded_metadata": False,
            "warnings": [],
            "errors": [],
            "inspection": None,
        }
    integrity = sqlite_integrity(package.geopackage)
    if integrity != "ok":
        return {
            "status": "supplied_derivative_invalid",
            "embedded_metadata": geopackage_has_embedded_metadata(package.geopackage),
            "warnings": [],
            "errors": [f"sqlite_integrity:{integrity}"],
            "inspection": None,
        }
    try:
        candidate_inspection = inspector(package.geopackage)
    except RuntimeError as exc:
        return {
            "status": "supplied_derivative_invalid",
            "embedded_metadata": geopackage_has_embedded_metadata(package.geopackage),
            "warnings": [],
            "errors": [str(exc)],
            "inspection": None,
        }
    warnings, errors = compare_vector_datasets(source_inspection, candidate_inspection)
    return {
        "status": (
            "supplied_derivative_validated"
            if not errors
            else "supplied_derivative_invalid"
        ),
        "embedded_metadata": geopackage_has_embedded_metadata(package.geopackage),
        "warnings": warnings,
        "errors": errors,
        "inspection": candidate_inspection,
    }


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def directory_sha256(path: Path) -> str:
    """Hash a directory as ordered relative paths plus file digests."""
    digest = hashlib.sha256()
    files = sorted(
        item for item in path.rglob("*") if item.is_file() and item.name != ".DS_Store"
    )
    for item in files:
        relative = item.relative_to(path).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(file_sha256(item).encode("ascii"))
        digest.update(b"\0")
    return digest.hexdigest()


def directory_size(path: Path) -> int:
    return sum(
        item.stat().st_size
        for item in path.rglob("*")
        if item.is_file() and item.name != ".DS_Store"
    )


def portable_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return str(path.resolve())


def generate_curated_id(existing_ids: set[str]) -> str:
    while True:
        suffix = "".join(secrets.choice(NANOID_ALPHABET) for _ in range(NANOID_LENGTH))
        candidate = f"b1g_{suffix}"
        if candidate not in existing_ids:
            existing_ids.add(candidate)
            return candidate


def load_manifest(job: JobConfig, *, required: bool = True) -> dict[str, Any]:
    if not job.manifest_path.is_file():
        if required:
            raise RuntimeError(
                f"Manifest not found; run validate first: {job.manifest_path}"
            )
        return {}
    return json.loads(job.manifest_path.read_text(encoding="utf-8"))


def write_manifest(job: JobConfig, manifest: dict[str, Any]) -> None:
    job.work_dir.mkdir(parents=True, exist_ok=True)
    manifest["updated_at"] = utc_now()
    temporary = job.manifest_path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    temporary.replace(job.manifest_path)


def _existing_version_ids(manifest: dict[str, Any]) -> dict[str, str]:
    return {
        str(row.get("source_key", "")): str(row.get("curated_id", ""))
        for row in manifest.get("records", [])
        if str(row.get("curated_id", "")).startswith("b1g_")
    }


def assigned_ids(job: JobConfig) -> dict[str, str]:
    manifest = load_manifest(job, required=False)
    version_ids = _existing_version_ids(manifest)
    used = {job.collection.collection_id, *version_ids.values()}
    for series in job.series:
        for version in series.versions:
            version_ids.setdefault(version.source_key, generate_curated_id(used))
    return version_ids


def config_sha256(job: JobConfig) -> str:
    return file_sha256(job.config_path)


def _series_for(job: JobConfig, series_id: str) -> SeriesSpec:
    return next(series for series in job.series if series.series_id == series_id)


def inspect_job(
    job: JobConfig,
    *,
    inspector: VectorInspector = inspect_vector,
) -> list[dict[str, Any]]:
    """Validate every source package and return structured inspection records."""
    records: list[dict[str, Any]] = []
    for version in job.versions:
        series = _series_for(job, version.series_id)
        package = discover_resource_package(version)
        resource_metadata = parse_data_resource(package.data_resource_xml)
        actual_guid = resource_metadata.get("resource_guid", "")
        if actual_guid != series.resource_guid:
            raise RuntimeError(
                f"Resource GUID mismatch for {version.source_key}: "
                f"expected {series.resource_guid}, found {actual_guid or 'blank'}"
            )
        source_inspection = inspector(package.geodatabase)
        gpkg_evaluation = evaluate_geopackage(
            package,
            source_inspection,
            inspector=inspector,
        )
        records.append(
            {
                "version": version,
                "series": series,
                "package": package,
                "data_resource": resource_metadata,
                "source_inspection": source_inspection,
                "geopackage": gpkg_evaluation,
            }
        )
    return records


def _base_manifest(
    job: JobConfig,
    inspections: list[dict[str, Any]],
    version_ids: dict[str, str],
) -> dict[str, Any]:
    previous = load_manifest(job, required=False)
    current_config_hash = config_sha256(job)
    config_changed = bool(
        previous.get("config_sha256")
        and previous.get("config_sha256") != current_config_hash
    )
    manual_review = (
        {"status": "pending", "reason": "configuration_changed"}
        if config_changed
        else previous.get("manual_review", {"status": "pending"})
    )
    return {
        "version": 1,
        "job_id": job.job_id,
        "config_path": portable_path(job.config_path),
        "source_directory": portable_path(job.source_dir),
        "work_directory": portable_path(job.work_dir),
        "metadata_path": portable_path(job.metadata_path),
        "collection_metadata_path": portable_path(job.collection_metadata_path),
        "inventory_path": portable_path(job.inventory_path),
        "config_sha256": current_config_hash,
        "created_at": previous.get("created_at", utc_now()),
        "manual_review": manual_review,
        "stages": {} if config_changed else previous.get("stages", {}),
        "collection": {
            "curated_id": job.collection.collection_id,
            "title": job.collection.title,
        },
        "series": [
            {
                "series_id": series.series_id,
                "title": series.title,
                "resource_guid": series.resource_guid,
            }
            for series in job.series
        ],
        "records": [
            {
                "source_key": row["version"].source_key,
                "series_id": row["version"].series_id,
                "snapshot_year": row["version"].snapshot_year,
                "curated_id": version_ids[row["version"].source_key],
                "filename": row["version"].filename,
                "source_resource_path": portable_path(row["version"].source_path),
                "resource_guid": row["data_resource"].get("resource_guid", ""),
                "geodatabase_path": portable_path(row["package"].geodatabase),
                "geopackage_path": (
                    portable_path(row["package"].geopackage)
                    if row["package"].geopackage
                    else ""
                ),
                "geopackage_status": row["geopackage"]["status"],
                "geopackage_embedded_metadata": row["geopackage"]["embedded_metadata"],
                "layer_files": [
                    portable_path(path) for path in row["package"].layer_files
                ],
                "validation_warnings": row["geopackage"]["warnings"],
                "validation_errors": row["geopackage"]["errors"],
            }
            for row in inspections
        ],
    }


def run_validate_stage(
    job: JobConfig,
    *,
    inspector: VectorInspector = inspect_vector,
) -> list[dict[str, Any]]:
    inspections = inspect_job(job, inspector=inspector)
    version_ids = assigned_ids(job)
    manifest = _base_manifest(job, inspections, version_ids)
    manifest.setdefault("stages", {})["validate"] = {
        "status": "completed",
        "completed_at": utc_now(),
        "resources": len(inspections),
        "supplied_geopackages": sum(
            row["package"].geopackage is not None for row in inspections
        ),
        "validated_supplied_geopackages": sum(
            row["geopackage"]["status"] == "supplied_derivative_validated"
            for row in inspections
        ),
        "geopackages_to_generate": len(inspections),
    }
    write_manifest(job, manifest)
    return inspections


def _write_csv(
    path: Path, fieldnames: Iterable[str], rows: Iterable[dict[str, Any]]
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=tuple(fieldnames), extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(rows)


def _joined_layer_values(inspection: dict[str, Any], key: str) -> str:
    return "|".join(str(layer.get(key, "")) for layer in _layer_summary(inspection))


def run_inventory_stage(
    job: JobConfig,
    *,
    inspector: VectorInspector = inspect_vector,
) -> Path:
    """Write a fixity-bearing inventory of every configured source package."""
    inspections = run_validate_stage(job, inspector=inspector)
    rows = []
    for record in inspections:
        package: ResourcePackage = record["package"]
        gdb = package.geodatabase
        gpkg = package.geopackage
        layers = _layer_summary(record["source_inspection"])
        rows.append(
            {
                "series_id": package.version.series_id,
                "snapshot_year": package.version.snapshot_year,
                "source_resource_path": portable_path(package.version.source_path),
                "resource_guid": record["data_resource"].get("resource_guid", ""),
                "base_name": record["data_resource"].get("base_name", ""),
                "geodatabase_path": portable_path(gdb),
                "geodatabase_size_bytes": directory_size(gdb),
                "geodatabase_sha256": directory_sha256(gdb),
                "geopackage_path": portable_path(gpkg) if gpkg else "",
                "geopackage_size_bytes": gpkg.stat().st_size if gpkg else "",
                "geopackage_sha256": file_sha256(gpkg) if gpkg else "",
                "geopackage_status": record["geopackage"]["status"],
                "geopackage_embedded_metadata": record["geopackage"][
                    "embedded_metadata"
                ],
                "layer_count": len(layers),
                "layers": "|".join(layer["name"] for layer in layers),
                "feature_counts": "|".join(
                    str(layer["feature_count"]) for layer in layers
                ),
                "geometry_types": "|".join(layer["geometry_type"] for layer in layers),
                "metadata_xml_path": portable_path(package.metadata_xml),
                "metadata_xml_sha256": file_sha256(package.metadata_xml),
                "metadata_html_path": portable_path(package.metadata_html),
                "preview_path": portable_path(package.preview) if package.preview else "",
                "layer_file_count": len(package.layer_files),
                "layer_files": "|".join(
                    portable_path(path) for path in package.layer_files
                ),
                "layer_file_sha256": "|".join(
                    file_sha256(path) for path in package.layer_files
                ),
                "validation_warnings": "|".join(record["geopackage"]["warnings"]),
                "validation_errors": "|".join(record["geopackage"]["errors"]),
            }
        )
    _write_csv(job.inventory_path, INVENTORY_COLUMNS, rows)
    manifest = load_manifest(job)
    manifest.setdefault("stages", {})["inventory"] = {
        "status": "completed",
        "completed_at": utc_now(),
        "output": portable_path(job.inventory_path),
        "records": len(rows),
    }
    write_manifest(job, manifest)
    return job.inventory_path


def _resource_types(inspection: dict[str, Any]) -> str:
    values = []
    for layer in _layer_summary(inspection):
        normalized = layer["geometry_type"].replace(" ", "").casefold()
        value = GEOMETRY_RESOURCE_TYPES.get(normalized)
        if value and value not in values:
            values.append(value)
    return "|".join(values)


def _inspection_crs(inspection: dict[str, Any]) -> str:
    for layer in _layer_summary(inspection):
        authority = layer["crs_authority"]
        code = layer["crs_code"]
        if authority.casefold() == "epsg" and code:
            return f"https://spatialreference.org/ref/epsg/{code}/"
        if layer["crs_name"]:
            return layer["crs_name"]
    return ""


def _human_file_size(size: int | None) -> str:
    if size is None:
        return ""
    units = ("bytes", "KB", "MB", "GB", "TB")
    value = float(size)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{int(value)} {unit}" if unit == "bytes" else f"{value:.2f} {unit}"
        value /= 1024
    return ""


def _metadata_review_flags(
    job: JobConfig,
    version: VersionSpec,
    fgdc: FgdcMetadata,
    record: dict[str, Any],
    temporal_coverage: str,
) -> list[str]:
    flags = []
    if not fgdc.title:
        flags.append("missing_fgdc_title")
    if not temporal_coverage:
        flags.append("missing_temporal_coverage")
    elif not version.temporal_coverage and fgdc.temporal_source.endswith("fallback"):
        flags.append("temporal_coverage_falls_back_to_publication_date")
    temporal_match = re.search(r"\b(\d{4})\b", temporal_coverage)
    temporal_year = temporal_match.group(1) if temporal_match else ""
    if fgdc.publication_date and not re.fullmatch(
        r"\d{4}(?:-\d{2}(?:-\d{2})?)?", fgdc.publication_date
    ):
        flags.append("date_issued_is_not_an_iso_date")
    if temporal_year and temporal_year != str(version.snapshot_year):
        flags.append(
            f"snapshot_year_{version.snapshot_year}_differs_from_content_year_{temporal_year}"
        )
    if (
        fgdc.publication_date
        and temporal_coverage
        and fgdc.publication_date not in temporal_coverage
    ):
        flags.append("publication_date_differs_from_temporal_coverage")
    if not fgdc.bounding_box:
        flags.append("missing_fgdc_bounding_box")
    return list(dict.fromkeys(flags))


def _aardvark_row(
    job: JobConfig,
    record: dict[str, Any],
    version_id: str,
) -> dict[str, Any]:
    version: VersionSpec = record["version"]
    series: SeriesSpec = record["series"]
    package: ResourcePackage = record["package"]
    fgdc = parse_fgdc_metadata(package.metadata_xml)
    data_resource = record["data_resource"]
    temporal_coverage = version.temporal_coverage or fgdc.temporal_coverage
    temporal_years = re.findall(r"\b(\d{4})\b", temporal_coverage)
    temporal_year = temporal_years[0] if temporal_years else ""
    title_date = temporal_coverage or temporal_year or str(version.snapshot_year)
    source_title = fgdc.title or series.title
    controlled_place = series.spatial_coverage.split("|")[0]
    catalog_title = series.catalog_title or source_title
    title = f"{catalog_title} [{controlled_place}] {{{title_date}}}"
    rights = fgdc.use_constraints or "Review original FGDC use constraints."
    output_geopackage = job.output_geopackage(version)
    gpkg_size = output_geopackage.stat().st_size if output_geopackage.is_file() else None
    inspected_crs = _inspection_crs(record["source_inspection"])
    coordinate_reference_system = (
        inspected_crs
        if inspected_crs.startswith("https://spatialreference.org/")
        else fgdc.spatial_reference or inspected_crs
    )
    identifiers = [
        f"urn:uuid:{series.resource_guid}",
        f"gdrs-snapshot:{series.series_id}:{version.snapshot_year}",
    ]
    row = {field: "" for field in PRIMARY_FIELD_ORDER}
    row.update(
        {
            "ID": version_id,
            "Code": job.metadata.code,
            "Title": title,
            "Alternative Title": source_title,
            "Description": fgdc.description,
            "Language": "eng",
            "Display Note": job.metadata.display_note,
            "Creator": fgdc.creator,
            "Publisher": job.metadata.publisher,
            "Provider": job.metadata.provider,
            "Resource Class": "Datasets",
            "Resource Type": _resource_types(record["source_inspection"]),
            "Keyword": "|".join(
                dict.fromkeys((*fgdc.theme_keywords, *fgdc.place_keywords))
            ),
            "Local Collection": f"{job.metadata.series_label_prefix} {series.title}",
            "Temporal Coverage": temporal_coverage,
            "Date Issued": fgdc.publication_date,
            "Index Year": temporal_year,
            "Date Range": (
                f"{temporal_years[0]}-{temporal_years[-1]}"
                if version.temporal_coverage and temporal_years
                else fgdc.date_range
            ),
            "Spatial Coverage": series.spatial_coverage,
            "Bounding Box": fgdc.bounding_box,
            "Geometry": fgdc.geometry,
            "Centroid": fgdc.centroid,
            "Coordinate Reference System": coordinate_reference_system,
            "Access Rights": job.metadata.access_rights,
            "Rights": rights,
            "File Size": _human_file_size(gpkg_size),
            "Format": "GeoPackage",
            "Member Of": job.collection.collection_id,
            "Is Version Of": "",
            "Date Accessioned": job.metadata.accession_date,
            "Publication State": job.metadata.publication_state,
            "Identifier": "|".join(identifiers),
            "Provenance": (
                f"Captured in the {version.snapshot_date} Minnesota Geospatial "
                "Commons snapshot and curated from the preserved File Geodatabase."
            ),
            "Website Platform": "Minnesota Geospatial Commons",
            "Accrual Method": "Mediated deposit",
            "Harvest Workflow": "curation_mncommons",
            "Admin Note": (
                f"Snapshot date: {version.snapshot_date}; source resource GUID: "
                f"{series.resource_guid}."
            ),
        }
    )
    row.update(series.metadata_overrides)
    row.update(version.metadata_overrides)
    if version.description_prefix:
        description = clean_text(str(row.get("Description", "")))
        if not description.startswith(version.description_prefix):
            row["Description"] = clean_text(
                f"{version.description_prefix} {description}"
            )
    flags = _metadata_review_flags(
        job, version, fgdc, record, clean_text(str(row["Temporal Coverage"]))
    )
    row.update(
        {
            "filename": version.filename,
            "series_id": series.series_id,
            "snapshot_year": str(version.snapshot_year),
            "snapshot_date": version.snapshot_date,
            "source_resource_path": portable_path(version.source_path),
            "resource_guid": series.resource_guid,
            "gdrs_current_as_of": data_resource.get("current_as_of", ""),
            "fgdc_publication_date": fgdc.publication_date,
            "fgdc_calendar_date": "|".join(fgdc.calendar_dates),
            "fgdc_begin_date": fgdc.begin_date,
            "fgdc_end_date": fgdc.end_date,
            "fgdc_metadata_date": fgdc.metadata_date,
            "proposed_temporal_source": (
                "job_config" if version.temporal_coverage else fgdc.temporal_source
            ),
            "geopackage_status": record["geopackage"]["status"],
            "metadata_review_flags": "|".join(flags),
        }
    )
    return row


def _collection_aardvark_row(job: JobConfig) -> dict[str, Any]:
    """Build the standalone collection record referenced by the child datasets."""
    collection = job.collection
    row = {field: "" for field in PRIMARY_FIELD_ORDER}
    row.update(
        {
            "ID": collection.collection_id,
            "Code": job.metadata.code,
            "Title": collection.title,
            "Description": collection.description,
            "Language": "eng",
            "Creator": collection.creator,
            "Publisher": job.metadata.publisher,
            "Provider": job.metadata.provider,
            "Resource Class": "Collections",
            "Temporal Coverage": collection.temporal_coverage,
            "Date Issued": collection.date_issued,
            "Date Range": collection.temporal_coverage,
            "Spatial Coverage": collection.spatial_coverage,
            "Access Rights": job.metadata.access_rights,
            "Rights": collection.rights,
            "Date Accessioned": job.metadata.accession_date,
            "Publication State": job.metadata.publication_state,
            "Identifier": f"gdrs-archive-collection:{job.job_id}",
            "Provenance": (
                "Created to describe the pilot archival collection assembled from "
                "Minnesota Geospatial Commons snapshots."
            ),
            "Website Platform": "Minnesota Geospatial Commons",
            "Accrual Method": "Mediated deposit",
            "Harvest Workflow": "curation_mncommons",
        }
    )
    row.update(collection.metadata_overrides)
    return row


def run_metadata_stage(
    job: JobConfig,
    *,
    inspector: VectorInspector = inspect_vector,
) -> Path:
    """Write a draft Aardvark CSV and reset the manual checkpoint."""
    inspections = run_validate_stage(job, inspector=inspector)
    manifest = load_manifest(job)
    version_ids = _existing_version_ids(manifest)
    rows = [
        _aardvark_row(
            job,
            record,
            version_ids[record["version"].source_key],
        )
        for record in inspections
    ]
    columns = ("filename", *PRIMARY_FIELD_ORDER, *REVIEW_COLUMNS)
    _write_csv(job.metadata_path, columns, rows)
    _write_csv(
        job.collection_metadata_path,
        PRIMARY_FIELD_ORDER,
        [_collection_aardvark_row(job)],
    )
    manifest["manual_review"] = {
        "status": "pending",
        "reason": "metadata_generated_or_replaced",
    }
    manifest.setdefault("stages", {})["metadata"] = {
        "status": "completed",
        "completed_at": utc_now(),
        "output": portable_path(job.metadata_path),
        "collection_output": portable_path(job.collection_metadata_path),
        "records": len(rows),
        "collection_records": 1,
    }
    manifest["stages"].pop("review", None)
    write_manifest(job, manifest)
    return job.metadata_path


def read_metadata_csv(job: JobConfig) -> list[dict[str, str]]:
    if not job.metadata_path.is_file():
        raise RuntimeError(f"Metadata CSV does not exist: {job.metadata_path}")
    with job.metadata_path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def read_collection_metadata_csv(job: JobConfig) -> list[dict[str, str]]:
    if not job.collection_metadata_path.is_file():
        raise RuntimeError(
            f"Collection metadata CSV does not exist: {job.collection_metadata_path}"
        )
    with job.collection_metadata_path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def validate_reviewed_metadata(job: JobConfig) -> list[dict[str, str]]:
    rows = read_metadata_csv(job)
    expected = {version.filename for version in job.versions}
    actual = {clean_text(row.get("filename", "")) for row in rows}
    if actual != expected or len(rows) != len(expected):
        raise RuntimeError(
            f"Metadata filenames must match the job. Expected {sorted(expected)}; "
            f"found {sorted(actual)}"
        )
    missing_columns = [
        field
        for field in job.required_review_fields
        if not rows or field not in rows[0]
    ]
    if missing_columns:
        raise RuntimeError(
            "Metadata CSV is missing review columns: " + ", ".join(missing_columns)
        )
    blanks = []
    for row in rows:
        blank_fields = [
            field
            for field in job.required_review_fields
            if not clean_text(row.get(field, ""))
        ]
        if blank_fields:
            blanks.append(f"{row.get('filename', '')}: {', '.join(blank_fields)}")
    if blanks:
        raise RuntimeError("Required review fields are blank: " + " | ".join(blanks))

    collection_rows = read_collection_metadata_csv(job)
    if len(collection_rows) != 1:
        raise RuntimeError("Collection metadata CSV must contain exactly one record")
    collection_row = collection_rows[0]
    if clean_text(collection_row.get("ID", "")) != job.collection.collection_id:
        raise RuntimeError(
            "Collection metadata ID must match collection.id in the job configuration"
        )
    collection_blanks = [
        field
        for field in COLLECTION_REQUIRED_REVIEW_FIELDS
        if not clean_text(collection_row.get(field, ""))
    ]
    if collection_blanks:
        raise RuntimeError(
            "Required collection review fields are blank: "
            + ", ".join(collection_blanks)
        )
    wrong_members = [
        row.get("filename", "")
        for row in rows
        if clean_text(row.get("Member Of", "")) != job.collection.collection_id
    ]
    if wrong_members:
        raise RuntimeError(
            "Child Member Of values must match the collection ID: "
            + ", ".join(wrong_members)
        )
    return rows


def confirm_review(job: JobConfig) -> None:
    validate_reviewed_metadata(job)
    manifest = load_manifest(job)
    checksum = file_sha256(job.metadata_path)
    collection_checksum = file_sha256(job.collection_metadata_path)
    manifest["manual_review"] = {
        "status": "confirmed",
        "confirmed_at": utc_now(),
        "metadata_sha256": checksum,
        "collection_metadata_sha256": collection_checksum,
    }
    manifest.setdefault("stages", {})["review"] = {
        "status": "completed",
        "completed_at": utc_now(),
        "metadata_sha256": checksum,
        "collection_metadata_sha256": collection_checksum,
    }
    write_manifest(job, manifest)


def require_confirmed_review(job: JobConfig) -> dict[str, Any]:
    manifest = load_manifest(job)
    review = manifest.get("manual_review", {})
    if review.get("status") != "confirmed":
        raise RuntimeError("Metadata review has not been confirmed")
    expected = str(review.get("metadata_sha256", ""))
    actual = file_sha256(job.metadata_path)
    if not expected or expected != actual:
        raise RuntimeError(
            "Metadata changed after review; inspect it and run review --confirm again"
        )
    expected_collection = str(review.get("collection_metadata_sha256", ""))
    actual_collection = file_sha256(job.collection_metadata_path)
    if not expected_collection or expected_collection != actual_collection:
        raise RuntimeError(
            "Collection metadata changed after review; inspect it and run "
            "review --confirm again"
        )
    return manifest


def mark_stage(
    job: JobConfig,
    stage: str,
    *,
    details: dict[str, Any] | None = None,
) -> None:
    manifest = load_manifest(job)
    value: dict[str, Any] = {"status": "completed", "completed_at": utc_now()}
    if details:
        value.update(details)
    manifest.setdefault("stages", {})[stage] = value
    write_manifest(job, manifest)


def _copy_file(source: Path, destination: Path, *, overwrite: bool) -> str:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if file_sha256(source) == file_sha256(destination):
            return "existing_identical"
        if not overwrite:
            raise RuntimeError(
                f"Destination differs and --overwrite was not supplied: {destination}"
            )
    temporary = destination.with_name(f".{destination.name}.tmp")
    shutil.copy2(source, temporary)
    temporary.replace(destination)
    return "copied"


def _zip_geodatabase(source: Path, destination: Path, *, overwrite: bool) -> str:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and not overwrite:
        try:
            with zipfile.ZipFile(destination) as archive:
                if archive.testzip() is None:
                    return "existing_valid"
        except zipfile.BadZipFile:
            pass
        raise RuntimeError(
            f"Existing geodatabase ZIP is invalid; rerun with --overwrite: {destination}"
        )
    temporary = destination.with_name(f".{destination.name}.tmp")
    temporary.unlink(missing_ok=True)
    try:
        with zipfile.ZipFile(
            temporary,
            mode="w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=6,
        ) as archive:
            for path in sorted(item for item in source.rglob("*") if item.is_file()):
                archive.write(
                    path,
                    arcname=(Path(source.name) / path.relative_to(source)).as_posix(),
                )
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)
    return "created"


def run_preserve_stage(job: JobConfig, *, overwrite: bool = False) -> None:
    """Preserve source masters while excluding GDRS-generated GeoPackages."""
    require_confirmed_review(job)
    outputs: list[dict[str, Any]] = []
    for version in job.versions:
        package = discover_resource_package(version)
        item_dir = job.item_dir(package.version)
        original_dir = item_dir / "original"
        removed_derivatives = []
        if original_dir.is_dir():
            for stale in sorted(original_dir.rglob("*.gpkg")):
                removed_derivatives.append(portable_path(stale))
                stale.unlink()
        gdb_zip = original_dir / f"{package.geodatabase.name}.zip"
        zip_status = _zip_geodatabase(
            package.geodatabase,
            gdb_zip,
            overwrite=overwrite,
        )
        copied = []
        for source in sorted(
            path
            for path in package.version.source_path.rglob("*")
            if path.is_file() and path.name != ".DS_Store"
        ):
            if source.is_relative_to(package.geodatabase):
                continue
            if source.suffix.casefold() in {".gpkg", ".gpkg-shm", ".gpkg-wal"}:
                continue
            destination = original_dir / source.relative_to(package.version.source_path)
            copied.append(
                {
                    "source": portable_path(source),
                    "destination": portable_path(destination),
                    "status": _copy_file(source, destination, overwrite=overwrite),
                }
            )
        outputs.append(
            {
                "source_key": package.version.source_key,
                "geodatabase_zip": portable_path(gdb_zip),
                "geodatabase_zip_status": zip_status,
                "removed_generated_geopackages": removed_derivatives,
                "copied_files": copied,
            }
        )
    mark_stage(job, "preserve", details={"records": outputs})


def _run_ogr2ogr(source: Path, destination: Path) -> None:
    temporary = destination.with_name(f".{destination.name}.tmp.gpkg")
    temporary.unlink(missing_ok=True)
    command = [
        "ogr2ogr",
        "-f",
        "GPKG",
        str(temporary),
        str(source),
        "-lco",
        "SPATIAL_INDEX=YES",
    ]
    result = subprocess.run(command, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        temporary.unlink(missing_ok=True)
        raise RuntimeError(
            f"ogr2ogr could not create {destination}: {result.stderr.strip()}"
        )
    temporary.replace(destination)


def _update_metadata_file_sizes(job: JobConfig) -> None:
    rows = read_metadata_csv(job)
    fieldnames = list(rows[0]) if rows else []
    versions_by_filename = {version.filename: version for version in job.versions}
    for row in rows:
        version = versions_by_filename.get(row.get("filename", ""))
        if version:
            path = job.output_geopackage(version)
            if path.is_file():
                row["File Size"] = _human_file_size(path.stat().st_size)
    _write_csv(job.metadata_path, fieldnames, rows)
    manifest = load_manifest(job)
    checksum = file_sha256(job.metadata_path)
    collection_checksum = file_sha256(job.collection_metadata_path)
    manifest.setdefault("manual_review", {})["metadata_sha256"] = checksum
    manifest.setdefault("manual_review", {})["collection_metadata_sha256"] = (
        collection_checksum
    )
    manifest.setdefault("stages", {}).setdefault("review", {})["metadata_sha256"] = (
        checksum
    )
    manifest.setdefault("stages", {}).setdefault("review", {})[
        "collection_metadata_sha256"
    ] = collection_checksum
    write_manifest(job, manifest)


def run_convert_stage(
    job: JobConfig,
    *,
    overwrite: bool = False,
    inspector: VectorInspector = inspect_vector,
    converter: Callable[[Path, Path], None] = _run_ogr2ogr,
) -> None:
    """Generate every curated access GeoPackage from its File Geodatabase."""
    require_confirmed_review(job)
    inspections = inspect_job(job, inspector=inspector)
    outputs = []
    for record in inspections:
        package: ResourcePackage = record["package"]
        destination = job.output_geopackage(package.version)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists() and not overwrite:
            candidate = inspector(destination)
            warnings, errors = compare_vector_datasets(
                record["source_inspection"], candidate
            )
            if sqlite_integrity(destination) != "ok" or errors:
                raise RuntimeError(
                    f"Existing work GeoPackage is invalid; rerun with --overwrite: {destination}"
                )
            action = "existing_valid"
        else:
            converter(package.geodatabase, destination)
            action = "generated_from_geodatabase"
            candidate = inspector(destination)
            warnings, errors = compare_vector_datasets(
                record["source_inspection"], candidate
            )
        if sqlite_integrity(destination) != "ok" or errors:
            raise RuntimeError(
                f"Output GeoPackage failed validation for {package.version.source_key}: "
                + " | ".join(errors)
            )
        outputs.append(
            {
                "source_key": package.version.source_key,
                "action": action,
                "output": portable_path(destination),
                "size_bytes": destination.stat().st_size,
                "sha256_before_metadata": file_sha256(destination),
                "warnings": warnings,
            }
        )
    _update_metadata_file_sizes(job)
    mark_stage(job, "convert", details={"records": outputs})


DICTIONARY_COLUMNS = (
    "friendlier_id",
    "layer_name",
    "field_name",
    "field_type",
    "values",
    "definition",
    "definition_source",
    "parent_field_name",
    "position",
    "match_status",
)


def _normalized_field_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.casefold())


def _fgdc_layer_descriptions(detail: str) -> dict[str, str]:
    """Parse numbered ``layer; description`` entries from FGDC eadetcit text."""
    matches = re.finditer(
        r"(?:^|\s)\d+\)\s*([^;\n]+?)\s*;\s*(.+?)(?=(?:\s+\d+\)\s)|$)",
        detail,
    )
    return {
        _normalized_field_name(match.group(1)): clean_text(match.group(2))
        for match in matches
    }


def _write_layer_guide(
    job: JobConfig,
    version: VersionSpec,
    layers: list[dict[str, Any]],
    fgdc: FgdcMetadata,
) -> Path | None:
    """Document the files representing each layer in a multilayer GeoPackage."""
    if len(layers) < 2:
        return None
    descriptions = _fgdc_layer_descriptions(fgdc.entity_attribute_detail)
    lines = [
        f"Layer guide: {version.filename}",
        "",
        (
            "This GeoPackage contains multiple related feature layers. Each layer "
            "has its own data dictionary and PMTiles derivative."
        ),
        "",
    ]
    for layer in layers:
        name = layer["name"]
        slug = _slug(name)
        description = descriptions.get(_normalized_field_name(name), "")
        lines.extend(
            [
                f"Layer: {name}",
                f"Description: {description or 'Not supplied in the source metadata.'}",
                f"Feature count: {layer['feature_count']}",
                f"Geometry type: {layer['geometry_type']}",
                f"Data dictionary: {slug}.csv",
                f"PMTiles: {version.output_stem}__{slug}.pmtiles",
                "",
            ]
        )
    if fgdc.entity_attribute_detail:
        lines.extend(
            [
                "Source FGDC layer documentation:",
                fgdc.entity_attribute_detail,
                "",
            ]
        )
    output = job.item_dir(version) / "layer-guide.txt"
    output.write_text("\n".join(lines), encoding="utf-8")
    return output


def run_dictionary_stage(
    job: JobConfig,
    *,
    inspector: VectorInspector = inspect_vector,
) -> None:
    """Join live GeoPackage schemas to FGDC attribute definitions."""
    require_confirmed_review(job)
    metadata_rows = {row["filename"]: row for row in read_metadata_csv(job)}
    outputs = []
    for version in job.versions:
        package = discover_resource_package(version)
        fgdc = parse_fgdc_metadata(package.metadata_xml)
        definitions: dict[str, Any] = {}
        for attribute in fgdc.attributes:
            definitions.setdefault(_normalized_field_name(attribute.label), attribute)
        gpkg = job.output_geopackage(version)
        if not gpkg.is_file():
            raise RuntimeError(f"Run convert before dictionaries; missing {gpkg}")
        inspection = inspector(gpkg)
        metadata_row = metadata_rows[version.filename]
        layers = _layer_summary(inspection)
        for layer in layers:
            dictionary_rows = []
            layer_matched_labels: set[str] = set()
            for position, field in enumerate(layer["fields"], start=1):
                normalized = _normalized_field_name(field["name"])
                attribute = definitions.get(normalized)
                if attribute:
                    layer_matched_labels.add(normalized)
                dictionary_rows.append(
                    {
                        "friendlier_id": metadata_row["ID"],
                        "layer_name": layer["name"],
                        "field_name": field["name"],
                        "field_type": field["type"],
                        "values": attribute.domain if attribute else "",
                        "definition": attribute.definition if attribute else "",
                        "definition_source": (
                            attribute.definition_source if attribute else ""
                        ),
                        "parent_field_name": "",
                        "position": position,
                        "match_status": "matched_fgdc"
                        if attribute
                        else "unmatched_field",
                    }
                )
            output = (
                job.item_dir(version)
                / "data-dictionaries"
                / f"{_slug(layer['name'])}.csv"
            )
            _write_csv(output, DICTIONARY_COLUMNS, dictionary_rows)
            outputs.append(
                {
                    "source_key": version.source_key,
                    "layer": layer["name"],
                    "output": portable_path(output),
                    "fields": len(dictionary_rows),
                    "matched_fields": sum(
                        row["match_status"] == "matched_fgdc" for row in dictionary_rows
                    ),
                    "unmatched_fgdc_attributes": sorted(
                        attribute.label
                        for attribute in fgdc.attributes
                        if _normalized_field_name(attribute.label)
                        not in layer_matched_labels
                    ),
                }
            )
        guide = _write_layer_guide(job, version, layers, fgdc)
        if guide is not None:
            outputs.append(
                {
                    "source_key": version.source_key,
                    "layer": "*",
                    "output": portable_path(guide),
                    "fields": 0,
                    "matched_fields": 0,
                    "unmatched_fgdc_attributes": [],
                }
            )
    mark_stage(job, "dictionaries", details={"outputs": outputs})


FGDC_STANDARD_URI = "https://www.fgdc.gov/metadata/csdgm/"


def append_fgdc_metadata(gpkg: Path, fgdc_xml: str) -> None:
    """Append the original FGDC XML after QGIS metadata has been embedded."""
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with sqlite3.connect(gpkg) as connection:
        feature_tables = [
            row[0]
            for row in connection.execute(
                "SELECT table_name FROM gpkg_contents WHERE data_type='features' ORDER BY table_name"
            )
        ]
        cursor = connection.execute(
            "INSERT INTO gpkg_metadata (md_scope, md_standard_uri, mime_type, metadata) "
            "VALUES (?, ?, ?, ?)",
            ("dataset", FGDC_STANDARD_URI, "text/xml", fgdc_xml),
        )
        metadata_id = cursor.lastrowid
        for table_name in feature_tables:
            connection.execute(
                "INSERT INTO gpkg_metadata_reference "
                "(reference_scope, table_name, column_name, row_id_value, timestamp, "
                "md_file_id, md_parent_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("table", table_name, None, None, timestamp, metadata_id, None),
            )
        connection.commit()


def apply_fgdc_layer_descriptions(gpkg: Path, fgdc: FgdcMetadata) -> None:
    """Store source-supplied layer explanations in GeoPackage contents."""
    descriptions = _fgdc_layer_descriptions(fgdc.entity_attribute_detail)
    if not descriptions:
        return
    with sqlite3.connect(gpkg) as connection:
        tables = connection.execute(
            "SELECT table_name FROM gpkg_contents WHERE data_type='features'"
        ).fetchall()
        for (table_name,) in tables:
            description = descriptions.get(_normalized_field_name(table_name))
            if description:
                connection.execute(
                    "UPDATE gpkg_contents SET description=? WHERE table_name=?",
                    (description, table_name),
                )
        connection.commit()


def run_embed_stage(job: JobConfig) -> None:
    """Embed reviewed QGIS metadata and the unmodified source FGDC XML."""
    require_confirmed_review(job)
    metadata_lookup = load_metadata_lookup(job.metadata_path, "filename")
    outputs = []
    for version in job.versions:
        gpkg = job.output_geopackage(version)
        if not gpkg.is_file():
            raise RuntimeError(f"Run convert before embed; missing {gpkg}")
        record = metadata_lookup.get(version.filename)
        if record is None:
            raise RuntimeError(f"Metadata row not found for {version.filename}")
        qgis_xml = build_metadata_xml(get_default_template_path(), record, gpkg)
        write_metadata_to_gpkg(gpkg, qgis_xml)
        package = discover_resource_package(version)
        fgdc_xml = package.metadata_xml.read_text(encoding="utf-8-sig")
        append_fgdc_metadata(gpkg, fgdc_xml)
        apply_fgdc_layer_descriptions(
            gpkg,
            parse_fgdc_metadata(package.metadata_xml),
        )
        with sqlite3.connect(gpkg) as connection:
            metadata_rows = connection.execute(
                "SELECT count(*) FROM gpkg_metadata"
            ).fetchone()[0]
        if metadata_rows != 2:
            raise RuntimeError(
                f"Expected two embedded metadata records in {gpkg}; found {metadata_rows}"
            )
        outputs.append(
            {
                "source_key": version.source_key,
                "output": portable_path(gpkg),
                "metadata_records": metadata_rows,
                "sha256_after_metadata": file_sha256(gpkg),
            }
        )
    mark_stage(job, "embed", details={"outputs": outputs})


def run_thumbnail_stage(
    job: JobConfig,
    *,
    overwrite: bool = False,
    renderer: Callable[[Path, Path], None] = create_vector_thumbnail,
) -> None:
    """Render a geometry-based PNG thumbnail from each curated GeoPackage."""
    require_confirmed_review(job)
    outputs = []
    for version in job.versions:
        gpkg = job.output_geopackage(version)
        if not gpkg.is_file():
            raise RuntimeError(
                f"GeoPackage is missing before thumbnail creation: {gpkg}"
            )
        legacy_preview = job.item_dir(version) / "preview.jpg"
        legacy_preview.unlink(missing_ok=True)
        destination = job.thumbnail_path(version)
        if destination.exists() and not overwrite:
            status = "existing"
        else:
            temporary = destination.with_name(f".{destination.stem}.tmp.png")
            temporary.unlink(missing_ok=True)
            try:
                renderer(gpkg, temporary)
                temporary.replace(destination)
            finally:
                temporary.unlink(missing_ok=True)
            status = "created"
        outputs.append(
            {
                "source_key": version.source_key,
                "output": portable_path(destination),
                "status": status,
                "sha256": file_sha256(destination),
            }
        )
    mark_stage(job, "thumbnails", details={"outputs": outputs})


def run_derivatives_stage(job: JobConfig, *, overwrite: bool = False) -> None:
    """Create FlatGeoBuf intermediates and PMTiles web derivatives."""
    require_confirmed_review(job)
    script = REPO_ROOT / "curation" / "scripts" / "build_pmtiles_from_gpkg.py"
    report = job.work_dir / "reports" / "pmtiles_build_report.csv"
    command = [
        sys.executable,
        str(script),
        "--input-dir",
        str(job.items_dir),
        "--fgb-dir",
        str(job.items_dir),
        "--pmtiles-dir",
        str(job.items_dir),
        "--report",
        str(report),
        "--resource-layout",
    ]
    if job.pmtiles_config:
        command.extend(["--config", str(job.pmtiles_config)])
    command.append("--overwrite" if overwrite else "--skip-existing")
    result = subprocess.run(command, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"PMTiles build failed: {result.stderr.strip()}")
    mark_stage(
        job,
        "derivatives",
        details={
            "report": portable_path(report),
            "command": " ".join(command),
        },
    )


def run_verify_stage(
    job: JobConfig,
    *,
    inspector: VectorInspector = inspect_vector,
) -> None:
    """Verify final access files without treating PMTiles as analytical data."""
    require_confirmed_review(job)
    records = []
    failures = []
    for version in job.versions:
        package = discover_resource_package(version)
        gpkg = job.output_geopackage(version)
        item_dir = job.item_dir(version)
        item_failures = []
        if not gpkg.is_file():
            item_failures.append("missing_geopackage")
        else:
            source_inspection = inspector(package.geodatabase)
            output_inspection = inspector(gpkg)
            _, comparison_errors = compare_vector_datasets(
                source_inspection, output_inspection
            )
            item_failures.extend(comparison_errors)
            if sqlite_integrity(gpkg) != "ok":
                item_failures.append("geopackage_integrity_failed")
            if not geopackage_has_embedded_metadata(gpkg):
                item_failures.append("embedded_metadata_missing")
        if not job.thumbnail_path(version).is_file():
            item_failures.append("thumbnail_missing")
        dictionaries = sorted((item_dir / "data-dictionaries").glob("*.csv"))
        if not dictionaries:
            item_failures.append("data_dictionary_missing")
        if len(_layer_summary(inspector(gpkg))) > 1:
            if not (item_dir / "layer-guide.txt").is_file():
                item_failures.append("multilayer_guide_missing")
        pmtiles = sorted(item_dir.glob("*.pmtiles"))
        if not pmtiles:
            item_failures.append("pmtiles_missing")
        failures.extend(f"{version.source_key}:{value}" for value in item_failures)
        records.append(
            {
                "source_key": version.source_key,
                "status": "passed" if not item_failures else "failed",
                "failures": item_failures,
                "geopackage": portable_path(gpkg),
                "pmtiles": [portable_path(path) for path in pmtiles],
                "dictionaries": [portable_path(path) for path in dictionaries],
            }
        )
    report = job.work_dir / "reports" / "final_validation.json"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(
        json.dumps({"records": records, "failures": failures}, indent=2) + "\n",
        encoding="utf-8",
    )
    if failures:
        raise RuntimeError(
            f"Final validation failed; see {report}: " + " | ".join(failures)
        )
    mark_stage(job, "verify", details={"report": portable_path(report)})


def _write_upload_zip(
    destination: Path,
    members: list[tuple[Path, str]],
    *,
    overwrite: bool,
) -> str:
    """Create an atomic ZIP with a fixity manifest for its source members."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and not overwrite:
        try:
            with zipfile.ZipFile(destination) as archive:
                if archive.testzip() is None:
                    return "existing_valid"
        except zipfile.BadZipFile:
            pass
        raise RuntimeError(
            f"Existing upload ZIP is invalid; rerun with --overwrite: {destination}"
        )
    temporary = destination.with_name(f".{destination.name}.tmp")
    temporary.unlink(missing_ok=True)
    checksums = []
    try:
        with zipfile.ZipFile(
            temporary,
            mode="w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=6,
        ) as archive:
            for source, archive_name in sorted(members, key=lambda item: item[1]):
                archive.write(source, arcname=archive_name)
                checksums.append(f"{file_sha256(source)}  {archive_name}")
            archive.writestr("SHA256SUMS.txt", "\n".join(checksums) + "\n")
        with zipfile.ZipFile(temporary) as archive:
            if archive.testzip() is not None:
                raise RuntimeError(
                    f"Upload ZIP failed integrity testing: {destination}"
                )
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)
    return "created"


def _write_upload_dictionary(
    source: Path,
    destination: Path,
    *,
    overwrite: bool,
) -> str:
    """Write an application-facing dictionary without curation-only columns."""
    with source.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        internal_only_fields = {"layer_name", "match_status"}
        fieldnames = [
            field
            for field in (reader.fieldnames or [])
            if field not in internal_only_fields
        ]
        rows = list(reader)
    if not fieldnames:
        raise RuntimeError(f"Data dictionary has no uploadable columns: {source}")

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.tmp")
    temporary.unlink(missing_ok=True)
    try:
        _write_csv(temporary, fieldnames, rows)
        if destination.exists() and not overwrite:
            if file_sha256(temporary) == file_sha256(destination):
                return "existing_identical"
            raise RuntimeError(
                "Upload data dictionary differs and --overwrite was not supplied: "
                f"{destination}"
            )
        temporary.replace(destination)
    finally:
        temporary.unlink(missing_ok=True)
    return "created"


def run_package_stage(job: JobConfig, *, overwrite: bool = False) -> Path:
    """Build access and preservation ZIPs plus an upload fixity manifest."""
    require_confirmed_review(job)
    if job.uploads_dir.is_dir():
        for finder_file in job.uploads_dir.rglob(".DS_Store"):
            finder_file.unlink()
    rows = []
    for version in job.versions:
        item_dir = job.item_dir(version)
        gpkg = job.output_geopackage(version)
        original_dir = item_dir / "original"
        if not gpkg.is_file():
            raise RuntimeError(f"Curated GeoPackage is missing: {gpkg}")
        if not original_dir.is_dir():
            raise RuntimeError(
                f"Preserved original directory is missing: {original_dir}"
            )

        upload_dir = job.uploads_dir / version.output_stem
        nested_dictionary_dir = upload_dir / "data-dictionaries"
        if nested_dictionary_dir.is_dir():
            for stale in sorted(nested_dictionary_dir.rglob("*"), reverse=True):
                if stale.is_file():
                    stale.unlink()
                elif stale.is_dir():
                    stale.rmdir()
            nested_dictionary_dir.rmdir()
        if upload_dir.is_dir():
            for stale_preview in upload_dir.glob("*.jpg"):
                stale_preview.unlink()
        access_zip = upload_dir / f"{version.filename}.zip"
        access_status = _write_upload_zip(
            access_zip,
            [(gpkg, gpkg.name)],
            overwrite=overwrite,
        )

        original_members = [
            (path, (Path("original") / path.relative_to(original_dir)).as_posix())
            for path in original_dir.rglob("*")
            if path.is_file()
            and path.name != ".DS_Store"
            and path.suffix.casefold() not in {".gpkg", ".gpkg-shm", ".gpkg-wal"}
        ]
        if not original_members:
            raise RuntimeError(f"No preservation files found under: {original_dir}")
        original_zip = upload_dir / f"{version.output_stem}_original.zip"
        original_status = _write_upload_zip(
            original_zip,
            original_members,
            overwrite=overwrite,
        )

        for role, path, status, members in (
            ("access_geopackage", access_zip, access_status, [(gpkg, gpkg.name)]),
            (
                "preservation_original",
                original_zip,
                original_status,
                original_members,
            ),
        ):
            rows.append(
                {
                    "source_key": version.source_key,
                    "role": role,
                    "path": portable_path(path),
                    "size_bytes": path.stat().st_size,
                    "sha256": file_sha256(path),
                    "contents": "|".join(archive_name for _, archive_name in members),
                    "status": status,
                }
            )

        thumbnail = job.thumbnail_path(version)
        pmtiles = sorted(item_dir.glob("*.pmtiles"))
        dictionaries = sorted((item_dir / "data-dictionaries").glob("*.csv"))
        layer_guide = item_dir / "layer-guide.txt"
        if not thumbnail.is_file():
            raise RuntimeError(f"Generated thumbnail is missing: {thumbnail}")
        if not pmtiles:
            raise RuntimeError(f"PMTiles derivative is missing under: {item_dir}")
        if not dictionaries:
            raise RuntimeError(f"Data dictionary is missing under: {item_dir}")
        upload_files = [
            ("thumbnail", thumbnail, upload_dir / thumbnail.name),
            *(("pmtiles", source, upload_dir / source.name) for source in pmtiles),
        ]
        if layer_guide.is_file():
            upload_files.append(
                ("layer_documentation", layer_guide, upload_dir / layer_guide.name)
            )
        for role, source, destination in upload_files:
            status = _copy_file(source, destination, overwrite=overwrite)
            rows.append(
                {
                    "source_key": version.source_key,
                    "role": role,
                    "path": portable_path(destination),
                    "size_bytes": destination.stat().st_size,
                    "sha256": file_sha256(destination),
                    "contents": portable_path(source),
                    "status": status,
                }
            )
        for source in dictionaries:
            destination = upload_dir / source.name
            status = _write_upload_dictionary(
                source,
                destination,
                overwrite=overwrite,
            )
            rows.append(
                {
                    "source_key": version.source_key,
                    "role": "data_dictionary",
                    "path": portable_path(destination),
                    "size_bytes": destination.stat().st_size,
                    "sha256": file_sha256(destination),
                    "contents": portable_path(source),
                    "status": status,
                }
            )

    _write_csv(job.upload_manifest_path, UPLOAD_MANIFEST_COLUMNS, rows)
    mark_stage(
        job,
        "package",
        details={
            "output": portable_path(job.uploads_dir),
            "manifest": portable_path(job.upload_manifest_path),
            "archives": len(job.versions) * 2,
            "artifacts": len(rows),
        },
    )
    return job.upload_manifest_path


def run_postprocess(job: JobConfig, *, overwrite: bool = False) -> None:
    """Run all automated stages after manual metadata confirmation."""
    require_confirmed_review(job)
    LOGGER.info("Postprocess 1/8: preserving source packages")
    run_preserve_stage(job, overwrite=overwrite)
    LOGGER.info("Postprocess 2/8: generating GeoPackages from File Geodatabases")
    run_convert_stage(job, overwrite=overwrite)
    LOGGER.info("Postprocess 3/8: generating FGDC data dictionaries")
    run_dictionary_stage(job)
    LOGGER.info("Postprocess 4/8: embedding QGIS and FGDC metadata")
    run_embed_stage(job)
    LOGGER.info("Postprocess 5/8: rendering geometry-based thumbnails")
    run_thumbnail_stage(job, overwrite=overwrite)
    LOGGER.info("Postprocess 6/8: creating PMTiles")
    run_derivatives_stage(job, overwrite=overwrite)
    LOGGER.info("Postprocess 7/8: verifying final outputs")
    run_verify_stage(job)
    LOGGER.info("Postprocess 8/8: creating upload ZIPs")
    run_package_stage(job, overwrite=overwrite)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path, help="MnCommons curation YAML job")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("validate", help="Validate source File Geodatabases")
    commands.add_parser(
        "inventory", help="Write source fixity and validation inventory"
    )
    commands.add_parser("metadata", help="Create the draft Aardvark review CSV")
    review = commands.add_parser("review", help="Record the manual metadata checkpoint")
    review.add_argument(
        "--confirm", action="store_true", help="Confirm the current CSV checksum"
    )
    for command_name in (
        "preserve",
        "convert",
        "thumbnails",
        "derivatives",
        "package",
        "postprocess",
    ):
        command = commands.add_parser(command_name)
        command.add_argument("--overwrite", action="store_true")
    commands.add_parser("dictionaries")
    commands.add_parser("embed")
    commands.add_parser("verify")
    commands.add_parser("status", help="Print the current manifest")
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = build_parser().parse_args(argv)
    try:
        job = load_job_config(args.config)
        if args.command == "validate":
            inspections = run_validate_stage(job)
            print(
                f"Validated {len(inspections)} resources; "
                "all curated GeoPackages will be generated from File Geodatabases."
            )
            print(f"Manifest: {job.manifest_path}")
        elif args.command == "inventory":
            print(f"Inventory: {run_inventory_stage(job)}")
        elif args.command == "metadata":
            print(f"Metadata review CSV: {run_metadata_stage(job)}")
            print(f"Collection review CSV: {job.collection_metadata_path}")
        elif args.command == "review":
            if not args.confirm:
                raise RuntimeError(
                    "Pass --confirm after manually reviewing the metadata CSV"
                )
            confirm_review(job)
            print(
                "Confirmed metadata review: "
                f"{job.metadata_path} and {job.collection_metadata_path}"
            )
        elif args.command == "preserve":
            run_preserve_stage(job, overwrite=args.overwrite)
            print(f"Preserved source packages under: {job.items_dir}")
        elif args.command == "convert":
            run_convert_stage(job, overwrite=args.overwrite)
            print(f"GeoPackages: {job.items_dir}")
        elif args.command == "dictionaries":
            run_dictionary_stage(job)
            print(f"Data dictionaries: {job.items_dir}")
        elif args.command == "embed":
            run_embed_stage(job)
            print(f"Embedded metadata: {job.items_dir}")
        elif args.command == "thumbnails":
            run_thumbnail_stage(job, overwrite=args.overwrite)
            print(f"Thumbnails: {job.items_dir}")
        elif args.command == "derivatives":
            run_derivatives_stage(job, overwrite=args.overwrite)
            print(f"PMTiles derivatives: {job.items_dir}")
        elif args.command == "verify":
            run_verify_stage(job)
            print(f"Final validation passed: {job.items_dir}")
        elif args.command == "package":
            manifest_path = run_package_stage(job, overwrite=args.overwrite)
            print(f"Upload packages: {job.uploads_dir}")
            print(f"Upload manifest: {manifest_path}")
        elif args.command == "postprocess":
            run_postprocess(job, overwrite=args.overwrite)
            print(f"Postprocess completed: {job.items_dir}")
        elif args.command == "status":
            print(json.dumps(load_manifest(job), indent=2))
    except (CurationConfigError, OSError, RuntimeError, ET.ParseError) as exc:
        LOGGER.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
