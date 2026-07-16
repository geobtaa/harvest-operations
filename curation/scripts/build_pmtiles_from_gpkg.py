#!/usr/bin/env python3
"""Build FlatGeoBuf and PMTiles outputs from GeoPackage vector layers."""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import datetime as dt
import hashlib
import json
import logging
import re
import shlex
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


SYSTEM_LAYER_PREFIXES = ("gpkg_", "rtree_", "sqlite_", "idx_")
SYSTEM_LAYER_NAMES = {
    "geometry_columns",
    "spatial_ref_sys",
    "sqlite_sequence",
}
DEFAULT_CONFIG: dict[str, Any] = {
    "default": {
        "mode": "keep_all",
        "always_keep": ["id", "name", "type", "class", "code"],
        "include_patterns": [],
        "exclude_patterns": [
            "^shape_",
            ".*_area$",
            ".*_length$",
            "description",
            "notes",
            "comments",
            "metadata",
            "url",
            "website",
        ],
        "max_string_width": None,
    },
    "layers": {},
}


@dataclass
class CommandResult:
    """Captured subprocess execution details."""

    command: list[str]
    command_string: str
    start_time: str
    end_time: str
    elapsed_seconds: float
    returncode: int
    stdout: str
    stderr: str


@dataclass
class FieldDecision:
    """Keep/drop decision for one source field."""

    name: str
    field_type: str = ""
    width: str = ""
    kept: bool = True
    reason: str = "default keep"


@dataclass
class LayerJob:
    """All metadata and paths needed to process one vector layer."""

    source_path: Path
    source_layer: str
    source_stem_slug: str
    layer_slug: str
    output_stem: str
    fgb_path: Path
    pmtiles_path: Path
    feature_count: str = ""
    geometry_type: str = ""
    source_crs: str = ""
    extent: list[float] | None = None
    fields: list[dict[str, Any]] = field(default_factory=list)
    field_decisions: list[FieldDecision] = field(default_factory=list)
    field_mode: str = "keep_all"
    warnings: list[str] = field(default_factory=list)

    @property
    def kept_fields(self) -> list[str]:
        """Return fields that should be retained in output attributes."""
        return [decision.name for decision in self.field_decisions if decision.kept]

    @property
    def dropped_fields(self) -> list[str]:
        """Return fields that should be removed from output attributes."""
        return [decision.name for decision in self.field_decisions if not decision.kept]


def utc_now() -> str:
    """Return an ISO-8601 UTC timestamp with second precision."""
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds")


def slugify(value: str, max_length: int = 80, hash_suffix: str | None = None) -> str:
    """Create a safe lowercase slug for filenames and vector layer names."""
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip().lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    if not slug:
        slug = "layer"

    suffix = f"_{hash_suffix}" if hash_suffix else ""
    max_base_length = max(1, max_length - len(suffix))
    slug = slug[:max_base_length].rstrip("_") or "layer"
    return f"{slug}{suffix}"


def short_hash(value: str, length: int = 8) -> str:
    """Return a stable short SHA-1 hash for disambiguating names."""
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:length]


def command_to_string(command: list[str]) -> str:
    """Render a command argument list as a copyable shell string."""
    return shlex.join(command)


def run_command(command: list[str], timeout: float | None = None) -> CommandResult:
    """Run a subprocess without a shell and capture output and timing."""
    start = dt.datetime.now(dt.UTC)
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        stdout = completed.stdout
        stderr = completed.stderr
        returncode = completed.returncode
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout if isinstance(exc.stdout, str) else ""
        stderr = exc.stderr if isinstance(exc.stderr, str) else ""
        stderr = f"{stderr}\nCommand timed out after {timeout} seconds.".strip()
        returncode = 124
    end = dt.datetime.now(dt.UTC)
    return CommandResult(
        command=command,
        command_string=command_to_string(command),
        start_time=start.isoformat(timespec="seconds"),
        end_time=end.isoformat(timespec="seconds"),
        elapsed_seconds=round((end - start).total_seconds(), 3),
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
    )


def require_executable(name: str, needed_for: str) -> str:
    """Find a required executable or exit with a helpful message."""
    executable = shutil.which(name)
    if executable:
        return executable
    raise SystemExit(
        f"Missing required command '{name}' for {needed_for}. Install it and ensure "
        f"it is available on PATH."
    )


def load_config(path: Path | None) -> dict[str, Any]:
    """Load and merge field selection config with built-in defaults."""
    config = json.loads(json.dumps(DEFAULT_CONFIG))
    if path is None:
        return config
    with path.open("r", encoding="utf-8") as file_obj:
        user_config = json.load(file_obj)
    config["default"].update(user_config.get("default", {}))
    config["layers"] = user_config.get("layers", {})
    return config


def compile_patterns(patterns: list[str]) -> list[re.Pattern[str]]:
    """Compile regex patterns and raise a clear config error if invalid."""
    compiled = []
    for pattern in patterns:
        try:
            compiled.append(re.compile(pattern, re.IGNORECASE))
        except re.error as exc:
            raise SystemExit(f"Invalid field pattern {pattern!r}: {exc}") from exc
    return compiled


def matches_any_pattern(value: str, patterns: list[re.Pattern[str]]) -> bool:
    """Return true when a value matches any configured regex pattern."""
    return any(pattern.search(value) for pattern in patterns)


def find_layer_config(
    config: dict[str, Any], source_layer: str, layer_slug: str
) -> tuple[str, dict[str, Any]]:
    """Find a layer-specific config by case-insensitive source or slug name."""
    source_key = source_layer.lower()
    slug_key = layer_slug.lower()
    for configured_name, layer_config in config.get("layers", {}).items():
        configured_key = configured_name.lower()
        configured_slug = slugify(configured_name).lower()
        if configured_key in {source_key, slug_key} or configured_slug in {
            source_key,
            slug_key,
        }:
            return configured_name, layer_config
    return "", {}


def field_name(field: dict[str, Any]) -> str:
    """Extract a field name from common ogrinfo JSON field shapes."""
    return str(field.get("name") or field.get("Name") or "")


def field_type(field: dict[str, Any]) -> str:
    """Extract a field type from common ogrinfo JSON field shapes."""
    return str(field.get("type") or field.get("Type") or field.get("subtype") or "")


def field_width(field: dict[str, Any]) -> str:
    """Extract a field width from common ogrinfo JSON field shapes."""
    width = field.get("width", field.get("Width", ""))
    return "" if width is None else str(width)


def select_fields_for_layer(
    job: LayerJob, config: dict[str, Any], logger: logging.Logger
) -> None:
    """Apply configured field selection rules to a layer job."""
    default_config = config.get("default", {})
    configured_name, layer_config = find_layer_config(
        config, job.source_layer, job.layer_slug
    )
    merged = dict(default_config)
    merged.update(layer_config)

    mode = str(merged.get("mode", "keep_all")).lower()
    if mode not in {"keep_all", "keep_only", "geometry_only"}:
        raise SystemExit(
            f"Invalid mode {mode!r} for layer {configured_name or job.source_layer}."
        )
    job.field_mode = mode

    keep_names = {str(name).lower() for name in merged.get("keep", [])}
    drop_names = {str(name).lower() for name in merged.get("drop", [])}
    always_keep = {str(name).lower() for name in merged.get("always_keep", [])}
    include_patterns = compile_patterns(merged.get("include_patterns", []))
    exclude_patterns = compile_patterns(merged.get("exclude_patterns", []))
    max_string_width = merged.get("max_string_width")
    existing_names = {field_name(field).lower() for field in job.fields}

    for missing in sorted((keep_names | always_keep) - existing_names):
        message = (
            f"Configured keep field {missing!r} is missing from "
            f"{job.source_path.name}:{job.source_layer}."
        )
        job.warnings.append(message)
        logger.warning(message)

    decisions: list[FieldDecision] = []
    for field_def in job.fields:
        name = field_name(field_def)
        if not name:
            continue
        lower_name = name.lower()
        f_type = field_type(field_def)
        width = field_width(field_def)

        if mode == "geometry_only":
            decisions.append(
                FieldDecision(name, f_type, width, kept=False, reason="geometry only")
            )
            continue

        if lower_name in keep_names:
            decisions.append(
                FieldDecision(name, f_type, width, kept=True, reason="explicit keep")
            )
            continue
        if lower_name in always_keep:
            decisions.append(
                FieldDecision(name, f_type, width, kept=True, reason="always keep")
            )
            continue

        include_match = matches_any_pattern(name, include_patterns)
        exclude_match = matches_any_pattern(name, exclude_patterns)

        if mode == "keep_only":
            if include_match:
                decisions.append(
                    FieldDecision(
                        name, f_type, width, kept=True, reason="include pattern"
                    )
                )
            else:
                decisions.append(
                    FieldDecision(
                        name, f_type, width, kept=False, reason="default drop"
                    )
                )
            continue

        if lower_name in drop_names:
            decisions.append(
                FieldDecision(name, f_type, width, kept=False, reason="explicit drop")
            )
        elif exclude_match:
            decisions.append(
                FieldDecision(name, f_type, width, kept=False, reason="exclude pattern")
            )
        elif include_match:
            decisions.append(
                FieldDecision(name, f_type, width, kept=True, reason="include pattern")
            )
        elif (
            max_string_width is not None
            and "string" in f_type.lower()
            and width.isdigit()
            and int(width) > int(max_string_width)
        ):
            decisions.append(
                FieldDecision(
                    name,
                    f_type,
                    width,
                    kept=False,
                    reason=f"max string width > {max_string_width}",
                )
            )
        else:
            decisions.append(
                FieldDecision(name, f_type, width, kept=True, reason="default keep")
            )

    job.field_decisions = decisions


def layer_geometry_type(layer: dict[str, Any]) -> str:
    """Extract a geometry type from ogrinfo JSON."""
    if layer.get("geometryType"):
        return str(layer["geometryType"])
    geometry_fields = layer.get("geometryFields") or []
    if geometry_fields and isinstance(geometry_fields[0], dict):
        return str(
            geometry_fields[0].get("type")
            or geometry_fields[0].get("geometryType")
            or ""
        )
    return ""


def layer_crs(layer: dict[str, Any]) -> str:
    """Extract a CRS/auth code from ogrinfo JSON when available."""
    geometry_fields = layer.get("geometryFields") or []
    spatial_ref = layer.get("srs") or layer.get("spatialReference")
    if geometry_fields and isinstance(geometry_fields[0], dict):
        spatial_ref = (
            geometry_fields[0].get("coordinateSystem")
            or geometry_fields[0].get("srs")
            or spatial_ref
        )
    if isinstance(spatial_ref, dict):
        authority = spatial_ref.get("authority")
        code = spatial_ref.get("code")
        if authority and code:
            return f"{authority}:{code}"
        if spatial_ref.get("wkt"):
            return "WKT"
        return json.dumps(spatial_ref, sort_keys=True)
    return "" if spatial_ref is None else str(spatial_ref)


def layer_extent(layer: dict[str, Any]) -> list[float] | None:
    """Extract layer extent as [minx, miny, maxx, maxy]."""
    extent = layer.get("extent")
    if isinstance(extent, list) and len(extent) == 4:
        return [float(value) for value in extent]
    if isinstance(extent, dict):
        values = [
            extent.get("minX", extent.get("xmin")),
            extent.get("minY", extent.get("ymin")),
            extent.get("maxX", extent.get("xmax")),
            extent.get("maxY", extent.get("ymax")),
        ]
        if all(value is not None for value in values):
            return [float(value) for value in values]
    return None


def layer_fields(layer: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract field definitions from ogrinfo JSON."""
    fields = layer.get("fields") or layer.get("fieldDefinitions") or []
    return fields if isinstance(fields, list) else []


def is_spatial_layer(layer: dict[str, Any]) -> bool:
    """Return true when a layer appears to have vector geometry."""
    geom_type = layer_geometry_type(layer).lower()
    if geom_type and geom_type not in {"none", "unknown (any)", "null"}:
        return True
    geometry_fields = layer.get("geometryFields") or []
    return bool(geometry_fields)


def is_system_layer(name: str) -> bool:
    """Return true for obvious GeoPackage/system metadata tables."""
    lower_name = name.lower()
    return lower_name in SYSTEM_LAYER_NAMES or lower_name.startswith(
        SYSTEM_LAYER_PREFIXES
    )


def parse_ogrinfo_layers(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract layers from common ogrinfo JSON output shapes."""
    layers = payload.get("layers")
    if isinstance(layers, list):
        return layers
    if isinstance(payload.get("layer"), list):
        return payload["layer"]
    return []


def inspect_geopackage(
    gpkg_path: Path,
    ogrinfo: str,
    timeout: float | None,
    logger: logging.Logger,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Inspect one GeoPackage and return vector layer metadata."""
    command = [ogrinfo, "-json", "-ro", str(gpkg_path)]
    result = run_command(command, timeout=timeout)
    if result.returncode != 0:
        warning = f"ogrinfo failed for {gpkg_path}: {result.stderr.strip()}"
        logger.error(warning)
        return [], [warning]
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        warning = f"Could not parse ogrinfo JSON for {gpkg_path}: {exc}"
        logger.error(warning)
        return [], [warning]

    layers = []
    warnings = []
    for layer in parse_ogrinfo_layers(payload):
        name = str(layer.get("name") or layer.get("layerName") or "")
        if not name:
            warnings.append(f"Skipping unnamed layer in {gpkg_path}.")
            continue
        if is_system_layer(name):
            warnings.append(f"Skipping system layer {name} in {gpkg_path}.")
            continue
        if not is_spatial_layer(layer):
            warnings.append(f"Skipping non-spatial layer {name} in {gpkg_path}.")
            continue
        layers.append(layer)
    return layers, warnings


def discover_jobs(
    input_dir: Path,
    fgb_dir: Path,
    pmtiles_dir: Path,
    config: dict[str, Any],
    ogrinfo: str,
    timeout: float | None,
    logger: logging.Logger,
    outputs_next_to_input: bool = False,
) -> list[LayerJob]:
    """Find GeoPackages, inspect layers, and build unique layer jobs."""
    gpkg_paths = sorted(input_dir.rglob("*.gpkg"))
    logger.info("Found %s GeoPackage file(s).", len(gpkg_paths))
    prelim_jobs: list[LayerJob] = []

    for gpkg_path in gpkg_paths:
        logger.info("Inspecting %s", gpkg_path)
        layers, warnings = inspect_geopackage(gpkg_path, ogrinfo, timeout, logger)
        for warning in warnings:
            logger.warning(warning)
        source_stem_slug = (
            gpkg_path.stem if outputs_next_to_input else slugify(gpkg_path.stem)
        )
        output_parent = gpkg_path.parent if outputs_next_to_input else None
        layer_count = len(layers)
        for layer in layers:
            source_layer = str(layer.get("name") or layer.get("layerName"))
            layer_slug = slugify(source_layer)
            output_stem = (
                source_stem_slug
                if layer_count == 1
                else f"{source_stem_slug}__{layer_slug}"
            )
            prelim_jobs.append(
                LayerJob(
                    source_path=gpkg_path,
                    source_layer=source_layer,
                    source_stem_slug=source_stem_slug,
                    layer_slug=layer_slug,
                    output_stem=output_stem,
                    fgb_path=(output_parent or fgb_dir) / f"{output_stem}.fgb",
                    pmtiles_path=(output_parent or pmtiles_dir) / f"{output_stem}.pmtiles",
                    feature_count=str(layer.get("featureCount", "")),
                    geometry_type=layer_geometry_type(layer),
                    source_crs=layer_crs(layer),
                    extent=layer_extent(layer),
                    fields=layer_fields(layer),
                    warnings=warnings.copy(),
                )
            )

    used_output_keys: set[str] = set()
    jobs: list[LayerJob] = []
    for job in prelim_jobs:
        output_key = job.output_stem
        if output_key in used_output_keys:
            suffix = short_hash(f"{job.source_path}:{job.source_layer}")
            output_key = slugify(output_key, hash_suffix=suffix)
            job.output_stem = output_key
            if outputs_next_to_input:
                job.fgb_path = job.source_path.parent / f"{output_key}.fgb"
                job.pmtiles_path = job.source_path.parent / f"{output_key}.pmtiles"
            else:
                job.fgb_path = fgb_dir / f"{output_key}.fgb"
                job.pmtiles_path = pmtiles_dir / f"{output_key}.pmtiles"
        used_output_keys.add(output_key)
        select_fields_for_layer(job, config, logger)
        jobs.append(job)

    logger.info("Discovered %s vector layer(s).", len(jobs))
    return jobs


def select_args(job: LayerJob) -> list[str]:
    """Return ogr2ogr select arguments for the job's field decisions."""
    kept = job.kept_fields
    if len(kept) == len(job.field_decisions):
        return []
    if not kept:
        return ["-select", ""]
    return ["-select", ",".join(kept)]


def validate_fgb(
    fgb_path: Path, ogrinfo: str, timeout: float | None
) -> tuple[bool, str, CommandResult]:
    """Validate FlatGeoBuf extent and return plausibility status."""
    command = [ogrinfo, "-json", "-so", str(fgb_path)]
    result = run_command(command, timeout=timeout)
    if result.returncode != 0:
        return False, f"FlatGeoBuf validation failed: {result.stderr.strip()}", result
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        return False, f"FlatGeoBuf validation JSON parse failed: {exc}", result

    layers = parse_ogrinfo_layers(payload)
    extent = layer_extent(layers[0]) if layers else layer_extent(payload)
    if extent is None:
        return True, "FlatGeoBuf extent unavailable; CRS plausibility not checked.", result
    minx, miny, maxx, maxy = extent
    plausible = -180 <= minx <= 180 and -180 <= maxx <= 180
    plausible = plausible and -90 <= miny <= 90 and -90 <= maxy <= 90
    if not plausible:
        return (
            False,
            f"Likely CRS problem: EPSG:4326 extent is implausible ({extent}).",
            result,
        )
    return True, "", result


def nonempty(path: Path) -> bool:
    """Return true when a path exists and has content."""
    return path.exists() and path.stat().st_size > 0


def build_fgb_command(
    job: LayerJob, ogr2ogr: str, overwrite: bool
) -> list[str]:
    """Build the ogr2ogr command for one FlatGeoBuf output."""
    command = [
        ogr2ogr,
        "-f",
        "FlatGeobuf",
        str(job.fgb_path),
        str(job.source_path),
        job.source_layer,
        "-t_srs",
        "EPSG:4326",
        "-makevalid",
        "-nlt",
        "PROMOTE_TO_MULTI",
        "-lco",
        "SPATIAL_INDEX=YES",
    ]
    command.extend(select_args(job))
    if overwrite:
        command.append("-overwrite")
    return command


def build_tippecanoe_command(
    job: LayerJob,
    tippecanoe: str,
    extra_args: list[str],
) -> list[str]:
    """Build the tippecanoe command for one PMTiles output."""
    command = [
        tippecanoe,
        "-o",
        str(job.pmtiles_path),
        "-f",
        "-zg",
        "--drop-densest-as-needed",
        "--extend-zooms-if-still-dropping",
        "-l",
        job.layer_slug,
    ]
    if job.field_mode == "keep_only":
        for field_name_value in job.kept_fields:
            command.extend(["-y", field_name_value])
    command.extend(extra_args)
    command.append(str(job.fgb_path))
    return command


def empty_layer_report(job: LayerJob) -> dict[str, Any]:
    """Create a base report row for one layer."""
    return {
        "source_path": str(job.source_path),
        "source_layer": job.source_layer,
        "output_fgb": str(job.fgb_path),
        "output_pmtiles": str(job.pmtiles_path),
        "feature_count": job.feature_count,
        "geometry_type": job.geometry_type,
        "source_crs": job.source_crs,
        "source_extent": json.dumps(job.extent or []),
        "field_mode": job.field_mode,
        "kept_fields": json.dumps(job.kept_fields),
        "dropped_fields": json.dumps(job.dropped_fields),
        "fgb_command": "",
        "fgb_start_time": "",
        "fgb_end_time": "",
        "fgb_returncode": "",
        "fgb_elapsed_seconds": "",
        "fgb_stdout": "",
        "fgb_stderr": "",
        "validation_command": "",
        "validation_returncode": "",
        "validation_stdout": "",
        "validation_stderr": "",
        "tippecanoe_command": "",
        "tippecanoe_start_time": "",
        "tippecanoe_end_time": "",
        "tippecanoe_returncode": "",
        "tippecanoe_elapsed_seconds": "",
        "tippecanoe_stdout": "",
        "tippecanoe_stderr": "",
        "status": "pending",
        "warnings": json.dumps(job.warnings),
        "errors": json.dumps([]),
        "elapsed_seconds": 0,
    }


def process_job(
    job: LayerJob,
    ogr2ogr: str,
    ogrinfo: str,
    tippecanoe: str,
    args: argparse.Namespace,
    extra_tippecanoe_args: list[str],
) -> dict[str, Any]:
    """Convert one layer to FlatGeoBuf and PMTiles and return report data."""
    logger = logging.getLogger("build_pmtiles")
    start = dt.datetime.now(dt.UTC)
    row = empty_layer_report(job)
    warnings = list(job.warnings)
    errors: list[str] = []
    status_parts: list[str] = []
    row["fgb_command"] = command_to_string(
        build_fgb_command(job, ogr2ogr, args.overwrite)
    )
    row["tippecanoe_command"] = command_to_string(
        build_tippecanoe_command(job, tippecanoe, extra_tippecanoe_args)
    )

    if args.dry_run:
        row.update(
            {
                "status": "dry_run",
                "warnings": json.dumps(warnings),
            }
        )
        return row

    job.fgb_path.parent.mkdir(parents=True, exist_ok=True)
    job.pmtiles_path.parent.mkdir(parents=True, exist_ok=True)

    if nonempty(job.fgb_path) and not args.overwrite:
        if args.skip_existing:
            status_parts.append("fgb skipped existing")
            logger.info("Skipping existing FGB %s", job.fgb_path)
        else:
            errors.append(f"FGB exists and --overwrite was not set: {job.fgb_path}")
            row.update(
                {
                    "status": "failed",
                    "errors": json.dumps(errors),
                    "elapsed_seconds": round(
                        (dt.datetime.now(dt.UTC) - start).total_seconds(), 3
                    ),
                }
            )
            return row
    else:
        fgb_command = build_fgb_command(job, ogr2ogr, args.overwrite)
        row["fgb_command"] = command_to_string(fgb_command)
        logger.info("Creating FGB %s", job.fgb_path)
        fgb_result = run_command(fgb_command, timeout=args.timeout)
        row["fgb_start_time"] = fgb_result.start_time
        row["fgb_end_time"] = fgb_result.end_time
        row["fgb_returncode"] = fgb_result.returncode
        row["fgb_elapsed_seconds"] = fgb_result.elapsed_seconds
        row["fgb_stdout"] = fgb_result.stdout
        row["fgb_stderr"] = fgb_result.stderr
        if fgb_result.returncode != 0:
            errors.append(f"ogr2ogr failed: {fgb_result.stderr.strip()}")
            row.update(
                {
                    "status": "failed",
                    "errors": json.dumps(errors),
                    "elapsed_seconds": round(
                        (dt.datetime.now(dt.UTC) - start).total_seconds(), 3
                    ),
                }
            )
            return row
        status_parts.append("fgb created")

    valid, validation_message, validation_result = validate_fgb(
        job.fgb_path, ogrinfo, args.timeout
    )
    row["validation_command"] = validation_result.command_string
    row["validation_returncode"] = validation_result.returncode
    row["validation_stdout"] = validation_result.stdout
    row["validation_stderr"] = validation_result.stderr
    if validation_message:
        warnings.append(validation_message)
    if not valid:
        row.update(
            {
                "status": "likely_crs_error",
                "warnings": json.dumps(warnings),
                "errors": json.dumps(errors),
                "elapsed_seconds": round(
                    (dt.datetime.now(dt.UTC) - start).total_seconds(), 3
                ),
            }
        )
        return row

    if nonempty(job.pmtiles_path) and not args.overwrite:
        if args.skip_existing:
            status_parts.append("pmtiles skipped existing")
            logger.info("Skipping existing PMTiles %s", job.pmtiles_path)
        else:
            errors.append(
                f"PMTiles exists and --overwrite was not set: {job.pmtiles_path}"
            )
            row.update(
                {
                    "status": "failed",
                    "errors": json.dumps(errors),
                    "elapsed_seconds": round(
                        (dt.datetime.now(dt.UTC) - start).total_seconds(), 3
                    ),
                }
            )
            return row
    else:
        tip_command = build_tippecanoe_command(
            job, tippecanoe, extra_tippecanoe_args
        )
        row["tippecanoe_command"] = command_to_string(tip_command)
        logger.info("Creating PMTiles %s", job.pmtiles_path)
        tip_result = run_command(tip_command, timeout=args.timeout)
        row["tippecanoe_start_time"] = tip_result.start_time
        row["tippecanoe_end_time"] = tip_result.end_time
        row["tippecanoe_returncode"] = tip_result.returncode
        row["tippecanoe_elapsed_seconds"] = tip_result.elapsed_seconds
        row["tippecanoe_stdout"] = tip_result.stdout
        row["tippecanoe_stderr"] = tip_result.stderr
        if tip_result.returncode != 0:
            errors.append(f"tippecanoe failed: {tip_result.stderr.strip()}")
            row.update(
                {
                    "status": "failed",
                    "errors": json.dumps(errors),
                    "elapsed_seconds": round(
                        (dt.datetime.now(dt.UTC) - start).total_seconds(), 3
                    ),
                }
            )
            return row
        status_parts.append("pmtiles created")

    end = dt.datetime.now(dt.UTC)
    row["elapsed_seconds"] = round((end - start).total_seconds(), 3)
    row["status"] = (
        "skipped"
        if status_parts and all("skipped" in part for part in status_parts)
        else "succeeded"
    )
    row["warnings"] = json.dumps(warnings)
    row["errors"] = json.dumps(errors)
    return row


def field_inventory_rows(jobs: list[LayerJob]) -> list[dict[str, Any]]:
    """Build field inventory report rows for all discovered jobs."""
    rows = []
    for job in jobs:
        for decision in job.field_decisions:
            rows.append(
                {
                    "source_path": str(job.source_path),
                    "source_layer": job.source_layer,
                    "layer_slug": job.layer_slug,
                    "field_name": decision.name,
                    "field_type": decision.field_type,
                    "width": decision.width,
                    "kept": decision.kept,
                    "reason": decision.reason,
                }
            )
    return rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    """Write dictionaries to CSV, preserving all keys seen in rows."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, rows: list[dict[str, Any]]) -> None:
    """Write dictionaries to pretty JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file_obj:
        json.dump(rows, file_obj, indent=2)
        file_obj.write("\n")


def write_report(path: Path, rows: list[dict[str, Any]]) -> None:
    """Write CSV or JSON report based on file extension."""
    if path.suffix.lower() == ".json":
        write_json(path, rows)
    else:
        write_csv(path, rows)


def field_report_path(report_path: Path) -> Path:
    """Return the sibling path used for field inventory during full builds."""
    return report_path.with_name(f"{report_path.stem}_fields{report_path.suffix}")


def parse_extra_tippecanoe_args(values: list[str] | None) -> list[str]:
    """Parse repeated or quoted extra tippecanoe argument strings."""
    if not values:
        return []
    parsed: list[str] = []
    for value in values:
        parsed.extend(shlex.split(value))
    return parsed


def setup_logging(log_file: Path | None) -> logging.Logger:
    """Configure console and optional file logging."""
    logger = logging.getLogger("build_pmtiles")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)

    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def build_parser() -> argparse.ArgumentParser:
    """Create the command-line argument parser."""
    parser = argparse.ArgumentParser(
        description="Convert GeoPackage vector layers to FlatGeoBuf and PMTiles."
    )
    parser.add_argument("--input-dir", required=True, type=Path)
    parser.add_argument("--fgb-dir", required=True, type=Path)
    parser.add_argument("--pmtiles-dir", required=True, type=Path)
    parser.add_argument(
        "--outputs-next-to-input",
        action="store_true",
        help="Write each FlatGeoBuf and PMTiles beside its source GeoPackage.",
    )
    parser.add_argument("--config", type=Path)
    parser.add_argument("--report", type=Path)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--field-report-only", action="store_true")
    parser.add_argument("--max-workers", type=int, default=1)
    parser.add_argument(
        "--tippecanoe-extra-args",
        action="append",
        help="Additional tippecanoe args. May be repeated or passed as a quoted string.",
    )
    parser.add_argument("--log-file", type=Path)
    parser.add_argument(
        "--timeout",
        type=float,
        default=None,
        help="Optional subprocess timeout in seconds. Defaults to no timeout.",
    )
    return parser


def summarize(rows: list[dict[str, Any]]) -> dict[str, int]:
    """Summarize processing statuses."""
    summary = {
        "total_layers": len(rows),
        "succeeded": 0,
        "failed": 0,
        "skipped": 0,
        "likely_crs_errors": 0,
        "dry_run": 0,
    }
    for row in rows:
        status = str(row.get("status", ""))
        if status in summary:
            summary[status] += 1
        elif status == "likely_crs_error":
            summary["likely_crs_errors"] += 1
    return summary


def main(argv: list[str] | None = None) -> int:
    """Run the command-line workflow."""
    parser = build_parser()
    args = parser.parse_args(argv)
    logger = setup_logging(args.log_file)

    if args.max_workers < 1:
        parser.error("--max-workers must be at least 1")
    if not args.input_dir.exists():
        parser.error(f"--input-dir does not exist: {args.input_dir}")
    if args.overwrite and args.skip_existing:
        logger.warning("--overwrite takes precedence over --skip-existing.")

    ogrinfo = require_executable("ogrinfo", "GeoPackage inspection")
    ogr2ogr = ""
    tippecanoe = ""
    if not args.field_report_only:
        ogr2ogr = require_executable("ogr2ogr", "FlatGeoBuf conversion")
        tippecanoe = require_executable("tippecanoe", "PMTiles creation")

    logger.info("Started at %s", utc_now())
    config = load_config(args.config)
    jobs = discover_jobs(
        args.input_dir,
        args.fgb_dir,
        args.pmtiles_dir,
        config,
        ogrinfo,
        args.timeout,
        logger,
        args.outputs_next_to_input,
    )

    if args.report:
        inventory_path = args.report if args.field_report_only else field_report_path(args.report)
        write_report(inventory_path, field_inventory_rows(jobs))
        logger.info("Wrote field inventory report to %s", inventory_path)

    if args.field_report_only:
        logger.info("Field report only requested; no conversion work was run.")
        return 0

    extra_tippecanoe_args = parse_extra_tippecanoe_args(args.tippecanoe_extra_args)
    rows: list[dict[str, Any]] = []
    if args.max_workers == 1:
        for job in jobs:
            rows.append(
                process_job(
                    job, ogr2ogr, ogrinfo, tippecanoe, args, extra_tippecanoe_args
                )
            )
    else:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=args.max_workers
        ) as executor:
            futures = [
                executor.submit(
                    process_job,
                    job,
                    ogr2ogr,
                    ogrinfo,
                    tippecanoe,
                    args,
                    extra_tippecanoe_args,
                )
                for job in jobs
            ]
            for future in concurrent.futures.as_completed(futures):
                rows.append(future.result())

    rows.sort(key=lambda row: (row["source_path"], row["source_layer"]))
    if args.report:
        write_report(args.report, rows)
        logger.info("Wrote processing report to %s", args.report)

    summary = summarize(rows)
    logger.info(
        "Summary: total=%s succeeded=%s failed=%s skipped=%s likely_crs_errors=%s",
        summary["total_layers"],
        summary["succeeded"],
        summary["failed"],
        summary["skipped"],
        summary["likely_crs_errors"],
    )
    return 1 if summary["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
