#!/usr/bin/env python3
"""Inventory locally stored datasets in a GDRS ``data/pub`` tree.

The inventory intentionally ignores metadata, previews, layer files, and
external/service-only subresources. Composite datasets such as shapefiles and
File Geodatabases are emitted once, with a size that includes their component
files.
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import xml.etree.ElementTree as ET
from collections.abc import Iterable, Iterator
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_GDRS_ROOT = REPOSITORY_ROOT / "inputs" / "GDRS-December-2017"
DEFAULT_OUTPUT = REPOSITORY_ROOT / "outputs" / "gdrs_dataset_inventory.csv"
DEFAULT_PREFERRED_OUTPUT = (
    REPOSITORY_ROOT / "outputs" / "gdrs_dataset_inventory_preferred_formats.csv"
)

FORMAT_LABELS = {
    "aaigrid": "Arc/Info ASCII Grid",
    "cad": "CAD",
    "csv": "CSV",
    "fgdb": "File Geodatabase",
    "geojson": "GeoJSON",
    "gpkg": "GeoPackage",
    "kmz": "KML/KMZ",
    "shp": "Shapefile",
    "tif": "GeoTIFF",
    "tiles": "GeoTIFF tiles",
    "xlsx": "Excel",
}

# The first five entries are the requested order. The remaining formats keep
# raster-, CAD-, spreadsheet-, and package-only resources in the second pass.
FORMAT_PRIORITY = (
    "fgdb",
    "shp",
    "kmz",
    "geojson",
    "csv",
    "gpkg",
    "tif",
    "tiles",
    "aaigrid",
    "cad",
    "xlsx",
)

ATOMIC_EXTENSIONS = {
    "cad": {".dwg", ".dxf", ".dgn"},
    "geojson": {".geojson", ".json"},
    "gpkg": {".gpkg"},
    "kmz": {".kml", ".kmz"},
    "tif": {".tif", ".tiff"},
    "tiles": {".tif", ".tiff"},
    "xlsx": {".xls", ".xlsx"},
}

ROOT_EXTENSION_FORMATS = {
    ".asc": "aaigrid",
    ".csv": "csv",
    ".dgn": "cad",
    ".dwg": "cad",
    ".dxf": "cad",
    ".geojson": "geojson",
    ".gpkg": "gpkg",
    ".json": "geojson",
    ".kml": "kmz",
    ".kmz": "kmz",
    ".shp": "shp",
    ".tif": "tif",
    ".tiff": "tif",
    ".xls": "xlsx",
    ".xlsx": "xlsx",
}

NON_DATA_TEXT_NAMES = {
    "error.txt",
    "error_warning.txt",
    "license.txt",
    "readme.txt",
    "warning.txt",
}

SHAPEFILE_COMPONENT_EXTENSIONS = {
    ".aih",
    ".ain",
    ".atx",
    ".cpg",
    ".dbf",
    ".fbn",
    ".fbx",
    ".ixs",
    ".mxs",
    ".prj",
    ".qix",
    ".sbn",
    ".sbx",
    ".shp",
    ".shx",
}

CSV_FIELDS = [
    "organization",
    "resource",
    "dataset_filename",
    "format",
    "format_folder",
    "storage_location",
    "filesize_bytes",
    "filesize_mb",
    "component_count",
    "title",
    "creator_originator",
    "publication_date",
    "date_source",
    "dataset_path",
    "metadata_file",
]


@dataclass(frozen=True)
class MetadataFields:
    title: str = ""
    creator_originator: str = ""
    publication_date: str = ""
    date_source: str = ""
    metadata_file: str = ""


@dataclass(frozen=True)
class DatasetItem:
    path: Path
    component_paths: tuple[Path, ...]


@dataclass(frozen=True)
class InventoryRow:
    organization: str
    resource: str
    dataset_filename: str
    format: str
    format_folder: str
    storage_location: str
    filesize_bytes: int
    filesize_mb: str
    component_count: int
    title: str
    creator_originator: str
    publication_date: str
    date_source: str
    dataset_path: str
    metadata_file: str


@dataclass(frozen=True)
class InventoryStats:
    organizations: int
    resources: int
    resources_with_stored_data: int
    resources_without_stored_data: int
    stored_dataset_artifacts: int
    metadata_missing: int
    metadata_parse_errors: int


def resolve_pub_root(input_path: Path) -> Path:
    """Resolve an archive root, ``data`` directory, or ``pub`` directory."""
    input_path = input_path.expanduser().resolve()
    candidates = (input_path, input_path / "data" / "pub", input_path / "pub")
    for candidate in candidates:
        if candidate.is_dir() and candidate.name == "pub":
            return candidate
    raise FileNotFoundError(
        f"Could not find a pub directory at or below {input_path} "
        "(expected PATH, PATH/data/pub, or PATH/pub)."
    )


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower()


def _clean_text(value: str | None) -> str:
    return " ".join((value or "").split())


def _path_values(root: ET.Element, path: tuple[str, ...]) -> list[str]:
    nodes = [root]
    for expected_tag in path:
        nodes = [
            child
            for node in nodes
            for child in node
            if _local_name(child.tag) == expected_tag.lower()
        ]
    return [value for node in nodes if (value := _clean_text(node.text))]


def _first_path_value(root: ET.Element, paths: Iterable[tuple[str, ...]]) -> str:
    for path in paths:
        values = _path_values(root, path)
        if values:
            return values[0]
    return ""


def _unique_path_values(root: ET.Element, path: tuple[str, ...]) -> list[str]:
    return list(dict.fromkeys(_path_values(root, path)))


def normalize_fgdc_date(value: str) -> str:
    """Convert common FGDC compact dates to ISO form, preserving other text."""
    value = _clean_text(value)
    compact_formats = {
        r"\d{8}": ("%Y%m%d", "%Y-%m-%d"),
        r"\d{6}": ("%Y%m", "%Y-%m"),
        r"\d{4}": ("%Y", "%Y"),
    }
    for pattern, (input_format, output_format) in compact_formats.items():
        if re.fullmatch(pattern, value):
            try:
                return datetime.strptime(value, input_format).strftime(output_format)
            except ValueError:
                return value
    return value


def _preferred_metadata_file(resource_dir: Path) -> Path | None:
    metadata_dir = resource_dir / "metadata"
    if not metadata_dir.is_dir():
        return None

    xml_files = sorted(
        path
        for path in metadata_dir.iterdir()
        if path.is_file() and path.suffix.lower() == ".xml"
    )
    if not xml_files:
        return None

    for path in xml_files:
        if path.name.lower() == "metadata.xml":
            return path
    return xml_files[0]


def _parse_xml(path: Path | None) -> tuple[ET.Element | None, bool]:
    if path is None:
        return None, False
    try:
        return ET.parse(path).getroot(), False
    except (ET.ParseError, OSError):
        return None, True


def extract_metadata(
    resource_dir: Path, pub_root: Path
) -> tuple[MetadataFields, bool, bool]:
    """Extract preferred FGDC fields, with GDRS resource metadata fallbacks."""
    metadata_path = _preferred_metadata_file(resource_dir)
    fgdc_root, metadata_parse_error = _parse_xml(metadata_path)
    data_resource_path = resource_dir / "dataResource.xml"
    data_resource_root, _ = _parse_xml(
        data_resource_path if data_resource_path.is_file() else None
    )

    title = ""
    originators: list[str] = []
    publication_date = ""
    date_source = ""

    if fgdc_root is not None:
        title = _first_path_value(
            fgdc_root,
            (
                ("idinfo", "citation", "citeinfo", "title"),
                ("dataidinfo", "idcitation", "restitle"),
            ),
        )
        originators = _unique_path_values(
            fgdc_root, ("idinfo", "citation", "citeinfo", "origin")
        )
        if not originators:
            originators = _unique_path_values(
                fgdc_root,
                ("idinfo", "citation", "citeinfo", "pubinfo", "publish"),
            )
        publication_date = _first_path_value(
            fgdc_root, (("idinfo", "citation", "citeinfo", "pubdate"),)
        )
        if publication_date:
            date_source = "FGDC pubdate"

    if data_resource_root is not None:
        if not title:
            title = _first_path_value(data_resource_root, (("descriptivename",),))
        if not originators:
            publisher = _first_path_value(data_resource_root, (("publisher",),))
            if publisher:
                originators = [publisher]
        if not publication_date:
            publication_date = _first_path_value(
                data_resource_root, (("currentasofdate",),)
            )
            if publication_date:
                date_source = "GDRS currentAsofDate"

    if not publication_date and fgdc_root is not None:
        publication_date = _first_path_value(fgdc_root, (("metainfo", "metd"),))
        if publication_date:
            date_source = "FGDC metadata date"

    relative_metadata = ""
    if metadata_path is not None:
        relative_metadata = metadata_path.relative_to(pub_root).as_posix()

    return (
        MetadataFields(
            title=title,
            creator_originator="; ".join(originators),
            publication_date=normalize_fgdc_date(publication_date),
            date_source=date_source,
            metadata_file=relative_metadata,
        ),
        metadata_path is None,
        metadata_parse_error,
    )


def _recursive_size(path: Path) -> tuple[int, tuple[Path, ...]]:
    if path.is_file():
        return path.stat().st_size, (path,)
    files = tuple(sorted(child for child in path.rglob("*") if child.is_file()))
    return sum(child.stat().st_size for child in files), files


def _shapefile_components(shapefile: Path) -> tuple[Path, ...]:
    stem = shapefile.stem.lower()
    return tuple(
        sorted(
            path
            for path in shapefile.parent.iterdir()
            if path.is_file()
            and path.stem.lower() == stem
            and path.suffix.lower() in SHAPEFILE_COMPONENT_EXTENSIONS
        )
    )


def _raster_components(raster: Path) -> tuple[Path, ...]:
    stem = raster.stem.lower()
    filename = raster.name.lower()
    allowed_names = {
        f"{stem}.aux",
        f"{stem}.tfw",
        f"{stem}.wld",
        f"{filename}.aux.xml",
        f"{filename}.ovr",
        f"{filename}.xml",
    }
    return tuple(
        sorted(
            path
            for path in raster.parent.iterdir()
            if path.is_file() and (path == raster or path.name.lower() in allowed_names)
        )
    )


def _file_geodatabases(format_dir: Path) -> Iterator[DatasetItem]:
    for current_dir, directory_names, _ in os.walk(format_dir):
        current = Path(current_dir)
        geodatabases = sorted(
            name for name in directory_names if name.lower().endswith(".gdb")
        )
        for name in geodatabases:
            path = current / name
            _, files = _recursive_size(path)
            if files:
                yield DatasetItem(path=path, component_paths=files)
        directory_names[:] = [
            name for name in directory_names if not name.lower().endswith(".gdb")
        ]


def _ascii_grids(format_dir: Path) -> Iterator[DatasetItem]:
    for path in sorted(format_dir.rglob("*")):
        if path.is_file() and path.suffix.lower() == ".asc":
            components = tuple(
                sorted(
                    candidate
                    for candidate in path.parent.iterdir()
                    if candidate.is_file()
                    and candidate.stem.lower() == path.stem.lower()
                    and candidate.suffix.lower() in {".asc", ".prj"}
                )
            )
            yield DatasetItem(path=path, component_paths=components)
        elif path.is_dir() and (path / "hdr.adf").is_file():
            _, files = _recursive_size(path)
            yield DatasetItem(path=path, component_paths=files)


def dataset_items(format_dir: Path) -> Iterator[DatasetItem]:
    """Yield primary dataset artifacts from one recognized format directory."""
    format_name = format_dir.name.lower()
    if format_name == "fgdb":
        yield from _file_geodatabases(format_dir)
        return
    if format_name == "aaigrid":
        yield from _ascii_grids(format_dir)
        return

    if format_name == "shp":
        for path in sorted(format_dir.rglob("*")):
            if path.is_file() and path.suffix.lower() == ".shp":
                yield DatasetItem(
                    path=path, component_paths=_shapefile_components(path)
                )
        return

    if format_name == "csv":
        for path in sorted(format_dir.rglob("*")):
            if not path.is_file():
                continue
            is_csv = path.suffix.lower() == ".csv"
            is_data_text = (
                path.suffix.lower() == ".txt"
                and path.name.lower() not in NON_DATA_TEXT_NAMES
            )
            if is_csv or is_data_text:
                yield DatasetItem(path=path, component_paths=(path,))
        return

    extensions = ATOMIC_EXTENSIONS.get(format_name, set())
    for path in sorted(format_dir.rglob("*")):
        if path.is_file() and path.suffix.lower() in extensions:
            components = (
                _raster_components(path)
                if path.suffix.lower() in {".tif", ".tiff"}
                else (path,)
            )
            yield DatasetItem(path=path, component_paths=components)


def resource_root_dataset_items(
    resource_dir: Path,
) -> Iterator[tuple[str, DatasetItem]]:
    """Yield recognizable datasets stored outside a format-named directory."""
    for path in sorted(resource_dir.iterdir()):
        if path.is_dir() and path.suffix.lower() == ".gdb":
            _, files = _recursive_size(path)
            if files:
                yield "fgdb", DatasetItem(path=path, component_paths=files)
            continue
        if not path.is_file():
            continue

        format_name = ROOT_EXTENSION_FORMATS.get(path.suffix.lower())
        if format_name is None:
            continue
        if format_name == "shp":
            components = _shapefile_components(path)
        elif format_name == "tif":
            components = _raster_components(path)
        elif format_name == "aaigrid":
            components = tuple(
                sorted(
                    candidate
                    for candidate in path.parent.iterdir()
                    if candidate.is_file()
                    and candidate.stem.lower() == path.stem.lower()
                    and candidate.suffix.lower() in {".asc", ".prj"}
                )
            )
        else:
            components = (path,)
        yield format_name, DatasetItem(path=path, component_paths=components)


def inventory_gdrs(pub_root: Path) -> tuple[list[InventoryRow], InventoryStats]:
    """Build inventory rows and collection-level counts for a GDRS pub tree."""
    pub_root = resolve_pub_root(pub_root)
    rows: list[InventoryRow] = []
    resource_count = 0
    resources_with_data = 0
    metadata_missing = 0
    metadata_parse_errors = 0

    organizations = sorted(path for path in pub_root.iterdir() if path.is_dir())
    for organization_dir in organizations:
        resources = sorted(path for path in organization_dir.iterdir() if path.is_dir())
        for resource_dir in resources:
            resource_count += 1
            metadata, is_missing, has_parse_error = extract_metadata(
                resource_dir, pub_root
            )
            metadata_missing += int(is_missing)
            metadata_parse_errors += int(has_parse_error)

            resource_rows_before = len(rows)
            resource_items: list[tuple[str, str, DatasetItem]] = []
            seen_dataset_names: set[tuple[str, str]] = set()
            for format_dir in sorted(
                path
                for path in resource_dir.iterdir()
                if path.is_dir() and path.name.lower() in FORMAT_LABELS
            ):
                format_folder = format_dir.name.lower()
                for item in dataset_items(format_dir):
                    key = (format_folder, item.path.name.casefold())
                    seen_dataset_names.add(key)
                    resource_items.append((format_folder, "format_directory", item))

            for format_name, item in resource_root_dataset_items(resource_dir):
                key = (format_name, item.path.name.casefold())
                if key not in seen_dataset_names:
                    resource_items.append((format_name, "resource_root", item))

            for format_name, storage_location, item in resource_items:
                size_bytes = sum(path.stat().st_size for path in item.component_paths)
                rows.append(
                    InventoryRow(
                        organization=organization_dir.name,
                        resource=resource_dir.name,
                        dataset_filename=item.path.name,
                        format=FORMAT_LABELS[format_name],
                        format_folder=format_name,
                        storage_location=storage_location,
                        filesize_bytes=size_bytes,
                        filesize_mb=f"{size_bytes / (1024 * 1024):.3f}",
                        component_count=len(item.component_paths),
                        title=metadata.title,
                        creator_originator=metadata.creator_originator,
                        publication_date=metadata.publication_date,
                        date_source=metadata.date_source,
                        dataset_path=item.path.relative_to(pub_root).as_posix(),
                        metadata_file=metadata.metadata_file,
                    )
                )
            if len(rows) > resource_rows_before:
                resources_with_data += 1

    rows.sort(
        key=lambda row: (
            row.organization.lower(),
            row.resource.lower(),
            row.format_folder,
            row.dataset_path.lower(),
        )
    )
    return rows, InventoryStats(
        organizations=len(organizations),
        resources=resource_count,
        resources_with_stored_data=resources_with_data,
        resources_without_stored_data=resource_count - resources_with_data,
        stored_dataset_artifacts=len(rows),
        metadata_missing=metadata_missing,
        metadata_parse_errors=metadata_parse_errors,
    )


def write_inventory(path: Path, rows: Iterable[InventoryRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(asdict(row) for row in rows)


def select_preferred_format_rows(
    rows: Iterable[InventoryRow],
) -> list[InventoryRow]:
    """Keep rows from the highest-priority available format in each resource."""
    rows_by_resource: dict[tuple[str, str], list[InventoryRow]] = {}
    for row in rows:
        rows_by_resource.setdefault((row.organization, row.resource), []).append(row)

    priority = {format_name: rank for rank, format_name in enumerate(FORMAT_PRIORITY)}
    selected: list[InventoryRow] = []
    for resource_rows in rows_by_resource.values():
        chosen_format = min(
            {row.format_folder for row in resource_rows},
            key=lambda format_name: (
                priority.get(format_name, len(priority)),
                format_name,
            ),
        )
        selected.extend(
            row for row in resource_rows if row.format_folder == chosen_format
        )

    return sorted(
        selected,
        key=lambda row: (
            row.organization.lower(),
            row.resource.lower(),
            row.dataset_path.lower(),
        ),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "input",
        nargs="?",
        type=Path,
        default=DEFAULT_GDRS_ROOT,
        help="GDRS archive root or pub directory (default: %(default)s)",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="output CSV path (default depends on inventory mode)",
    )
    parser.add_argument(
        "--one-format-per-resource",
        "--preferred-formats-only",
        action="store_true",
        help="retain only the highest-priority available format per resource",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        rows, stats = inventory_gdrs(args.input)
        if args.one_format_per_resource:
            rows = select_preferred_format_rows(rows)
        output = args.output or (
            DEFAULT_PREFERRED_OUTPUT if args.one_format_per_resource else DEFAULT_OUTPUT
        )
        write_inventory(output, rows)
    except (FileNotFoundError, NotADirectoryError, PermissionError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    print(f"Wrote {len(rows):,} rows to {output.resolve()}")
    if args.one_format_per_resource:
        print(
            "Applied one-format-per-resource priority: " + " > ".join(FORMAT_PRIORITY)
        )
    print(f"Organizations: {stats.organizations:,}")
    print(f"Catalog resources: {stats.resources:,}")
    print(f"Resources with stored data: {stats.resources_with_stored_data:,}")
    print(f"Resources without stored data: {stats.resources_without_stored_data:,}")
    print(f"Stored dataset artifacts found: {stats.stored_dataset_artifacts:,}")
    print(f"Resources missing metadata XML: {stats.metadata_missing:,}")
    print(f"Metadata XML parse errors: {stats.metadata_parse_errors:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
