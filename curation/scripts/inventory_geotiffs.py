"""Inventory GeoTIFF technical and descriptive metadata."""

from __future__ import annotations

import argparse
import csv
import html
import json
import re
import shutil
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

DEFAULT_INPUT_DIR = Path("purdue-campus")
DEFAULT_OUTPUT_DIR = Path("purdue-campus-metadata")

CSV_FIELDS = [
    "id",
    "source_path",
    "title",
    "year",
    "purpose",
    "abstract",
    "credits",
    "keywords",
    "metadata_date",
    "declared_epsg",
    "detected_epsg",
    "crs_name",
    "width",
    "height",
    "pixel_count",
    "resolution_x",
    "resolution_y",
    "band_count",
    "band_types",
    "color_interpretations",
    "nodata_values",
    "compression",
    "has_overviews",
    "overview_count",
    "source_size_bytes",
    "source_size_mb",
    "sidecar_size_bytes",
    "bbox_wgs84",
    "warnings",
]


def discover_geotiffs(input_directory: Path) -> list[Path]:
    """Find source GeoTIFF files recursively."""
    return sorted(
        path
        for path in input_directory.rglob("*")
        if path.is_file() and path.suffix.lower() in {".tif", ".tiff"}
    )


def require_command(command: str) -> str:
    """Return an executable path or raise a useful error."""
    executable = shutil.which(command)
    if not executable:
        raise RuntimeError(
            f"{command} was not found in PATH. Install GDAL and try again."
        )
    return executable


def run_gdalinfo(geotiff_path: Path) -> dict[str, Any]:
    """Extract technical raster metadata using gdalinfo JSON output."""
    result = subprocess.run(
        [require_command("gdalinfo"), "-json", str(geotiff_path)],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def clean_xml_text(value: str | None) -> str:
    """Normalize text extracted from ArcGIS metadata XML."""
    if not value:
        return ""
    value = html.unescape(value)
    value = re.sub(r"<[^>]+>", " ", value)
    return " ".join(value.split())


def first_xml_text(root: ET.Element, tag: str) -> str:
    """Return the first non-empty value for an XML tag."""
    for element in root.iter(tag):
        value = clean_xml_text(element.text)
        if value:
            return value
    return ""


def all_xml_text(root: ET.Element, tag: str) -> list[str]:
    """Return unique non-empty values for an XML tag."""
    values = []
    for element in root.iter(tag):
        value = clean_xml_text(element.text)
        if value and value not in values:
            values.append(value)
    return values


def extract_esri_metadata(xml_path: Path) -> dict[str, Any]:
    """Extract useful descriptive metadata from an Esri .tif.xml sidecar."""
    if not xml_path.exists():
        return {}

    root = ET.parse(xml_path).getroot()
    declared_epsg = first_xml_text(root, "identCode")
    if not declared_epsg:
        pe_xml = first_xml_text(root, "peXml")
        match = re.search(r"(?:LatestWKID|AUTHORITY)[^0-9]+([0-9]{4,6})", pe_xml)
        declared_epsg = match.group(1) if match else ""

    return {
        "title": first_xml_text(root, "resTitle"),
        "purpose": first_xml_text(root, "idPurp"),
        "abstract": first_xml_text(root, "idAbs"),
        "credits": first_xml_text(root, "idCredit"),
        "keywords": all_xml_text(root, "keyword"),
        "metadata_date": first_xml_text(root, "mdDateSt"),
        "creation_date": first_xml_text(root, "CreaDate"),
        "declared_epsg": declared_epsg,
    }


def sidecar_paths(geotiff_path: Path) -> dict[str, Path]:
    """Return expected sidecar paths for a GeoTIFF."""
    return {
        "world_file": geotiff_path.with_suffix(".tfw"),
        "metadata_xml": Path(f"{geotiff_path}.xml"),
        "aux_xml": Path(f"{geotiff_path}.aux.xml"),
        "external_overviews": Path(f"{geotiff_path}.ovr"),
    }


def sidecar_size(paths: dict[str, Path]) -> int:
    """Return the combined size of existing sidecar files."""
    return sum(path.stat().st_size for path in paths.values() if path.exists())


def bbox_from_extent(extent: dict[str, Any] | None) -> list[float] | None:
    """Calculate a bounding box from a GeoJSON Polygon extent."""
    if not extent or extent.get("type") != "Polygon":
        return None
    coordinates = extent.get("coordinates", [[]])[0]
    if not coordinates:
        return None
    xs = [coordinate[0] for coordinate in coordinates]
    ys = [coordinate[1] for coordinate in coordinates]
    return [min(xs), min(ys), max(xs), max(ys)]


def validate_source_raster(
    technical: dict[str, Any],
    descriptive: dict[str, Any],
    sidecars: dict[str, Path],
) -> list[str]:
    """Identify source metadata and processing concerns."""
    warnings = []
    stac = technical.get("stac", {})
    detected_epsg = stac.get("proj:epsg")
    declared_epsg = descriptive.get("declared_epsg")
    bands = technical.get("bands", [])

    if not technical.get("coordinateSystem"):
        warnings.append("missing_crs")
    if not detected_epsg:
        warnings.append("crs_not_resolved_to_epsg")
    if detected_epsg and declared_epsg and str(detected_epsg) != str(declared_epsg):
        warnings.append("crs_disagrees_with_xml")
    if not descriptive:
        warnings.append("missing_metadata_xml")
    if any(band.get("noDataValue") is None for band in bands):
        warnings.append("missing_nodata")
    if len(bands) not in {1, 3, 4}:
        warnings.append("unexpected_band_count")
    if technical.get("size", [0, 0])[0] * technical.get("size", [0, 0])[1] > 150_000_000:
        warnings.append("very_large_raster")
    if (
        technical.get("metadata", {})
        .get("IMAGE_STRUCTURE", {})
        .get("COMPRESSION", "NONE")
        == "NONE"
    ):
        warnings.append("uncompressed_source")
    for name, path in sidecars.items():
        if not path.exists():
            warnings.append(f"missing_{name}")
    return warnings


def build_inventory_record(
    geotiff_path: Path,
    input_directory: Path,
) -> dict[str, Any]:
    """Build a combined technical and descriptive metadata record."""
    technical = run_gdalinfo(geotiff_path)
    sidecars = sidecar_paths(geotiff_path)
    descriptive = extract_esri_metadata(sidecars["metadata_xml"])
    bands = technical.get("bands", [])
    width, height = technical.get("size", [0, 0])
    transform = technical.get("geoTransform", [None] * 6)
    image_structure = technical.get("metadata", {}).get("IMAGE_STRUCTURE", {})
    overviews = bands[0].get("overviews", []) if bands else []
    stac = technical.get("stac", {})
    bbox = bbox_from_extent(technical.get("wgs84Extent"))
    relative_path = geotiff_path.relative_to(input_directory)
    year_match = re.search(r"(?:18|19|20)\d{2}", geotiff_path.stem)

    return {
        "id": geotiff_path.stem,
        "source_path": str(relative_path),
        "title": descriptive.get("title", geotiff_path.name),
        "year": year_match.group(0) if year_match else "",
        "purpose": descriptive.get("purpose", ""),
        "abstract": descriptive.get("abstract", ""),
        "credits": descriptive.get("credits", ""),
        "keywords": descriptive.get("keywords", []),
        "metadata_date": descriptive.get("metadata_date", ""),
        "creation_date": descriptive.get("creation_date", ""),
        "declared_epsg": descriptive.get("declared_epsg", ""),
        "detected_epsg": stac.get("proj:epsg"),
        "crs_name": technical.get("coordinateSystem", {}).get("wkt", "").split("\n")[0],
        "width": width,
        "height": height,
        "pixel_count": width * height,
        "resolution_x": abs(transform[1]) if transform[1] is not None else None,
        "resolution_y": abs(transform[5]) if transform[5] is not None else None,
        "band_count": len(bands),
        "band_types": [band.get("type") for band in bands],
        "color_interpretations": [
            band.get("colorInterpretation") for band in bands
        ],
        "nodata_values": [band.get("noDataValue") for band in bands],
        "compression": image_structure.get("COMPRESSION", "NONE"),
        "has_overviews": bool(overviews),
        "overview_count": len(overviews),
        "source_size_bytes": geotiff_path.stat().st_size,
        "source_size_mb": round(geotiff_path.stat().st_size / 1024**2, 3),
        "sidecar_size_bytes": sidecar_size(sidecars),
        "bbox_wgs84": bbox,
        "geometry_wgs84": technical.get("wgs84Extent"),
        "sidecars": {
            name: str(path.relative_to(input_directory)) if path.exists() else None
            for name, path in sidecars.items()
        },
        "warnings": validate_source_raster(technical, descriptive, sidecars),
    }


def csv_value(value: Any) -> Any:
    """Serialize nested values for a CSV cell."""
    if isinstance(value, list):
        return "; ".join(str(item) for item in value)
    if value is None:
        return ""
    return value


def write_csv(records: list[dict[str, Any]], output_path: Path) -> None:
    """Write a flat inventory CSV."""
    with output_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for record in records:
            writer.writerow({field: csv_value(record.get(field)) for field in CSV_FIELDS})


def write_json(records: list[dict[str, Any]], output_path: Path) -> None:
    """Write the structured inventory JSON."""
    with output_path.open("w", encoding="utf-8") as file:
        json.dump(records, file, indent=2, ensure_ascii=False)


def write_stac_items(records: list[dict[str, Any]], output_directory: Path) -> None:
    """Write one basic STAC Item per raster."""
    output_directory.mkdir(parents=True, exist_ok=True)
    for record in records:
        declared_epsg = (
            int(record["declared_epsg"])
            if str(record["declared_epsg"]).isdigit()
            else None
        )
        item = {
            "stac_version": "1.0.0",
            "type": "Feature",
            "id": record["id"],
            "bbox": record["bbox_wgs84"],
            "geometry": record["geometry_wgs84"],
            "properties": {
                "datetime": None,
                "title": record["title"],
                "description": record["abstract"],
                "proj:epsg": record["detected_epsg"] or declared_epsg,
                "proj:shape": [record["height"], record["width"]],
                "gsd": record["resolution_x"],
            },
            "assets": {
                "source": {
                    "href": f"../../{record['source_path']}",
                    "type": "image/tiff; application=geotiff",
                    "roles": ["data"],
                }
            },
            "links": [],
        }
        with (output_directory / f"{record['id']}.json").open(
            "w", encoding="utf-8"
        ) as file:
            json.dump(item, file, indent=2, ensure_ascii=False)


def inventory_directory(
    input_directory: Path,
    output_directory: Path,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    """Inventory all GeoTIFFs in a directory and write catalog outputs."""
    geotiffs = discover_geotiffs(input_directory)
    if limit is not None:
        geotiffs = geotiffs[:limit]

    output_directory.mkdir(parents=True, exist_ok=True)
    records = []
    for index, geotiff_path in enumerate(geotiffs, start=1):
        print(f"[{index}/{len(geotiffs)}] Inventorying {geotiff_path}")
        records.append(build_inventory_record(geotiff_path, input_directory))

    write_csv(records, output_directory / "geotiff_inventory.csv")
    write_json(records, output_directory / "geotiff_inventory.json")
    write_stac_items(records, output_directory / "stac")
    return records


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract GeoTIFF technical and descriptive metadata."
    )
    parser.add_argument("input_directory", nargs="?", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument(
        "--output-directory", type=Path, default=DEFAULT_OUTPUT_DIR
    )
    parser.add_argument("--limit", type=int, help="Only inventory the first N rasters.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    records = inventory_directory(
        args.input_directory,
        args.output_directory,
        args.limit,
    )
    print(f"Wrote metadata for {len(records)} GeoTIFFs to {args.output_directory}")


if __name__ == "__main__":
    main()
