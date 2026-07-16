"""Embed QGIS-style metadata XML into GeoPackages."""

from __future__ import annotations

import argparse
import csv
import logging
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable
from xml.etree import ElementTree as ET


LOGGER = logging.getLogger(__name__)

QGIS_METADATA_URI = "http://mrcc.com/qgis.dtd"
GPKG_METADATA_EXTENSION_URI = "http://www.geopackage.org/spec/#extension_metadata"
TOKEN_PATTERN = re.compile(r"\{([^{}]+)\}\}?")
DOCTYPE_PATTERN = re.compile(r"^\s*(<!DOCTYPE[^>]*>)\s*", re.MULTILINE)
YEAR_PATTERN = re.compile(r"\d{4}")
DEFAULT_TEMPLATE_PATH = (
    Path(__file__).resolve().parents[2] / "scripts" / "templates" / "qgis-metadata.xml"
)


@dataclass(frozen=True)
class MetadataRecord:
    """Single CSV metadata record with case-insensitive lookups."""

    values: dict[str, str]
    lower_values: dict[str, str]

    @classmethod
    def from_csv_row(cls, row: dict[str, str | None]) -> "MetadataRecord":
        values = {
            (key or "").strip(): "" if value is None else value
            for key, value in row.items()
            if key
        }
        lower_values = {key.lower(): value for key, value in values.items()}
        return cls(values=values, lower_values=lower_values)

    def get(self, key: str) -> str:
        return self.values.get(key, self.lower_values.get(key.lower(), ""))

    def resolve_token(self, token: str, today: date) -> str:
        normalized_token = token.strip()
        lowered_token = normalized_token.lower()

        if lowered_token == "now":
            return today.isoformat()

        if lowered_token.endswith(" first value"):
            field_name = normalized_token[: -len(" first value")]
            return extract_range_value(self.get(field_name), first=True)

        if lowered_token.endswith(" last value"):
            field_name = normalized_token[: -len(" last value")]
            return extract_range_value(self.get(field_name), first=False)

        return self.get(normalized_token)


@dataclass
class SpatialMetadata:
    """Spatial values used to overwrite the template CRS and extent blocks."""

    wkt: str = ""
    proj4: str = ""
    srsid: str = ""
    srid: str = ""
    authid: str = ""
    description: str = ""
    projectionacronym: str = ""
    ellipsoidacronym: str = ""
    geographicflag: str = ""
    extent: dict[str, str] = field(
        default_factory=lambda: {
            "minx": "",
            "miny": "",
            "maxx": "",
            "maxy": "",
            "minz": "0",
            "maxz": "0",
            "crs": "",
            "dimensions": "2",
        }
    )


@dataclass
class EmbedSummary:
    """Summary of an embedding run."""

    processed_files: list[str] = field(default_factory=list)
    skipped_files: list[str] = field(default_factory=list)
    unused_metadata_rows: list[str] = field(default_factory=list)


def _format_number(value: float | int | None) -> str:
    if value is None:
        return ""
    return format(float(value), ".15g")


def _infer_geographic_flag_from_wkt(wkt: str) -> str:
    if "GEOGCS[" in wkt or "GEODCRS[" in wkt:
        return "true"
    if "PROJCS[" in wkt or "PROJCRS[" in wkt:
        return "false"
    return ""


def _extract_ellipsoid_acronym(wkt: str) -> str:
    patterns = (
        r'SPHEROID\[[^\]]*AUTHORITY\["([^"]+)","([^"]+)"\]',
        r'ELLIPSOID\[[^\]]*AUTHORITY\["([^"]+)","([^"]+)"\]',
        r'ELLIPSOID\[[^\]]*ID\["([^"]+)",\s*([0-9]+)\s*\]',
    )
    for pattern in patterns:
        match = re.search(pattern, wkt)
        if match:
            return f"{match.group(1)}:{match.group(2)}"
    return ""


def _extract_projection_acronym(wkt: str, geographic_flag: str) -> str:
    if geographic_flag == "true":
        return "longlat"

    projection_patterns = (
        (r'PROJECTION\["Lambert_Conformal_Conic', "lcc"),
        (r'METHOD\["Lambert Conic Conformal', "lcc"),
        (r'PROJECTION\["Transverse_Mercator', "tmerc"),
        (r'METHOD\["Transverse Mercator', "tmerc"),
        (r'PROJECTION\["Mercator', "merc"),
        (r'METHOD\["Mercator', "merc"),
        (r'PROJECTION\["Albers', "aea"),
        (r'METHOD\["Albers', "aea"),
    )
    for pattern, acronym in projection_patterns:
        if re.search(pattern, wkt):
            return acronym
    return ""


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ? LIMIT 1",
        (name,),
    ).fetchone()
    return row is not None


def get_feature_tables(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        """
        SELECT table_name
        FROM gpkg_contents
        WHERE data_type = 'features'
        ORDER BY table_name
        """
    ).fetchall()
    return [table_name for (table_name,) in rows]


def extract_range_value(value: str, *, first: bool) -> str:
    years = YEAR_PATTERN.findall(value or "")
    if years:
        return years[0] if first else years[-1]

    separators = ("|", ";", ",", "/")
    cleaned = value.strip()
    if not cleaned:
        return ""

    for separator in separators:
        if separator in cleaned:
            parts = [part.strip() for part in cleaned.split(separator) if part.strip()]
            if parts:
                return parts[0] if first else parts[-1]

    return cleaned


def load_metadata_lookup(csv_path: Path, match_column: str) -> dict[str, MetadataRecord]:
    with csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError(f"CSV has no header row: {csv_path}")

        normalized_headers = {header.strip().lower(): header for header in reader.fieldnames if header}
        source_column = normalized_headers.get(match_column.lower())
        if source_column is None:
            raise KeyError(
                f"Missing required match column {match_column!r} in {csv_path}. "
                f"Found columns: {reader.fieldnames}"
            )

        lookup: dict[str, MetadataRecord] = {}
        for row in reader:
            record = MetadataRecord.from_csv_row(row)
            filename = record.get(source_column).strip()
            if not filename:
                continue
            if filename in lookup:
                raise ValueError(f"Duplicate metadata row for {filename!r} in {csv_path}")
            lookup[filename] = record

    return lookup


def extract_spatial_metadata(gpkg_path: Path) -> SpatialMetadata:
    with sqlite3.connect(gpkg_path) as conn:
        contents_row = conn.execute(
            """
            SELECT table_name, min_x, min_y, max_x, max_y, srs_id
            FROM gpkg_contents
            WHERE data_type = 'features'
            ORDER BY table_name
            LIMIT 1
            """
        ).fetchone()

        if contents_row is None:
            raise RuntimeError(f"No feature tables found in {gpkg_path}")

        _, min_x, min_y, max_x, max_y, srs_id = contents_row
        srs_row = conn.execute(
            """
            SELECT srs_name, srs_id, organization, organization_coordsys_id, definition, description
            FROM gpkg_spatial_ref_sys
            WHERE srs_id = ?
            """,
            (srs_id,),
        ).fetchone()

    spatial = SpatialMetadata()
    spatial.extent.update(
        {
            "minx": _format_number(min_x),
            "miny": _format_number(min_y),
            "maxx": _format_number(max_x),
            "maxy": _format_number(max_y),
        }
    )

    if srs_row is not None:
        srs_name, gpkg_srs_id, organization, organization_coordsys_id, definition, description = srs_row
        spatial.wkt = definition or ""
        spatial.srsid = str(gpkg_srs_id or "")
        spatial.srid = str(organization_coordsys_id or gpkg_srs_id or "")
        if organization and organization_coordsys_id:
            spatial.authid = f"{organization}:{organization_coordsys_id}"
            spatial.extent["crs"] = spatial.authid
        spatial.description = description or srs_name or ""
        spatial.geographicflag = _infer_geographic_flag_from_wkt(spatial.wkt)
        spatial.ellipsoidacronym = _extract_ellipsoid_acronym(spatial.wkt)
        spatial.projectionacronym = _extract_projection_acronym(
            spatial.wkt, spatial.geographicflag
        )

    return spatial


def repair_template(template_text: str) -> tuple[str, str]:
    doctype_match = DOCTYPE_PATTERN.search(template_text)
    doctype = doctype_match.group(1) if doctype_match else ""
    body = DOCTYPE_PATTERN.sub("", template_text, count=1)

    if "<history>" in body and "</history>" not in body and "<dates>" in body:
        body = re.sub(
            r"(<history>)(.*?)(\n\s*<dates>)",
            r"\1\2</history>\3",
            body,
            count=1,
            flags=re.DOTALL,
        )

    return doctype, body


def replace_tokens(value: str, record: MetadataRecord, today: date) -> str:
    def substitute(match: re.Match[str]) -> str:
        token = match.group(1).strip()
        replacement = record.resolve_token(token, today)
        if replacement == "":
            LOGGER.debug("Template token %r did not resolve to a value", token)
        return replacement

    return TOKEN_PATTERN.sub(substitute, value)


def replace_tokens_in_tree(element: ET.Element, record: MetadataRecord, today: date) -> None:
    if element.text is not None:
        element.text = replace_tokens(element.text, record, today)

    if element.tail is not None:
        element.tail = replace_tokens(element.tail, record, today)

    for key, value in list(element.attrib.items()):
        element.attrib[key] = replace_tokens(value, record, today)

    for child in list(element):
        replace_tokens_in_tree(child, record, today)


def set_spatialrefsys(root: ET.Element, spatial: SpatialMetadata) -> None:
    crs_element = root.find("./crs")
    if crs_element is None:
        crs_element = ET.SubElement(root, "crs")

    spatialrefsys_element = crs_element.find("./spatialrefsys")
    if spatialrefsys_element is None:
        spatialrefsys_element = ET.SubElement(crs_element, "spatialrefsys")

    spatialrefsys_element.clear()
    spatialrefsys_element.attrib["nativeFormat"] = "Wkt"

    child_values = [
        ("wkt", spatial.wkt),
        ("proj4", spatial.proj4),
        ("srsid", spatial.srsid),
        ("srid", spatial.srid),
        ("authid", spatial.authid),
        ("description", spatial.description),
        ("projectionacronym", spatial.projectionacronym),
        ("ellipsoidacronym", spatial.ellipsoidacronym),
        ("geographicflag", spatial.geographicflag),
    ]
    for tag, value in child_values:
        child = ET.SubElement(spatialrefsys_element, tag)
        child.text = value


def set_extent_spatial(root: ET.Element, spatial: SpatialMetadata) -> None:
    extent_element = root.find("./extent")
    if extent_element is None:
        extent_element = ET.SubElement(root, "extent")

    spatial_element = extent_element.find("./spatial")
    if spatial_element is None:
        spatial_element = ET.SubElement(extent_element, "spatial")

    spatial_element.attrib.clear()
    spatial_element.attrib.update(spatial.extent)


def build_metadata_xml(template_path: Path, record: MetadataRecord, gpkg_path: Path) -> str:
    today = date.today()
    template_text = template_path.read_text(encoding="utf-8")
    doctype, template_body = repair_template(template_text)
    root = ET.fromstring(template_body)

    replace_tokens_in_tree(root, record, today)
    spatial = extract_spatial_metadata(gpkg_path)
    set_spatialrefsys(root, spatial)
    set_extent_spatial(root, spatial)

    ET.indent(root, space="  ")
    xml_body = ET.tostring(root, encoding="unicode", short_empty_elements=True)
    if doctype:
        return f"{doctype}\n{xml_body}\n"
    return f"{xml_body}\n"


def ensure_metadata_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE gpkg_metadata (
            id INTEGER CONSTRAINT m_pk PRIMARY KEY ASC NOT NULL,
            md_scope TEXT NOT NULL DEFAULT 'dataset',
            md_standard_uri TEXT NOT NULL,
            mime_type TEXT NOT NULL DEFAULT 'text/xml',
            metadata TEXT NOT NULL DEFAULT ''
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE gpkg_metadata_reference (
            reference_scope TEXT NOT NULL,
            table_name TEXT,
            column_name TEXT,
            row_id_value INTEGER,
            timestamp DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            md_file_id INTEGER NOT NULL,
            md_parent_id INTEGER,
            FOREIGN KEY (md_file_id) REFERENCES gpkg_metadata(id),
            FOREIGN KEY (md_parent_id) REFERENCES gpkg_metadata(id)
        )
        """
    )


def refresh_extension_rows(conn: sqlite3.Connection) -> None:
    if not table_exists(conn, "gpkg_extensions"):
        return

    conn.execute("DELETE FROM gpkg_extensions WHERE extension_name = 'gpkg_metadata'")
    conn.executemany(
        """
        INSERT INTO gpkg_extensions (table_name, column_name, extension_name, definition, scope)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            ("gpkg_metadata", None, "gpkg_metadata", GPKG_METADATA_EXTENSION_URI, "read-write"),
            (
                "gpkg_metadata_reference",
                None,
                "gpkg_metadata",
                GPKG_METADATA_EXTENSION_URI,
                "read-write",
            ),
        ],
    )


def write_metadata_to_gpkg(gpkg_path: Path, metadata_xml: str) -> None:
    with sqlite3.connect(gpkg_path) as conn:
        feature_tables = get_feature_tables(conn)
        if not feature_tables:
            raise RuntimeError(f"No feature tables found in {gpkg_path}")

        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("BEGIN")
        try:
            if table_exists(conn, "gpkg_metadata_reference"):
                conn.execute("DROP TABLE gpkg_metadata_reference")
            if table_exists(conn, "gpkg_metadata"):
                conn.execute("DROP TABLE gpkg_metadata")

            ensure_metadata_tables(conn)
            refresh_extension_rows(conn)

            cursor = conn.execute(
                """
                INSERT INTO gpkg_metadata (md_scope, md_standard_uri, mime_type, metadata)
                VALUES (?, ?, ?, ?)
                """,
                ("dataset", QGIS_METADATA_URI, "text/xml", metadata_xml),
            )
            metadata_id = cursor.lastrowid

            for table_name in feature_tables:
                conn.execute(
                    """
                    INSERT INTO gpkg_metadata_reference (
                        reference_scope,
                        table_name,
                        column_name,
                        row_id_value,
                        timestamp,
                        md_file_id,
                        md_parent_id
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    ("table", table_name, None, None, timestamp, metadata_id, None),
                )

            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise


def iter_geopackages(gpkg_dir: Path) -> Iterable[Path]:
    return sorted(path for path in gpkg_dir.iterdir() if path.suffix.lower() == ".gpkg")


def embed_metadata_directory(
    gpkg_dir: Path,
    metadata_csv: Path,
    template_xml: Path,
    *,
    match_column: str = "filename",
) -> EmbedSummary:
    if not gpkg_dir.is_dir():
        raise NotADirectoryError(gpkg_dir)
    if not metadata_csv.is_file():
        raise FileNotFoundError(metadata_csv)
    if not template_xml.is_file():
        raise FileNotFoundError(template_xml)

    metadata_lookup = load_metadata_lookup(metadata_csv, match_column)
    used_rows: set[str] = set()
    summary = EmbedSummary()

    for gpkg_path in iter_geopackages(gpkg_dir):
        record = metadata_lookup.get(gpkg_path.name)
        if record is None:
            LOGGER.warning("No metadata row matched %s", gpkg_path.name)
            summary.skipped_files.append(gpkg_path.name)
            continue

        metadata_xml_text = build_metadata_xml(template_xml, record, gpkg_path)
        write_metadata_to_gpkg(gpkg_path, metadata_xml_text)
        summary.processed_files.append(gpkg_path.name)
        used_rows.add(gpkg_path.name)
        LOGGER.info("Embedded metadata into %s", gpkg_path.name)

    summary.unused_metadata_rows = sorted(
        filename for filename in metadata_lookup if filename not in used_rows
    )
    for filename in summary.unused_metadata_rows:
        LOGGER.warning("Metadata row did not match a GeoPackage: %s", filename)

    summary.processed_files.sort()
    summary.skipped_files.sort()
    return summary


def get_default_template_path() -> Path:
    return DEFAULT_TEMPLATE_PATH


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Embed QGIS-style metadata XML into every GeoPackage in a directory."
    )
    parser.add_argument("gpkg_dir", type=Path, help="Directory containing GeoPackages")
    parser.add_argument("metadata_csv", type=Path, help="CSV with dataset metadata rows")
    parser.add_argument(
        "template_xml",
        type=Path,
        nargs="?",
        default=get_default_template_path(),
        help=(
            "QGIS metadata XML template. "
            f"Defaults to {get_default_template_path()}"
        ),
    )
    parser.add_argument(
        "--match-column",
        default="filename",
        help="CSV column used to match metadata rows to GeoPackage filenames",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    summary = embed_metadata_directory(
        args.gpkg_dir,
        args.metadata_csv,
        args.template_xml,
        match_column=args.match_column,
    )
    LOGGER.info(
        "Processed %s GeoPackage(s), skipped %s, unused metadata rows %s",
        len(summary.processed_files),
        len(summary.skipped_files),
        len(summary.unused_metadata_rows),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
