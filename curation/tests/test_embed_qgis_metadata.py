from __future__ import annotations

import re
import shutil
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from xml.etree import ElementTree as ET

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from curation.embed_qgis_metadata import embed_metadata_directory, get_default_template_path


class EmbedQgisMetadataTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.workdir = Path(self.tempdir.name)
        fixture_dir = REPO_ROOT / "test-records"

        for path in fixture_dir.glob("*.gpkg"):
            shutil.copy2(path, self.workdir / path.name)

        self.csv_path = self.workdir / "test-metadata.csv"
        self.template_path = get_default_template_path()
        shutil.copy2(fixture_dir / "test-metadata.csv", self.csv_path)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_embed_replaces_existing_metadata_and_populates_dynamic_spatial_blocks(self) -> None:
        target = self.workdir / "edge_locales_2024.gpkg"
        with sqlite3.connect(target) as conn:
            conn.execute("DROP TABLE IF EXISTS gpkg_metadata_reference")
            conn.execute("DROP TABLE IF EXISTS gpkg_metadata")
            conn.execute(
                """
                CREATE TABLE gpkg_metadata (
                    id INTEGER PRIMARY KEY,
                    md_scope TEXT NOT NULL,
                    md_standard_uri TEXT NOT NULL,
                    mime_type TEXT NOT NULL,
                    metadata TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                INSERT INTO gpkg_metadata (id, md_scope, md_standard_uri, mime_type, metadata)
                VALUES (1, 'dataset', 'stale', 'text/plain', 'stale metadata')
                """
            )
            conn.commit()

        summary = embed_metadata_directory(self.workdir, self.csv_path, self.template_path)

        self.assertEqual(
            summary.processed_files,
            [
                "edge_locales_2024.gpkg",
                "edge_school_district_office_locations_2016.gpkg",
            ],
        )
        self.assertEqual(
            summary.skipped_files,
            ["edge_school_district_composites_tl_16_2015.gpkg"],
        )
        self.assertEqual(
            summary.unused_metadata_rows,
            ["edge_school_district_composites_tl_15_2013.gpkg"],
        )

        with sqlite3.connect(target) as conn:
            row = conn.execute(
                "SELECT md_scope, md_standard_uri, mime_type, metadata FROM gpkg_metadata"
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual("dataset", row[0])
            self.assertEqual("http://mrcc.com/qgis.dtd", row[1])
            self.assertEqual("text/xml", row[2])
            xml_text = row[3]

            reference_row = conn.execute(
                """
                SELECT reference_scope, table_name, md_file_id
                FROM gpkg_metadata_reference
                """
            ).fetchone()
            self.assertEqual(("table", "edge_locales_2024", 1), reference_row)

        self.assertNotIn("EPSG:2248", xml_text)
        self.assertNotIn("Maryland", xml_text)
        self.assertNotIn("stale metadata", xml_text)

        xml_without_doctype = re.sub(r"^\s*<!DOCTYPE[^>]*>\s*", "", xml_text, count=1)
        root = ET.fromstring(xml_without_doctype)

        self.assertEqual(
            "NCES Geographic Locale Boundaries [United States] {2024}",
            root.findtext("./title"),
        )
        self.assertEqual("b1g_0RXeJihvKepA", root.findtext("./identifier"))
        self.assertEqual("2024", root.findtext("./extent/temporal/period/start"))
        self.assertEqual("2024", root.findtext("./extent/temporal/period/end"))
        self.assertEqual("EPSG:4269", root.findtext("./crs/spatialrefsys/authid"))
        self.assertEqual("EPSG:7019", root.findtext("./crs/spatialrefsys/ellipsoidacronym"))
        self.assertEqual("true", root.findtext("./crs/spatialrefsys/geographicflag"))
        self.assertEqual(
            "https://geo.btaa.org/catalog/b1g_0RXeJihvKepA",
            root.find("./links/link").attrib["url"],
        )

        spatial = root.find("./extent/spatial")
        self.assertIsNotNone(spatial)
        self.assertEqual("EPSG:4269", spatial.attrib["crs"])
        self.assertEqual("-179.16855109086", spatial.attrib["minx"])
        self.assertEqual("-14.548699019644", spatial.attrib["miny"])
        self.assertEqual("179.748724227741", spatial.attrib["maxx"])
        self.assertEqual("71.389610165177", spatial.attrib["maxy"])

    def test_unmatched_geopackage_is_left_without_metadata_tables(self) -> None:
        summary = embed_metadata_directory(self.workdir, self.csv_path, self.template_path)
        self.assertIn("edge_school_district_composites_tl_16_2015.gpkg", summary.skipped_files)

        target = self.workdir / "edge_school_district_composites_tl_16_2015.gpkg"
        with sqlite3.connect(target) as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    """
                    SELECT name
                    FROM sqlite_master
                    WHERE type = 'table'
                    AND name IN ('gpkg_metadata', 'gpkg_metadata_reference')
                    """
                )
            }

        self.assertEqual(set(), tables)


if __name__ == "__main__":
    unittest.main()
