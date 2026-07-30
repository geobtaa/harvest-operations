from __future__ import annotations

import logging
import sqlite3
import sys
from pathlib import Path


CURATION_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CURATION_ROOT / "scripts"))

import build_pmtiles_from_gpkg as builder  # noqa: E402


def make_job(
    *,
    fields: list[str] | None = None,
    geometry_column: str = "",
) -> builder.LayerJob:
    return builder.LayerJob(
        source_path=Path("example.gpkg"),
        source_layer="example",
        source_stem_slug="example",
        layer_slug="example",
        output_stem="example",
        fgb_path=Path("example.fgb"),
        pmtiles_path=Path("example.pmtiles"),
        geometry_column=geometry_column,
        fields=[{"name": name, "type": "String"} for name in (fields or [])],
    )


def write_minimal_geopackage(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.executescript(
            """
            CREATE TABLE gpkg_contents (
                table_name TEXT PRIMARY KEY,
                data_type TEXT,
                min_x REAL,
                min_y REAL,
                max_x REAL,
                max_y REAL,
                srs_id INTEGER
            );
            CREATE TABLE gpkg_geometry_columns (
                table_name TEXT,
                column_name TEXT,
                geometry_type_name TEXT,
                srs_id INTEGER
            );
            CREATE TABLE gpkg_spatial_ref_sys (
                srs_id INTEGER PRIMARY KEY,
                organization TEXT,
                organization_coordsys_id INTEGER,
                definition TEXT
            );
            CREATE TABLE example (
                fid INTEGER PRIMARY KEY,
                geom BLOB,
                label TEXT
            );
            INSERT INTO gpkg_contents VALUES (
                'example', 'features', -83.1, 39.8, -82.8, 40.1, 4326
            );
            INSERT INTO gpkg_geometry_columns VALUES (
                'example', 'geom', 'POINT', 4326
            );
            INSERT INTO gpkg_spatial_ref_sys VALUES (
                4326, 'EPSG', 4326, 'GEOGCRS["WGS 84"]'
            );
            INSERT INTO example VALUES (1, NULL, 'test');
            """
        )
        connection.commit()
    finally:
        connection.close()


def test_default_optional_keep_fields_do_not_warn_when_missing() -> None:
    job = make_job(fields=["source_identifier"])

    builder.select_fields_for_layer(
        job,
        builder.DEFAULT_CONFIG,
        logging.getLogger("test_build_pmtiles"),
    )

    assert job.warnings == []
    assert job.kept_fields == ["source_identifier"]


def test_explicitly_requested_missing_field_still_warns() -> None:
    job = make_job(fields=["source_identifier"])
    config = {
        "default": {
            **builder.DEFAULT_CONFIG["default"],
            "keep": ["required_identifier"],
        },
        "layers": {},
    }

    builder.select_fields_for_layer(
        job,
        config,
        logging.getLogger("test_build_pmtiles"),
    )

    assert job.warnings == [
        "Explicitly requested field 'required_identifier' is missing from "
        "example.gpkg:example."
    ]


def test_fgb_command_linearizes_curved_geometry_before_promoting_to_multi() -> None:
    command = builder.build_fgb_command(
        make_job(),
        ogr2ogr="/usr/bin/ogr2ogr",
    )

    geometry_options = [
        command[index + 1]
        for index, argument in enumerate(command)
        if argument == "-nlt"
    ]
    assert geometry_options == ["CONVERT_TO_LINEAR", "PROMOTE_TO_MULTI"]


def test_fgb_command_can_target_temporary_file_for_atomic_overwrite() -> None:
    command = builder.build_fgb_command(
        make_job(),
        ogr2ogr="/usr/bin/ogr2ogr",
        output_path=Path(".example.tmp.fgb"),
    )

    assert command[3] == ".example.tmp.fgb"
    assert "-overwrite" not in command


def test_fgb_command_filters_null_and_empty_geometry() -> None:
    command = builder.build_fgb_command(
        make_job(geometry_column='shape"column'),
        ogr2ogr="/usr/bin/ogr2ogr",
    )

    where_index = command.index("-where")
    assert command[where_index + 1] == (
        '"shape""column" IS NOT NULL '
        'AND NOT ST_IsEmpty("shape""column")'
    )


def test_layer_extent_reads_ogrinfo_geometry_field_extent() -> None:
    layer = {
        "geometryFields": [
            {
                "name": "",
                "type": "MultiPolygon",
                "extent": [-76.72, 39.19, -76.52, 39.38],
            }
        ]
    }

    assert builder.layer_extent(layer) == [-76.72, 39.19, -76.52, 39.38]


def test_geopackage_sqlite_fallback_reads_layer_metadata(tmp_path: Path) -> None:
    path = tmp_path / "fallback.gpkg"
    write_minimal_geopackage(path)

    layers = builder.geopackage_layers_from_sqlite(path)

    assert layers == [
        {
            "name": "example",
            "featureCount": 1,
            "geometryType": "POINT",
            "geometryFields": [
                {
                    "name": "geom",
                    "type": "POINT",
                    "extent": [-83.1, 39.8, -82.8, 40.1],
                    "coordinateSystem": {
                        "authority": "EPSG",
                        "code": 4326,
                        "wkt": 'GEOGCRS["WGS 84"]',
                    },
                }
            ],
            "fields": [{"name": "label", "type": "TEXT"}],
        }
    ]


def test_inspection_falls_back_when_ogrinfo_is_terminated(
    tmp_path: Path,
    monkeypatch,
) -> None:
    path = tmp_path / "fallback.gpkg"
    write_minimal_geopackage(path)
    failed_result = builder.CommandResult(
        command=["ogrinfo"],
        command_string="ogrinfo",
        start_time="",
        end_time="",
        elapsed_seconds=0,
        returncode=-11,
        stdout="",
        stderr="",
    )
    monkeypatch.setattr(builder, "run_command", lambda *args, **kwargs: failed_result)

    layers, warnings = builder.inspect_geopackage(
        path,
        ogrinfo="/usr/bin/ogrinfo",
        timeout=None,
        logger=logging.getLogger("test_build_pmtiles"),
    )

    assert [layer["name"] for layer in layers] == ["example"]
    assert "terminated by signal 11" in warnings[0]
    assert "Used read-only GeoPackage metadata fallback" in warnings[1]


def test_empty_geometry_count_parses_ogrinfo_result(monkeypatch) -> None:
    result = builder.CommandResult(
        command=["ogrinfo"],
        command_string="ogrinfo",
        start_time="",
        end_time="",
        elapsed_seconds=0,
        returncode=0,
        stdout=(
            "Layer name: SELECT\n"
            "OGRFeature(SELECT):0\n"
            "  empty_geometry_count (Integer) = 15\n"
        ),
        stderr="",
    )
    monkeypatch.setattr(builder, "run_command", lambda *args, **kwargs: result)

    count, warning = builder.empty_geometry_count(
        Path("example.gpkg"),
        "example",
        "geom",
        ogrinfo="/usr/bin/ogrinfo",
        timeout=None,
    )

    assert count == 15
    assert warning == ""


def test_resource_layout_discovers_named_resource_directories_and_writes_siblings(
    tmp_path: Path,
    monkeypatch,
) -> None:
    resource_dir = tmp_path / "stp_majorStreets_2026"
    resource_dir.mkdir()
    gpkg_path = resource_dir / "stp_majorStreets_2026.gpkg"
    gpkg_path.write_bytes(b"placeholder")
    legacy_dir = tmp_path / "gpkg"
    legacy_dir.mkdir()
    (legacy_dir / "legacy.gpkg").write_bytes(b"legacy")
    monkeypatch.setattr(
        builder,
        "inspect_geopackage",
        lambda *args, **kwargs: (
            [
                {
                    "name": "stp_majorStreets_2026",
                    "featureCount": 1,
                    "geometryType": "Polygon",
                    "geometryFields": [{"name": "geom", "type": "Polygon"}],
                    "fields": [],
                }
            ],
            [],
        ),
    )
    monkeypatch.setattr(
        builder,
        "empty_geometry_count",
        lambda *args, **kwargs: (0, ""),
    )

    jobs = builder.discover_jobs(
        tmp_path,
        tmp_path,
        tmp_path,
        builder.DEFAULT_CONFIG,
        ogrinfo="/usr/bin/ogrinfo",
        timeout=None,
        logger=logging.getLogger("test_build_pmtiles"),
        resource_layout=True,
    )

    assert len(jobs) == 1
    assert jobs[0].source_path == gpkg_path
    assert jobs[0].fgb_path == resource_dir / "stp_majorStreets_2026.fgb"
    assert jobs[0].pmtiles_path == resource_dir / "stp_majorStreets_2026.pmtiles"
