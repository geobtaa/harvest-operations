from __future__ import annotations

import csv
from dataclasses import replace
import json
import shutil
import sqlite3
from pathlib import Path
import sys
import zipfile

import pytest


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "curation" / "src"))
sys.path.insert(0, str(ROOT))

from curation.mncommons_curation_pipeline import (  # noqa: E402
    append_fgdc_metadata,
    compare_vector_datasets,
    confirm_review,
    discover_resource_package,
    load_job_config,
    require_confirmed_review,
    run_convert_stage,
    run_dictionary_stage,
    run_inventory_stage,
    run_metadata_stage,
    run_package_stage,
    run_preserve_stage,
    run_thumbnail_stage,
)


FGDC_XML = """<?xml version="1.0"?>
<metadata>
  <idinfo>
    <citation><citeinfo>
      <origin>Example Agency</origin><pubdate>20170104</pubdate>
      <title>Listed Example Waters</title>
      <pubinfo><publish>Example Agency</publish></pubinfo>
    </citeinfo></citation>
    <descript><abstract>Example description.</abstract></descript>
    <timeperd><timeinfo><sngdate><caldate>20240919</caldate></sngdate></timeinfo></timeperd>
    <spdom><bounding><westbc>-97</westbc><eastbc>-89</eastbc>
      <northbc>49</northbc><southbc>43</southbc></bounding></spdom>
    <accconst>None</accconst><useconst>Public domain</useconst>
  </idinfo>
  <metainfo><metd>20240920</metd></metainfo>
</metadata>
"""


def inspection(*, count: int = 12, nullable: bool = False) -> dict:
    return {
        "layers": [
            {
                "name": "example_layer",
                "featureCount": count,
                "geometryFields": [
                    {
                        "name": "Shape",
                        "type": "MultiPolygon",
                        "extent": [1.0, 2.0, 3.0, 4.0],
                        "coordinateSystem": {
                            "projjson": {
                                "name": "NAD83 / UTM zone 15N",
                                "id": {"authority": "EPSG", "code": 26915},
                            }
                        },
                    }
                ],
                "fields": [{"name": "NAME", "type": "String", "nullable": nullable}],
            }
        ]
    }


def make_job(tmp_path: Path, *, include_gpkg: bool = True):
    source = tmp_path / "pilot"
    resource = source / "example-series" / "2026-example"
    metadata = resource / "metadata"
    fgdb = resource / "fgdb"
    gdb = fgdb / "example.gdb"
    metadata.mkdir(parents=True)
    gdb.mkdir(parents=True)
    (gdb / "table.gdbtable").write_bytes(b"source-data")
    (resource / "dataResource.xml").write_text(
        "<dataResource><resourceGUID>{11111111-2222-3333-4444-555555555555}</resourceGUID>"
        "<baseName>example</baseName><currentAsofDate>20240919</currentAsofDate>"
        "</dataResource>",
        encoding="utf-8",
    )
    (metadata / "metadata.xml").write_text(FGDC_XML, encoding="utf-8")
    (metadata / "metadata.html").write_text("<p>Metadata</p>", encoding="utf-8")
    (metadata / "preview.jpg").write_bytes(b"jpeg")
    (fgdb / "Example.lyr").write_bytes(b"layer")
    if include_gpkg:
        gpkg_dir = fgdb / "gpkg"
        gpkg_dir.mkdir()
        connection = sqlite3.connect(gpkg_dir / "example.gpkg")
        connection.execute("CREATE TABLE placeholder (id INTEGER)")
        connection.commit()
        connection.close()

    config = tmp_path / "job.yaml"
    config.write_text(
        f"""version: 1
job:
  id: test-mncommons
  source_directory: {source}
  work_directory: {tmp_path / "work"}
collection:
  id: b1g_test
  title: Test MnCommons Pilot
  description: Test archival collection.
  creator: University of Minnesota Libraries
  spatial_coverage: Minnesota
  temporal_coverage: 2026
  date_issued: 2026-09-12
  rights: Rights vary by item.
metadata:
  provider: University of Minnesota
  publisher: Minnesota Geospatial Commons
  code: "05"
  series_label_prefix: "Minnesota Geospatial Commons series:"
  accession_date: 2026-09-12
  access_rights: Public
  publication_state: draft
  display_note: Archived copy.
manual_review:
  required_fields: [filename, ID, Title, Description, Creator, Publisher,
    Provider, Resource Class, Resource Type, Temporal Coverage, Date Issued,
    Spatial Coverage, Bounding Box, Rights, Access Rights]
series:
  - id: example-series
    title: Example series
    resource_guid: 11111111-2222-3333-4444-555555555555
    spatial_coverage: Minnesota
    versions:
      - snapshot_year: 2026
        snapshot_date: 2026-01
        temporal_coverage: 2024-09-19
        path: example-series/2026-example
        metadata_overrides:
          Theme: Example theme
""",
        encoding="utf-8",
    )
    return load_job_config(config)


def test_metadata_stage_inventories_supplied_gpkg_and_reuses_ids(
    tmp_path: Path,
) -> None:
    job = make_job(tmp_path)

    first_path = run_metadata_stage(job, inspector=lambda _: inspection())
    first_manifest = json.loads(job.manifest_path.read_text(encoding="utf-8"))
    first_id = first_manifest["records"][0]["curated_id"]
    second_path = run_metadata_stage(job, inspector=lambda _: inspection())
    second_manifest = json.loads(job.manifest_path.read_text(encoding="utf-8"))

    assert first_path == second_path == job.metadata_path
    assert second_manifest["records"][0]["curated_id"] == first_id
    assert second_manifest["records"][0]["geopackage_status"] == (
        "supplied_derivative_validated"
    )
    assert second_manifest["records"][0]["geopackage_embedded_metadata"] is False

    with job.metadata_path.open(encoding="utf-8", newline="") as handle:
        row = next(csv.DictReader(handle))
    assert row["Title"] == "Listed Example Waters [Minnesota] {2024-09-19}"
    assert row["Alternative Title"] == "Listed Example Waters"
    assert row["Temporal Coverage"] == "2024-09-19"
    assert row["snapshot_date"] == "2026-01"
    assert row["proposed_temporal_source"] == "job_config"
    assert row["Theme"] == "Example theme"
    assert row["Date Issued"] == "2017-01-04"
    assert row["Resource Type"] == "Polygon data"
    assert row["Publisher"] == "Minnesota Geospatial Commons"
    assert row["Local Collection"] == (
        "Minnesota Geospatial Commons series: Example series"
    )
    assert row["Member Of"] == "b1g_test"
    assert row["Is Version Of"] == ""
    assert row["Coordinate Reference System"].endswith("/epsg/26915/")
    assert (
        "snapshot_year_2026_differs_from_content_year_2024"
        in row["metadata_review_flags"]
    )
    assert (
        "publication_date_differs_from_temporal_coverage"
        in row["metadata_review_flags"]
    )

    with job.collection_metadata_path.open(encoding="utf-8", newline="") as handle:
        collection_row = next(csv.DictReader(handle))
    assert collection_row["ID"] == "b1g_test"
    assert collection_row["Title"] == "Test MnCommons Pilot"
    assert collection_row["Resource Class"] == "Collections"
    assert collection_row["Publisher"] == "Minnesota Geospatial Commons"
    assert collection_row["Member Of"] == ""


def test_configured_temporal_range_populates_full_date_range(tmp_path: Path) -> None:
    job = make_job(tmp_path)
    version = replace(job.versions[0], temporal_coverage="2022-2024")
    series = replace(job.series[0], versions=(version,))
    job = replace(job, series=(series,))

    run_metadata_stage(job, inspector=lambda _: inspection())

    with job.metadata_path.open(encoding="utf-8", newline="") as handle:
        row = next(csv.DictReader(handle))
    assert row["Temporal Coverage"] == "2022-2024"
    assert row["Index Year"] == "2022"
    assert row["Date Range"] == "2022-2024"


def test_inventory_records_fixity_and_layer_file(tmp_path: Path) -> None:
    job = make_job(tmp_path)

    output = run_inventory_stage(job, inspector=lambda _: inspection())

    with output.open(encoding="utf-8", newline="") as handle:
        row = next(csv.DictReader(handle))
    assert len(row["geodatabase_sha256"]) == 64
    assert len(row["geopackage_sha256"]) == 64
    assert row["layer_file_count"] == "1"
    assert row["geopackage_status"] == "supplied_derivative_validated"


def test_missing_geopackage_is_planned_for_generation(tmp_path: Path) -> None:
    job = make_job(tmp_path, include_gpkg=False)
    package = discover_resource_package(job.versions[0])
    assert package.geopackage is None

    run_metadata_stage(job, inspector=lambda _: inspection())
    manifest = json.loads(job.manifest_path.read_text(encoding="utf-8"))
    assert manifest["records"][0]["geopackage_status"] == "not_supplied"


def test_legacy_preview_is_optional(tmp_path: Path) -> None:
    job = make_job(tmp_path)
    (job.versions[0].source_path / "metadata" / "preview.jpg").unlink()

    package = discover_resource_package(job.versions[0])

    assert package.preview is None


def test_review_checksum_detects_later_edits(tmp_path: Path) -> None:
    job = make_job(tmp_path)
    run_metadata_stage(job, inspector=lambda _: inspection())

    confirm_review(job)
    require_confirmed_review(job)
    job.metadata_path.write_text(
        job.metadata_path.read_text(encoding="utf-8") + "\n",
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="changed after review"):
        require_confirmed_review(job)


def test_review_checksum_covers_collection_metadata(tmp_path: Path) -> None:
    job = make_job(tmp_path)
    run_metadata_stage(job, inspector=lambda _: inspection())

    confirm_review(job)
    job.collection_metadata_path.write_text(
        job.collection_metadata_path.read_text(encoding="utf-8") + "\n",
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="Collection metadata changed"):
        require_confirmed_review(job)


def test_dataset_comparison_distinguishes_errors_from_nullable_warning() -> None:
    warnings, errors = compare_vector_datasets(
        inspection(nullable=False),
        inspection(nullable=True),
    )
    assert warnings == ["example_layer:NAME:nullability_changed"]
    assert errors == []

    _, errors = compare_vector_datasets(inspection(count=12), inspection(count=13))
    assert errors == ["example_layer:feature_count_mismatch:12!=13"]


def test_reviewed_local_stages_copy_sources_and_build_dictionary(
    tmp_path: Path,
) -> None:
    job = make_job(tmp_path)
    fake_inspector = lambda _: inspection()  # noqa: E731
    run_metadata_stage(job, inspector=fake_inspector)
    confirm_review(job)

    def fake_converter(source: Path, destination: Path) -> None:
        supplied = source.parent / "gpkg" / "example.gpkg"
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(supplied, destination)

    def fake_thumbnail_renderer(source: Path, destination: Path) -> None:
        assert source == job.output_geopackage(job.versions[0])
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"png")

    run_preserve_stage(job)
    run_convert_stage(job, inspector=fake_inspector, converter=fake_converter)
    run_dictionary_stage(job, inspector=fake_inspector)
    run_thumbnail_stage(job, renderer=fake_thumbnail_renderer)

    item_dir = job.item_dir(job.versions[0])
    assert (item_dir / "original" / "fgdb" / "Example.lyr").is_file()
    assert not (item_dir / "original" / "fgdb" / "gpkg" / "example.gpkg").exists()
    assert (item_dir / "original" / "example.gdb.zip").is_file()
    assert job.output_geopackage(job.versions[0]).is_file()
    assert (item_dir / "data-dictionaries" / "example_layer.csv").is_file()
    assert not (item_dir / "preview.jpg").exists()
    assert job.thumbnail_path(job.versions[0]).read_bytes() == b"png"
    (item_dir / f"{job.versions[0].output_stem}.pmtiles").write_bytes(b"pmtiles")

    manifest_path = run_package_stage(job)
    assert manifest_path == job.upload_manifest_path
    access_zip = (
        job.uploads_dir
        / job.versions[0].output_stem
        / f"{job.versions[0].filename}.zip"
    )
    original_zip = (
        job.uploads_dir
        / job.versions[0].output_stem
        / f"{job.versions[0].output_stem}_original.zip"
    )
    with zipfile.ZipFile(access_zip) as archive:
        assert set(archive.namelist()) == {
            job.versions[0].filename,
            "SHA256SUMS.txt",
        }
    with zipfile.ZipFile(original_zip) as archive:
        assert not any(name.casefold().endswith(".gpkg") for name in archive.namelist())
        assert "original/example.gdb.zip" in archive.namelist()
        assert "SHA256SUMS.txt" in archive.namelist()
    upload_dir = job.uploads_dir / job.versions[0].output_stem
    assert (upload_dir / f"{job.versions[0].output_stem}.pmtiles").is_file()
    assert (upload_dir / f"{job.versions[0].output_stem}.png").is_file()
    upload_dictionary = upload_dir / "example_layer.csv"
    assert upload_dictionary.is_file()
    assert not (upload_dir / "data-dictionaries").exists()
    with upload_dictionary.open(encoding="utf-8", newline="") as handle:
        dictionary_reader = csv.DictReader(handle)
        assert "match_status" not in (dictionary_reader.fieldnames or [])
        assert "layer_name" not in (dictionary_reader.fieldnames or [])
    with job.upload_manifest_path.open(encoding="utf-8", newline="") as handle:
        roles = {row["role"] for row in csv.DictReader(handle)}
    assert roles == {
        "access_geopackage",
        "preservation_original",
        "pmtiles",
        "thumbnail",
        "data_dictionary",
    }


def test_append_fgdc_metadata_retains_existing_metadata_record(tmp_path: Path) -> None:
    gpkg = tmp_path / "metadata.gpkg"
    with sqlite3.connect(gpkg) as connection:
        connection.executescript(
            """
            CREATE TABLE gpkg_contents (table_name TEXT, data_type TEXT);
            INSERT INTO gpkg_contents VALUES ('features', 'features');
            CREATE TABLE gpkg_metadata (
                id INTEGER PRIMARY KEY,
                md_scope TEXT NOT NULL,
                md_standard_uri TEXT NOT NULL,
                mime_type TEXT NOT NULL,
                metadata TEXT NOT NULL
            );
            CREATE TABLE gpkg_metadata_reference (
                reference_scope TEXT NOT NULL,
                table_name TEXT,
                column_name TEXT,
                row_id_value INTEGER,
                timestamp TEXT NOT NULL,
                md_file_id INTEGER NOT NULL,
                md_parent_id INTEGER
            );
            INSERT INTO gpkg_metadata
                (md_scope, md_standard_uri, mime_type, metadata)
                VALUES ('dataset', 'http://qgis.org', 'text/xml', '<qgis/>');
            """
        )

    append_fgdc_metadata(gpkg, "<metadata><idinfo/></metadata>")

    with sqlite3.connect(gpkg) as connection:
        rows = connection.execute(
            "SELECT md_standard_uri, metadata FROM gpkg_metadata ORDER BY id"
        ).fetchall()
    assert rows == [
        ("http://qgis.org", "<qgis/>"),
        (
            "https://www.fgdc.gov/metadata/csdgm/",
            "<metadata><idinfo/></metadata>",
        ),
    ]
