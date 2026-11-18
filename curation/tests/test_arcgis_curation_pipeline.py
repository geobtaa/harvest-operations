from __future__ import annotations

import json
import re
import shutil
import sys
from pathlib import Path
from zipfile import ZipFile

import fiona
import pandas as pd
import pytest
import yaml


CURATION_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = CURATION_ROOT.parent
sys.path.insert(0, str(CURATION_ROOT / "src"))

import curation.arcgis_curation_pipeline as pipeline  # noqa: E402

from curation.arcgis_curation_pipeline import (  # noqa: E402
    CurationConfigError,
    confirm_manual_review,
    load_job_config,
    mark_stage,
    mark_validation_stage,
    require_confirmed_review,
    run_dictionary_stage,
    run_download_stage,
    run_derivatives_stage,
    run_enrich_stage,
    run_metadata_stage,
    run_thumbnail_stage,
    run_zip_stage,
    save_run_record,
)


SOURCE_ID = "1ec9eb84c4fd41548946b1484d4f31ef_6"
SERVICE_URL = (
    "https://services1.arcgis.com/9meaaHE3uiba0zr8/arcgis/rest/services/"
    "PrincipalZoning_TEST/FeatureServer/6"
)
DIRECT_ITEM_ID = "86f5c57fc5894dd7ad821578b772f1d9"
DIRECT_SOURCE_ID = f"{DIRECT_ITEM_ID}_0"
DIRECT_SERVICE_URL = (
    "https://services1.arcgis.com/9meaaHE3uiba0zr8/ArcGIS/rest/services/"
    "AddressPoints_Historic/FeatureServer/0"
)
DIRECT_SERVICE_ROOT = DIRECT_SERVICE_URL.rsplit("/", 1)[0]
DIRECT_ITEM_API = f"https://www.arcgis.com/sharing/rest/content/items/{DIRECT_ITEM_ID}"


def catalog_fixture() -> dict:
    return {
        "dataset": [
            {
                "identifier": (
                    "https://www.arcgis.com/home/item.html?"
                    "id=1ec9eb84c4fd41548946b1484d4f31ef&sublayer=6"
                ),
                "landingPage": "https://information.stpaul.gov/datasets/stpaul::principal-zoning-1",
                "title": "Principal Zoning 2026",
                "description": "Polygon zoning districts for Saint Paul.",
                "publisher": {"name": "Saint Paul GIS"},
                "keyword": ["zoning", "polygon"],
                "issued": "2026-01-02T00:00:00Z",
                "modified": "2026-04-07T20:50:37Z",
                "license": "Public Domain",
                "spatial": "-93.2080,44.8875,-93.0037,44.9920",
                "distribution": [
                    {
                        "title": "ArcGIS GeoService",
                        "accessURL": SERVICE_URL,
                    },
                    {
                        "title": "Shapefile",
                        "accessURL": "https://information.stpaul.gov/zoning.zip",
                    },
                ],
            }
        ]
    }


def write_config(tmp_path: Path, *, records: list[dict] | None = None) -> Path:
    config = {
        "version": 1,
        "job": {"id": "test-job", "work_directory": str(tmp_path / "work")},
        "hub": {
            "name": "Open Information Saint Paul",
            "landing_page": "https://information.stpaul.gov/",
            "dcat_api": "https://information.stpaul.gov/api/feed/dcat-us/1.1.json",
            "website_reference_id": "05c-02",
            "websites_csv": str(REPO_ROOT / "reference_data" / "websites.csv"),
        },
        "coordinate_reference_system": {
            "authority": "ESRI:103768",
            "uri": "https://spatialreference.org/ref/esri/103768",
        },
        "metadata": {
            "code": "27_58000",
            "member_of": "b1g_urbanBaseLayers",
            "export_date": "2026-07-16",
        },
        "selection_criteria": {},
        "manual_review": {
            "required_fields": [
                "filename",
                "ID",
                "Title",
                "Provider",
                "Resource Class",
                "Rights",
            ]
        },
        "records": records
        or [
            {
                "id": SOURCE_ID,
                "filename": "stp_zoning_2026",
                "basic_theme": "Principal Zoning",
            }
        ],
    }
    path = tmp_path / "job.yaml"
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return path


def write_test_geopackage(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with fiona.open(
        path,
        "w",
        driver="GPKG",
        layer=path.stem,
        crs="EPSG:4326",
        schema={
            "geometry": "Polygon",
            "properties": {"NAME": "str:80", "COUNT": "int"},
        },
    ) as collection:
        collection.write(
            {
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            (-93.20804, 44.88746),
                            (-93.00366, 44.88746),
                            (-93.00366, 44.99204),
                            (-93.20804, 44.99204),
                            (-93.20804, 44.88746),
                        ]
                    ],
                },
                "properties": {"NAME": "Example", "COUNT": 1},
            }
        )


def test_inspect_geopackage_infers_unknown_declared_geometry(tmp_path: Path) -> None:
    path = tmp_path / "unknown-geometry.gpkg"
    with fiona.open(
        path,
        "w",
        driver="GPKG",
        layer=path.stem,
        crs="EPSG:4326",
        schema={"geometry": "Unknown", "properties": {"NAME": "str"}},
    ) as collection:
        collection.write(
            {
                "geometry": {
                    "type": "MultiPolygon",
                    "coordinates": [
                        [
                            [
                                (-122.4, 47.5),
                                (-122.3, 47.5),
                                (-122.3, 47.6),
                                (-122.4, 47.6),
                                (-122.4, 47.5),
                            ]
                        ]
                    ],
                },
                "properties": {"NAME": "Park"},
            }
        )
        collection.write({"geometry": None, "properties": {"NAME": "No geometry"}})

    inspection = pipeline.inspect_geopackage(path)

    assert inspection["geometry_type"] == "MultiPolygon"
    assert inspection["resource_type"] == "Polygon data"
    assert inspection["feature_count"] == 2


def test_config_accepts_a_single_selected_record(tmp_path: Path) -> None:
    job = load_job_config(write_config(tmp_path))

    assert job.records[0].filename == "stp_zoning_2026.gpkg"
    assert len(job.records) == 1


def test_job_groups_artifact_paths_by_resource_filename(tmp_path: Path) -> None:
    job = load_job_config(write_config(tmp_path))
    resource_dir = tmp_path / "work" / "stp_zoning_2026"

    assert job.resource_dir("stp_zoning_2026.gpkg") == resource_dir
    assert job.gpkg_path("stp_zoning_2026.gpkg") == (
        resource_dir / "stp_zoning_2026.gpkg"
    )
    assert job.dictionary_path("stp_zoning_2026.gpkg") == (
        resource_dir / "stp_zoning_2026.csv"
    )
    assert job.thumbnail_path("stp_zoning_2026.gpkg") == (
        resource_dir / "stp_zoning_2026.png"
    )


def test_config_accepts_direct_rest_source_and_metadata_overrides(
    tmp_path: Path,
) -> None:
    job = load_job_config(
        write_config(
            tmp_path,
            records=[
                {
                    "id": DIRECT_SOURCE_ID,
                    "filename": "stp_addressPoints_2025",
                    "basic_theme": "Address Points",
                    "temporal_year": "2025",
                    "source": {
                        "type": "arcgis_rest",
                        "service_url": DIRECT_SERVICE_URL + "/",
                    },
                    "metadata_overrides": {
                        "creator": "City of Saint Paul",
                        "rights": "Public Domain",
                    },
                }
            ],
        )
    )

    record = job.records[0]
    assert record.source_type == "arcgis_rest"
    assert record.service_url == DIRECT_SERVICE_URL
    assert record.temporal_year == "2025"
    assert record.metadata_overrides == {
        "creator": "City of Saint Paul",
        "rights": "Public Domain",
    }


def test_config_rejects_direct_rest_service_root_without_layer_id(
    tmp_path: Path,
) -> None:
    path = write_config(
        tmp_path,
        records=[
            {
                "id": DIRECT_SOURCE_ID,
                "filename": "stp_addressPoints_2025",
                "source": {
                    "type": "arcgis_rest",
                    "service_url": DIRECT_SERVICE_ROOT,
                },
            }
        ],
    )

    with pytest.raises(CurationConfigError, match="layer URL ending"):
        load_job_config(path)


def test_copyable_job_template_is_valid_yaml() -> None:
    template_path = CURATION_ROOT / "jobs" / "arcgis_curation_pipeline_template.yaml"
    template = yaml.safe_load(template_path.read_text(encoding="utf-8"))

    assert template["version"] == 1
    assert template["provider"] == "BTAA-GIN"
    assert template["metadata"]["member_of"] == "b1g_urbanBaseLayers"
    assert set(template["selection_criteria"]) == {"allowed_resource_types"}
    assert [record["basic_theme"] for record in template["records"]] == [
        "Address points",
        "Building footprints",
        "Municipal boundary",
        "Parks",
        "Streets",
        "Trees",
        "Zoning",
    ]


def test_config_rejects_duplicate_output_filenames(tmp_path: Path) -> None:
    path = write_config(
        tmp_path,
        records=[
            {"id": SOURCE_ID, "filename": "same"},
            {"id": "f03d32dc6dc8458ea3f0be92fab49318_0", "filename": "same.gpkg"},
        ],
    )

    with pytest.raises(CurationConfigError, match="Duplicate output filename"):
        load_job_config(path)


def test_config_rejects_unquoted_numeric_code(tmp_path: Path) -> None:
    path = write_config(tmp_path)
    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    config["metadata"]["code"] = 2_758_000
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    with pytest.raises(CurationConfigError, match="must be a quoted"):
        load_job_config(path)


def test_metadata_stage_reuses_arcgis_rules_and_applies_archive_exceptions(
    tmp_path: Path,
) -> None:
    job = load_job_config(write_config(tmp_path))

    output_path = run_metadata_stage(job, catalog=catalog_fixture())

    dataframe = pd.read_csv(output_path, dtype=str, keep_default_na=False)
    row = dataframe.iloc[0]
    assert row["filename"] == "stp_zoning_2026.gpkg"
    assert re.fullmatch(r"b1g_[A-Za-z0-9]{12}", row["ID"])
    assert row["Code"] == "27_58000"
    assert row["Member Of"] == "b1g_urbanBaseLayers"
    assert row["Is Part Of"] == ""
    assert row["Title"] == "Principal Zoning [Minnesota--Saint Paul] {2026}"
    assert row["Description"].startswith(
        "Historical dataset of Principal Zoning in Saint Paul, Minnesota as of 2026. "
    )
    assert row["Resource Class"] == "Datasets"
    assert row["Resource Type"] == "Polygon data"
    assert row["Provider"] == "BTAA-GIN"
    assert row["Publication State"] == "draft"
    assert (
        row["Coordinate Reference System"]
        == "https://spatialreference.org/ref/esri/103768"
    )
    assert row["Provenance"] == (
        f"Exported from {SERVICE_URL} as GeoPackage on July 16, 2026."
    )
    assert row["Source"] == ""
    assert row["Harvest Workflow"] == "curation_datasets"
    assert row["Display Note"] == (
        "Warning: This dataset is an archived copy held by the BTAA-GIN. "
        "For the most current layer, consult Open Information Saint Paul at "
        "https://information.stpaul.gov/"
    )

    manifest = json.loads(job.manifest_path.read_text(encoding="utf-8"))
    assert manifest["manual_review"]["status"] == "pending"
    assert manifest["stages"]["validate"]["status"] == "completed"
    assert manifest["stages"]["metadata"]["status"] == "completed"
    assert manifest["records"][0]["curated_id"] == row["ID"]
    assert manifest["records"][0]["service_url"] == SERVICE_URL


def test_metadata_stage_supports_mixed_dcat_and_direct_rest_records(
    tmp_path: Path,
) -> None:
    path = write_config(
        tmp_path,
        records=[
            {
                "id": SOURCE_ID,
                "filename": "stp_zoning_2026",
                "basic_theme": "Principal Zoning",
            },
            {
                "id": DIRECT_SOURCE_ID,
                "filename": "stp_addressPoints_2025",
                "basic_theme": "Address Points",
                "temporal_year": "2025",
                "source": {
                    "type": "arcgis_rest",
                    "service_url": DIRECT_SERVICE_URL,
                },
                "metadata_overrides": {
                    "description": (
                        "Historic address point locations in the City of Saint Paul."
                    ),
                    "creator": "City of Saint Paul",
                    "rights": "Public Domain",
                },
            },
        ],
    )
    job = load_job_config(path)
    requested_urls: list[str] = []

    def requester(url: str, params: dict | None, method: str) -> dict:
        requested_urls.append(url)
        if url == DIRECT_SERVICE_URL:
            return {
                "name": "StampAddress",
                "geometryType": "esriGeometryPoint",
                "capabilities": "Query",
                "serviceItemId": DIRECT_ITEM_ID,
                "editingInfo": {"dataLastEditDate": 1753104361052},
            }
        if url == DIRECT_SERVICE_ROOT:
            return {
                "serviceItemId": DIRECT_ITEM_ID,
                "layers": [{"id": 0, "name": "StampAddress"}],
            }
        if url == DIRECT_ITEM_API:
            return {
                "id": DIRECT_ITEM_ID,
                "owner": "CityofSaintPaul",
                "created": 1652808986000,
                "modified": 1652809292000,
                "title": "Address Points (Historic)",
                "snippet": (
                    "This layer depicts historic address point locations "
                    "in the City of Saint Paul."
                ),
                "tags": ["address", "historic"],
                "extent": [
                    [-93.2149999997, 44.8889999998],
                    [-92.9959999997, 44.9969999998],
                ],
            }
        raise AssertionError(f"Unexpected request: {method} {url} {params}")

    output_path = run_metadata_stage(
        job,
        requester=requester,
        catalog=catalog_fixture(),
    )

    dataframe = pd.read_csv(output_path, dtype=str, keep_default_na=False)
    assert list(dataframe["filename"]) == [
        "stp_zoning_2026.gpkg",
        "stp_addressPoints_2025.gpkg",
    ]
    address_row = dataframe.iloc[1]
    assert address_row["Title"] == "Address Points [Minnesota--Saint Paul] {2025}"
    assert address_row["Description"] == (
        "Historical dataset of Address Points in Saint Paul, Minnesota as of 2025. "
        "Historic address point locations in the City of Saint Paul."
    )
    assert address_row["Creator"] == "City of Saint Paul"
    assert address_row["Rights"] == "Public Domain"
    assert address_row["Temporal Coverage"] == "2025"
    assert address_row["Date Range"] == "2025-2025"
    assert address_row["Identifier"] == (
        f"https://hub.arcgis.com/datasets/{DIRECT_SOURCE_ID}"
    )
    assert address_row["Website Platform"] == "ArcGIS REST"
    assert address_row["Endpoint Description"] == "ArcGIS REST layer"
    assert address_row["Endpoint URL"] == DIRECT_SERVICE_URL
    assert job.dcat_api not in requested_urls

    manifest = json.loads(job.manifest_path.read_text(encoding="utf-8"))
    direct_manifest = manifest["records"][1]
    assert direct_manifest["source_id"] == DIRECT_SOURCE_ID
    assert direct_manifest["service_url"] == DIRECT_SERVICE_URL
    assert direct_manifest["source_type"] == "arcgis_rest"
    assert direct_manifest["metadata_source"] == "arcgis_rest_item"
    assert direct_manifest["item_id"] == DIRECT_ITEM_ID
    assert direct_manifest["item_url"] == (
        f"https://www.arcgis.com/home/item.html?id={DIRECT_ITEM_ID}"
    )


def test_validation_stage_is_recorded_before_metadata(tmp_path: Path) -> None:
    job = load_job_config(write_config(tmp_path))

    mark_validation_stage(job)

    manifest = json.loads(job.manifest_path.read_text(encoding="utf-8"))
    assert manifest["stages"]["validate"]["status"] == "completed"
    assert "metadata" not in manifest["stages"]
    assert manifest["records"] == []


def test_metadata_stage_preserves_nano_ids_when_regenerated(tmp_path: Path) -> None:
    job = load_job_config(write_config(tmp_path))
    run_metadata_stage(job, catalog=catalog_fixture())
    first_id = pd.read_csv(job.metadata_path, dtype=str).iloc[0]["ID"]

    run_metadata_stage(job, catalog=catalog_fixture())
    second_id = pd.read_csv(job.metadata_path, dtype=str).iloc[0]["ID"]

    assert second_id == first_id


def test_metadata_stage_recovers_id_from_saved_run_record(
    tmp_path: Path,
    monkeypatch,
) -> None:
    run_records_root = tmp_path / "run_records"
    saved_manifest_path = (
        run_records_root / "test-job" / "20260101T000000Z" / "manifest.json"
    )
    saved_manifest_path.parent.mkdir(parents=True)
    saved_manifest_path.write_text(
        json.dumps(
            {
                "job_id": "test-job",
                "records": [
                    {
                        "source_id": SOURCE_ID,
                        "curated_id": "b1g_ABCDEFGHIJKL",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(pipeline, "RUN_RECORDS_ROOT", run_records_root)
    job = load_job_config(write_config(tmp_path))

    run_metadata_stage(job, catalog=catalog_fixture())

    assert pd.read_csv(job.metadata_path, dtype=str).iloc[0]["ID"] == "b1g_ABCDEFGHIJKL"


def test_metadata_stage_preserves_existing_ids_when_records_are_added(
    tmp_path: Path,
) -> None:
    first_job = load_job_config(write_config(tmp_path))
    run_metadata_stage(first_job, catalog=catalog_fixture())
    first_id = pd.read_csv(first_job.metadata_path, dtype=str).iloc[0]["ID"]

    second_source_id = "f03d32dc6dc8458ea3f0be92fab49318_0"
    second_service_url = (
        "https://example.org/arcgis/rest/services/Boundary/FeatureServer/0"
    )
    second_resource = json.loads(json.dumps(catalog_fixture()["dataset"][0]))
    second_resource["identifier"] = (
        "https://www.arcgis.com/home/item.html?"
        "id=f03d32dc6dc8458ea3f0be92fab49318&sublayer=0"
    )
    second_resource["landingPage"] = "https://information.stpaul.gov/datasets/boundary"
    second_resource["title"] = "Municipal Boundary 2026"
    second_resource["distribution"][0]["accessURL"] = second_service_url
    second_path = write_config(
        tmp_path,
        records=[
            {
                "id": SOURCE_ID,
                "filename": "stp_zoning_2026",
                "basic_theme": "Principal Zoning",
            },
            {
                "id": second_source_id,
                "filename": "stp_boundary_2026",
                "basic_theme": "Municipal Boundary",
            },
        ],
    )
    second_job = load_job_config(second_path)
    run_metadata_stage(
        second_job,
        catalog={"dataset": [catalog_fixture()["dataset"][0], second_resource]},
    )

    ids_by_filename = pd.read_csv(second_job.metadata_path, dtype=str).set_index(
        "filename"
    )["ID"]
    assert ids_by_filename["stp_zoning_2026.gpkg"] == first_id
    assert re.fullmatch(
        r"b1g_[A-Za-z0-9]{12}", ids_by_filename["stp_boundary_2026.gpkg"]
    )
    assert ids_by_filename["stp_boundary_2026.gpkg"] != first_id


def test_review_checksum_detects_manual_edits_after_confirmation(
    tmp_path: Path,
) -> None:
    job = load_job_config(write_config(tmp_path))
    run_metadata_stage(job, catalog=catalog_fixture())
    confirm_manual_review(job, confirmed=True)

    dataframe = pd.read_csv(job.metadata_path, dtype=str, keep_default_na=False)
    dataframe.loc[0, "Title"] = "Changed after review"
    dataframe.to_csv(job.metadata_path, index=False)

    with pytest.raises(RuntimeError, match="changed after review confirmation"):
        require_confirmed_review(job)


def test_review_gate_detects_yaml_changes_after_metadata(tmp_path: Path) -> None:
    config_path = write_config(tmp_path)
    job = load_job_config(config_path)
    run_metadata_stage(job, catalog=catalog_fixture())
    confirm_manual_review(job, confirmed=True)
    config_path.write_text(config_path.read_text(encoding="utf-8") + "# changed\n")

    with pytest.raises(RuntimeError, match="YAML job changed after metadata"):
        require_confirmed_review(job)


def test_download_skips_existing_geopackages_and_continues(
    tmp_path: Path,
    monkeypatch,
) -> None:
    job = load_job_config(write_config(tmp_path))
    run_metadata_stage(job, catalog=catalog_fixture())
    confirm_manual_review(job, confirmed=True)
    existing_path = job.gpkg_path("stp_zoning_2026.gpkg")
    existing_path.parent.mkdir(parents=True)
    existing_path.write_bytes(b"existing")

    manifest = json.loads(job.manifest_path.read_text(encoding="utf-8"))
    manifest["records"].append(
        {
            "source_id": "new-source_0",
            "curated_id": "b1g_123456789012",
            "filename": "stp_new_2026.gpkg",
            "landing_page": "https://example.org/new",
            "service_url": "https://example.org/FeatureServer/0",
        }
    )
    job.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    downloaded: list[Path] = []

    def fake_download(
        service_url: str,
        output_path: Path,
        layer_name: str,
        output_crs: str,
        **kwargs,
    ) -> dict:
        downloaded.append(output_path)
        return {"feature_count": 10, "output": str(output_path)}

    monkeypatch.setattr(
        "curation.arcgis_curation_pipeline.download_service_geopackage",
        fake_download,
    )

    run_download_stage(job)

    updated_manifest = json.loads(job.manifest_path.read_text(encoding="utf-8"))
    outputs = updated_manifest["stages"]["download"]["outputs"]
    assert outputs[0] == {
        "status": "skipped_existing",
        "output": "stp_zoning_2026/stp_zoning_2026.gpkg",
    }
    assert outputs[1]["status"] == "downloaded"
    assert downloaded == [job.gpkg_path("stp_new_2026.gpkg")]


def test_download_records_failures_and_continues_with_later_datasets(
    tmp_path: Path,
    monkeypatch,
) -> None:
    job = load_job_config(write_config(tmp_path))
    run_metadata_stage(job, catalog=catalog_fixture())
    confirm_manual_review(job, confirmed=True)

    manifest = json.loads(job.manifest_path.read_text(encoding="utf-8"))
    manifest["records"].append(
        {
            "source_id": "available-source_0",
            "curated_id": "b1g_123456789012",
            "filename": "stp_available_2026.gpkg",
            "landing_page": "https://example.org/available",
            "service_url": "https://example.org/FeatureServer/0",
        }
    )
    job.manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    attempted: list[Path] = []

    def fake_download(
        service_url: str,
        output_path: Path,
        layer_name: str,
        output_crs: str,
        **kwargs,
    ) -> dict:
        attempted.append(output_path)
        if output_path.name == "stp_zoning_2026.gpkg":
            raise RuntimeError("ArcGIS REST services directory is disabled")
        return {"feature_count": 10, "output": str(output_path)}

    monkeypatch.setattr(
        "curation.arcgis_curation_pipeline.download_service_geopackage",
        fake_download,
    )

    results = run_download_stage(job)

    assert attempted == [
        job.gpkg_path("stp_zoning_2026.gpkg"),
        job.gpkg_path("stp_available_2026.gpkg"),
    ]
    assert [result["status"] for result in results] == ["failed", "downloaded"]
    updated_manifest = json.loads(job.manifest_path.read_text(encoding="utf-8"))
    download_stage = updated_manifest["stages"]["download"]
    assert download_stage["status"] == "completed_with_errors"
    assert download_stage["failed_count"] == 1
    assert download_stage["outputs"][0] == {
        "status": "failed",
        "source_id": SOURCE_ID,
        "filename": "stp_zoning_2026.gpkg",
        "service_url": SERVICE_URL,
        "output": "stp_zoning_2026/stp_zoning_2026.gpkg",
        "error": "ArcGIS REST services directory is disabled",
    }
    assert download_stage["outputs"][1]["status"] == "downloaded"


def test_postprocess_pauses_after_all_download_attempts_when_one_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    job = load_job_config(write_config(tmp_path))
    run_metadata_stage(job, catalog=catalog_fixture())
    confirm_manual_review(job, confirmed=True)
    later_stages: list[str] = []

    def fail_download(*args, **kwargs) -> dict:
        raise RuntimeError("service directory disabled")

    monkeypatch.setattr(pipeline, "download_service_geopackage", fail_download)
    monkeypatch.setattr(
        pipeline,
        "run_enrich_stage",
        lambda *args, **kwargs: later_stages.append("enrich"),
    )

    with pytest.raises(
        RuntimeError,
        match=(
            "Postprocess paused after attempting every download.*stp_zoning_2026.gpkg"
        ),
    ):
        pipeline.run_postprocess(job)

    assert later_stages == []


def test_derivatives_stage_runs_builder_from_curation_scripts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    job = load_job_config(write_config(tmp_path))
    run_metadata_stage(job, catalog=catalog_fixture())
    confirm_manual_review(job, confirmed=True)
    commands: list[list[str]] = []
    monkeypatch.setattr(
        pipeline,
        "_run_command",
        lambda command: commands.append(command),
    )

    run_derivatives_stage(job)

    assert len(commands) == 1
    command = commands[0]
    script_path = Path(command[1])
    assert script_path == CURATION_ROOT / "scripts" / "build_pmtiles_from_gpkg.py"
    assert script_path.is_file()
    assert command[command.index("--input-dir") + 1] == str(job.work_dir)
    assert command[command.index("--fgb-dir") + 1] == str(job.work_dir)
    assert command[command.index("--pmtiles-dir") + 1] == str(job.work_dir)
    assert "--resource-layout" in command
    assert command[-1] == "--skip-existing"
    manifest = json.loads(job.manifest_path.read_text(encoding="utf-8"))
    assert manifest["stages"]["derivatives"]["status"] == "completed"


def test_zip_stage_creates_one_upload_archive_per_geopackage(tmp_path: Path) -> None:
    job = load_job_config(write_config(tmp_path))
    run_metadata_stage(job, catalog=catalog_fixture())
    confirm_manual_review(job, confirmed=True)
    gpkg_path = job.gpkg_path("stp_zoning_2026.gpkg")
    gpkg_path.parent.mkdir(parents=True)
    gpkg_path.write_bytes(b"geopackage payload")

    run_zip_stage(job)

    archive_path = gpkg_path.with_name(f"{gpkg_path.name}.zip")
    assert archive_path.is_file()
    with ZipFile(archive_path) as archive:
        assert archive.namelist() == [gpkg_path.name]
        assert archive.read(gpkg_path.name) == b"geopackage payload"
    manifest = json.loads(job.manifest_path.read_text(encoding="utf-8"))
    assert manifest["stages"]["zip"]["outputs"] == [
        "stp_zoning_2026/stp_zoning_2026.gpkg.zip"
    ]


def test_dictionary_and_thumbnail_stages_write_into_resource_directory(
    tmp_path: Path,
    monkeypatch,
) -> None:
    job = load_job_config(write_config(tmp_path))
    run_metadata_stage(job, catalog=catalog_fixture())
    confirm_manual_review(job, confirmed=True)

    run_dictionary_stage(
        job,
        requester=lambda *args, **kwargs: {
            "fields": [
                {
                    "name": "NAME",
                    "type": "esriFieldTypeString",
                    "alias": "Name",
                }
            ]
        },
    )
    dictionary_path = job.dictionary_path("stp_zoning_2026.gpkg")
    assert dictionary_path.is_file()

    gpkg_path = job.gpkg_path("stp_zoning_2026.gpkg")
    gpkg_path.write_bytes(b"placeholder")

    def fake_thumbnail(source: Path, output: Path) -> None:
        assert source == gpkg_path
        output.write_bytes(b"thumbnail")

    monkeypatch.setattr(pipeline, "create_vector_thumbnail", fake_thumbnail)
    run_thumbnail_stage(job)

    thumbnail_path = job.thumbnail_path("stp_zoning_2026.gpkg")
    assert thumbnail_path.read_bytes() == b"thumbnail"
    manifest = json.loads(job.manifest_path.read_text(encoding="utf-8"))
    assert manifest["stages"]["dictionaries"]["outputs"] == [
        "stp_zoning_2026/stp_zoning_2026.csv"
    ]
    assert manifest["stages"]["thumbnails"]["outputs"] == [
        "stp_zoning_2026/stp_zoning_2026.png"
    ]


def test_dictionary_falls_back_to_local_geopackage_schema(
    tmp_path: Path,
) -> None:
    job = load_job_config(write_config(tmp_path))
    run_metadata_stage(job, catalog=catalog_fixture())
    confirm_manual_review(job, confirmed=True)
    write_test_geopackage(job.gpkg_path("stp_zoning_2026.gpkg"))

    def unavailable_service(*args, **kwargs) -> dict:
        raise RuntimeError("ArcGIS REST services directory is disabled")

    run_dictionary_stage(job, requester=unavailable_service)

    dictionary = pd.read_csv(
        job.dictionary_path("stp_zoning_2026.gpkg"),
        dtype=str,
        keep_default_na=False,
    )
    assert dictionary[["field_name", "field_type"]].to_dict("records") == [
        {"field_name": "NAME", "field_type": "str:80"},
        {"field_name": "COUNT", "field_type": "int"},
    ]
    assert dictionary["values"].tolist() == ["", ""]
    assert dictionary["definition"].tolist() == ["", ""]
    manifest = json.loads(job.manifest_path.read_text(encoding="utf-8"))
    stage = manifest["stages"]["dictionaries"]
    assert stage["status"] == "completed"
    assert stage["fallback_count"] == 1
    assert stage["records"][0]["metadata_source"] == "geopackage_schema"
    assert stage["records"][0]["fallback_error"] == (
        "ArcGIS REST services directory is disabled"
    )


def test_save_run_record_copies_small_inputs_and_describes_artifacts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    job = load_job_config(write_config(tmp_path))
    run_metadata_stage(job, catalog=catalog_fixture())
    confirm_manual_review(job, confirmed=True)
    for stage in pipeline.SNAPSHOT_REQUIRED_STAGES:
        if stage != "metadata":
            mark_stage(job, stage)

    artifact_path = job.gpkg_path("stp_zoning_2026.gpkg")
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_bytes(b"portable artifact identity")
    run_records_root = tmp_path / "curation" / "run_records"
    monkeypatch.setattr(pipeline, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(pipeline, "RUN_RECORDS_ROOT", run_records_root)

    run_record_path = save_run_record(job)

    assert run_record_path.parent == run_records_root / "test-job"
    assert (run_record_path / "job.yaml").read_text(
        encoding="utf-8"
    ) == job.config_path.read_text(encoding="utf-8")
    assert (
        run_record_path / "metadata.csv"
    ).read_bytes() == job.metadata_path.read_bytes()
    saved_manifest_text = (run_record_path / "manifest.json").read_text(
        encoding="utf-8"
    )
    saved_manifest = json.loads(saved_manifest_text)
    assert str(tmp_path) not in saved_manifest_text
    assert saved_manifest["config_path"] == "job.yaml"
    assert saved_manifest["work_directory"] == "work"
    assert saved_manifest["metadata_path"] == "metadata/metadata.csv"
    assert saved_manifest["run_record"]["metadata_csv"] == "metadata.csv"
    assert saved_manifest["run_record"]["job_config"] == "job.yaml"
    assert saved_manifest["stages"]["snapshot"]["status"] == "completed"
    assert saved_manifest["artifacts"] == [
        {
            "role": "geopackage",
            "path": "stp_zoning_2026/stp_zoning_2026.gpkg",
            "size_bytes": artifact_path.stat().st_size,
            "sha256": pipeline.file_sha256(artifact_path),
        }
    ]
    live_manifest_text = job.manifest_path.read_text(encoding="utf-8")
    assert str(tmp_path) not in live_manifest_text
    assert json.loads(live_manifest_text)["stages"]["snapshot"]["status"] == "completed"


def test_enrich_uses_local_geopackage_geometry_and_decimal_degree_bbox(
    tmp_path: Path,
) -> None:
    job = load_job_config(write_config(tmp_path))
    run_metadata_stage(job, catalog=catalog_fixture())
    confirm_manual_review(job, confirmed=True)
    gpkg_path = job.gpkg_path("stp_zoning_2026.gpkg")
    write_test_geopackage(gpkg_path)

    def requester(*args, **kwargs) -> dict:
        raise AssertionError("enrichment should not access the ArcGIS service")

    run_enrich_stage(job, requester=requester)

    row = pd.read_csv(job.metadata_path, dtype=str, keep_default_na=False).iloc[0]
    assert row["Resource Type"] == "Polygon data"
    assert row["Bounding Box"] == "-93.2080,44.8875,-93.0037,44.9920"
    assert row["Geometry"] == "ENVELOPE(-93.2080,-93.0037,44.9920,44.8875)"
    assert row["Centroid"] == "44.9398,-93.1059"
    require_confirmed_review(job)
    manifest = json.loads(job.manifest_path.read_text(encoding="utf-8"))
    assert manifest["stages"]["enrich"]["records"][0]["metadata_source"] == (
        "geopackage"
    )


PACKAGE_ITEM_ID = "21881eeccd734bdc9a20624bdeabc4b3"
PACKAGE_ITEM_API = (
    f"https://www.arcgis.com/sharing/rest/content/items/{PACKAGE_ITEM_ID}"
)


def package_record(layer: str = "Zoning") -> dict:
    return {
        "id": f"{PACKAGE_ITEM_ID}:{layer}",
        "filename": f"test_{layer.lower()}",
        "basic_theme": layer,
        "temporal_year": "2026",
        "source": {"type": "geopackage", "item_id": PACKAGE_ITEM_ID, "layer": layer},
    }


def package_requester(url: str, params: dict | None, method: str) -> dict:
    assert url == PACKAGE_ITEM_API
    return {
        "type": "GeoPackage",
        "title": "Planning package",
        "owner": "CityGIS",
        "description": "Planning layers.",
        "accessInformation": "City Planning",
        "licenseInfo": "ODbL",
        "tags": ["planning"],
        "created": 1717728314000,
        "modified": 1794939015000,
    }


def mock_package_download(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> list[str]:
    source = tmp_path / "input.gpkg"
    for layer in ("Zoning", "Districts"):
        with fiona.open(
            source,
            "w",
            driver="GPKG",
            layer=layer,
            crs="EPSG:4326",
            schema={"geometry": "Point", "properties": {"NAME": "str", "COUNT": "int"}},
        ) as collection:
            collection.write(
                {
                    "geometry": {"type": "Point", "coordinates": (-93.1, 44.9)},
                    "properties": {"NAME": layer, "COUNT": 1},
                }
            )
    calls = []

    def download(job, url):
        calls.append(url)
        destination = job.work_dir / "sources" / f"{pipeline.file_sha256(source)}.gpkg"
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
        return destination

    monkeypatch.setattr(pipeline, "download_source_geopackage", download)
    return calls


@pytest.mark.parametrize(
    "source_update,match",
    [
        ({"item_id": "bad"}, "32-character"),
        ({"item_id": ""}, "item_id"),
        ({"layer": ""}, "layer"),
        ({"layer": 1}, "layer"),
        ({"service_url": SERVICE_URL}, "service_url"),
        ({"portal_url": "not a URL"}, "portal"),
    ],
)
def test_config_rejects_invalid_geopackage_source(tmp_path, source_update, match):
    record = package_record()
    record["source"].update(source_update)
    with pytest.raises(CurationConfigError, match=match):
        load_job_config(write_config(tmp_path, records=[record]))


def test_config_rejects_layer_on_other_source_types(tmp_path):
    record = {"id": SOURCE_ID, "filename": "test", "source": {"layer": "Zoning"}}
    with pytest.raises(CurationConfigError, match="layer requires type: geopackage"):
        load_job_config(write_config(tmp_path, records=[record]))


def test_metadata_supports_mixed_dcat_and_package_layers(tmp_path, monkeypatch):
    calls = mock_package_download(tmp_path, monkeypatch)
    zoning = package_record()
    zoning["metadata_overrides"] = {
        "creator": "Planning Office",
        "rights": "Open license",
    }
    job = load_job_config(
        write_config(
            tmp_path,
            records=[
                {"id": SOURCE_ID, "filename": "rest", "basic_theme": "Zoning"},
                zoning,
                package_record("Districts"),
            ],
        )
    )
    output = run_metadata_stage(
        job, requester=package_requester, catalog=catalog_fixture()
    )
    rows = pd.read_csv(output, keep_default_na=False)
    assert list(rows["filename"]) == [
        "rest.gpkg",
        "test_zoning.gpkg",
        "test_districts.gpkg",
    ]
    assert rows.iloc[1]["Creator"] == "Planning Office"
    assert rows.iloc[1]["Rights"] == "Open license"
    assert rows.iloc[1]["Resource Type"] == "Point data"
    assert rows.iloc[1]["Temporal Coverage"] == 2026
    assert rows.iloc[1]["Alternative Title"] == "Zoning"
    assert (
        rows.iloc[1]["Identifier"]
        == f"https://www.arcgis.com/home/item.html?id={PACKAGE_ITEM_ID}"
    )
    assert rows.iloc[1]["Endpoint URL"] == PACKAGE_ITEM_API + "/data"
    assert "Extracted layer 'Zoning'" in rows.iloc[1]["Provenance"]
    assert calls == [PACKAGE_ITEM_API + "/data"]
    manifest = pipeline.load_manifest(job)
    package = manifest["records"][1]
    assert package["service_url"] == ""
    assert package["source_type"] == "geopackage"
    assert package["layer"] == "Zoning"
    assert package["download_url"] == PACKAGE_ITEM_API + "/data"
    assert len(package["source_sha256"]) == 64


def test_missing_package_layer_reports_available_layers(tmp_path, monkeypatch):
    mock_package_download(tmp_path, monkeypatch)
    job = load_job_config(write_config(tmp_path, records=[package_record("Streets")]))
    with pytest.raises(RuntimeError, match="available layers: Zoning, Districts"):
        run_metadata_stage(job, requester=package_requester)
    assert not job.metadata_path.exists()


def test_package_source_rejects_other_arcgis_item_types(tmp_path):
    job = load_job_config(write_config(tmp_path, records=[package_record()]))
    with pytest.raises(RuntimeError, match="not a GeoPackage"):
        run_metadata_stage(job, requester=lambda *args: {"type": "Feature Service"})


@pytest.mark.skipif(shutil.which("ogr2ogr") is None, reason="ogr2ogr required")
def test_package_extract_enrich_and_dictionary(tmp_path, monkeypatch):
    calls = mock_package_download(tmp_path, monkeypatch)
    job = load_job_config(
        write_config(tmp_path, records=[package_record(), package_record("Districts")])
    )
    run_metadata_stage(job, requester=package_requester)
    with pytest.raises(RuntimeError, match="review"):
        run_download_stage(job)
    confirm_manual_review(job, confirmed=True)
    results = run_download_stage(job)
    assert [result["status"] for result in results] == ["downloaded", "downloaded"]
    assert calls == [PACKAGE_ITEM_API + "/data"]
    for record in job.records:
        path = job.gpkg_path(record.filename)
        assert fiona.listlayers(path) == [record.filename_stem]
        with fiona.open(path) as collection:
            assert len(collection) == 1
            assert next(iter(collection))["properties"]["NAME"] == record.layer
            assert collection.crs.to_authority() == ("ESRI", "103768")
    assert all(
        result["status"] == "skipped_existing" for result in run_download_stage(job)
    )
    run_enrich_stage(job)

    def no_rest_request(*args):
        raise AssertionError("GeoPackage dictionaries must not request REST fields")

    run_dictionary_stage(job, requester=no_rest_request)
    fields = pd.read_csv(
        job.dictionary_path(job.records[0].filename), keep_default_na=False
    )
    assert list(fields["field_name"]) == ["NAME", "COUNT"]
    assert list(fields["definition"]) == ["", ""]
    manifest = pipeline.load_manifest(job)
    assert manifest["stages"]["dictionaries"]["fallback_count"] == 0
    assert all(
        row["metadata_source"] == "geopackage_schema"
        for row in manifest["stages"]["dictionaries"]["records"]
    )
    assert calls == [PACKAGE_ITEM_API + "/data"]


def test_package_extraction_rejects_changed_snapshot(tmp_path, monkeypatch):
    mock_package_download(tmp_path, monkeypatch)
    job = load_job_config(write_config(tmp_path, records=[package_record()]))
    run_metadata_stage(job, requester=package_requester)
    confirm_manual_review(job, confirmed=True)
    record = pipeline.load_manifest(job)["records"][0]
    cached = job.work_dir / "sources" / f"{record['source_sha256']}.gpkg"
    cached.write_bytes(b"changed")
    results = run_download_stage(job)
    assert results[0]["status"] == "failed"
    assert "rerun metadata and manual review" in results[0]["error"]
    assert not job.gpkg_path(job.records[0].filename).exists()


@pytest.mark.parametrize("valid", [True, False])
def test_package_binary_download_is_atomic_and_validated(tmp_path, monkeypatch, valid):
    job = load_job_config(write_config(tmp_path))
    source = tmp_path / "download.gpkg"
    write_test_geopackage(source)
    payload = source.read_bytes() if valid else b'{"error": {"message": "not found"}}'

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def raise_for_status(self):
            pass

        def iter_content(self, chunk_size):
            yield payload[:100]
            yield payload[100:]

    monkeypatch.setattr(pipeline.requests, "get", lambda *args, **kwargs: Response())
    if valid:
        package = pipeline.download_source_geopackage(job, PACKAGE_ITEM_API + "/data")
        assert package.stem == pipeline.file_sha256(source)
        assert package.read_bytes() == payload
    else:
        with pytest.raises(RuntimeError, match="not a GeoPackage"):
            pipeline.download_source_geopackage(job, PACKAGE_ITEM_API + "/data")
        assert list((job.work_dir / "sources").iterdir()) == []


@pytest.mark.skipif(shutil.which("ogr2ogr") is None, reason="ogr2ogr required")
@pytest.mark.parametrize("changed", [False, True])
def test_missing_package_cache_requires_identical_redownload(
    tmp_path, monkeypatch, changed
):
    calls = mock_package_download(tmp_path, monkeypatch)
    job = load_job_config(write_config(tmp_path, records=[package_record()]))
    run_metadata_stage(job, requester=package_requester)
    confirm_manual_review(job, confirmed=True)
    record = pipeline.load_manifest(job)["records"][0]
    (job.work_dir / "sources" / f"{record['source_sha256']}.gpkg").unlink()
    if changed:
        with fiona.open(tmp_path / "input.gpkg", "a", layer="Zoning") as collection:
            collection.write(
                {
                    "geometry": {"type": "Point", "coordinates": (-93.2, 44.8)},
                    "properties": {"NAME": "Changed", "COUNT": 2},
                }
            )
    results = run_download_stage(job)
    assert results[0]["status"] == ("failed" if changed else "downloaded")
    assert job.gpkg_path(job.records[0].filename).exists() is not changed
    assert calls == [PACKAGE_ITEM_API + "/data", PACKAGE_ITEM_API + "/data"]
