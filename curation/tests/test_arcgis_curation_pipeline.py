from __future__ import annotations

import json
import re
import sys
from pathlib import Path

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
    run_download_stage,
    run_enrich_stage,
    run_metadata_stage,
    save_run_record,
)


SOURCE_ID = "1ec9eb84c4fd41548946b1484d4f31ef_6"
SERVICE_URL = (
    "https://services1.arcgis.com/9meaaHE3uiba0zr8/arcgis/rest/services/"
    "PrincipalZoning_TEST/FeatureServer/6"
)


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


def test_config_accepts_a_single_selected_record(tmp_path: Path) -> None:
    job = load_job_config(write_config(tmp_path))

    assert job.records[0].filename == "stp_zoning_2026.gpkg"
    assert len(job.records) == 1


def test_copyable_job_template_is_valid_yaml() -> None:
    template_path = (
        CURATION_ROOT / "jobs" / "arcgis_curation_pipeline_template.yaml"
    )
    template = yaml.safe_load(template_path.read_text(encoding="utf-8"))

    assert template["version"] == 1
    assert template["provider"] == "BTAA-GIN"
    assert template["metadata"]["member_of"] == "b1g_urbanBaseLayers"
    assert set(template["selection_criteria"]) == {"allowed_resource_types"}
    assert template["records"][0]["basic_theme"] == "<basic-theme>"


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
    assert row["Coordinate Reference System"] == "https://spatialreference.org/ref/esri/103768"
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
    saved_manifest_path = run_records_root / "test-job" / "20260101T000000Z" / "manifest.json"
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
    second_service_url = "https://example.org/arcgis/rest/services/Boundary/FeatureServer/0"
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

    ids_by_filename = pd.read_csv(second_job.metadata_path, dtype=str).set_index("filename")[
        "ID"
    ]
    assert ids_by_filename["stp_zoning_2026.gpkg"] == first_id
    assert re.fullmatch(r"b1g_[A-Za-z0-9]{12}", ids_by_filename["stp_boundary_2026.gpkg"])
    assert ids_by_filename["stp_boundary_2026.gpkg"] != first_id


def test_review_checksum_detects_manual_edits_after_confirmation(tmp_path: Path) -> None:
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
    job.gpkg_dir.mkdir(parents=True)
    existing_path = job.gpkg_dir / "stp_zoning_2026.gpkg"
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
        "output": "gpkg/stp_zoning_2026.gpkg",
    }
    assert outputs[1]["status"] == "downloaded"
    assert downloaded == [job.gpkg_dir / "stp_new_2026.gpkg"]


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

    job.gpkg_dir.mkdir(parents=True)
    artifact_path = job.gpkg_dir / "stp_zoning_2026.gpkg"
    artifact_path.write_bytes(b"portable artifact identity")
    run_records_root = tmp_path / "curation" / "run_records"
    monkeypatch.setattr(pipeline, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(pipeline, "RUN_RECORDS_ROOT", run_records_root)

    run_record_path = save_run_record(job)

    assert run_record_path.parent == run_records_root / "test-job"
    assert (run_record_path / "job.yaml").read_text(encoding="utf-8") == job.config_path.read_text(
        encoding="utf-8"
    )
    assert (run_record_path / "metadata.csv").read_bytes() == job.metadata_path.read_bytes()
    saved_manifest_text = (run_record_path / "manifest.json").read_text(encoding="utf-8")
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
            "path": "gpkg/stp_zoning_2026.gpkg",
            "size_bytes": artifact_path.stat().st_size,
            "sha256": pipeline.file_sha256(artifact_path),
        }
    ]
    live_manifest_text = job.manifest_path.read_text(encoding="utf-8")
    assert str(tmp_path) not in live_manifest_text
    assert json.loads(live_manifest_text)["stages"]["snapshot"]["status"] == "completed"


def test_enrich_adds_service_geometry_and_decimal_degree_bbox(tmp_path: Path) -> None:
    job = load_job_config(write_config(tmp_path))
    run_metadata_stage(job, catalog=catalog_fixture())
    confirm_manual_review(job, confirmed=True)
    job.gpkg_dir.mkdir(parents=True)
    (job.gpkg_dir / "stp_zoning_2026.gpkg").write_bytes(b"test placeholder")

    def requester(url: str, params: dict | None, method: str) -> dict:
        if url == SERVICE_URL:
            return {"geometryType": "esriGeometryPolygon"}
        assert url == f"{SERVICE_URL}/query"
        assert method == "POST"
        assert params and params["outSR"] == "4326"
        return {
            "extent": {
                "xmin": -93.20804,
                "ymin": 44.88746,
                "xmax": -93.00366,
                "ymax": 44.99204,
            }
        }

    run_enrich_stage(job, requester=requester)

    row = pd.read_csv(job.metadata_path, dtype=str, keep_default_na=False).iloc[0]
    assert row["Resource Type"] == "Polygon data"
    assert row["Bounding Box"] == "-93.2080,44.8875,-93.0037,44.9920"
    assert row["Centroid"] == "44.9398,-93.1059"
    require_confirmed_review(job)
