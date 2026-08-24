from __future__ import annotations

import json
import sys
from copy import deepcopy
from pathlib import Path

import pandas as pd
import pytest
import yaml


CURATION_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = CURATION_ROOT.parent
sys.path.insert(0, str(CURATION_ROOT / "src"))

import curation.socrata_curation_pipeline as pipeline  # noqa: E402

from curation.socrata_curation_pipeline import (  # noqa: E402
    CurationConfigError,
    confirm_manual_review,
    download_socrata_geopackage,
    load_job_config,
    require_confirmed_review,
    run_dictionary_stage,
    run_enrich_stage,
    run_metadata_stage,
)


SOURCE_ID = "ndfd-h5qf"
API_BASE = "https://data.bloomington.in.gov"
METADATA_URL = f"{API_BASE}/api/views/{SOURCE_ID}"
GEOJSON_URL = f"{API_BASE}/resource/{SOURCE_ID}.geojson"


def catalog_fixture() -> dict:
    return {
        "dataset": [
            {
                "identifier": METADATA_URL,
                "landingPage": f"{API_BASE}/d/{SOURCE_ID}",
                "title": "TreeKeeper Inventory",
                "description": "Individual tree inventory maintained by the Parks Department.",
                "publisher": {"name": "City of Bloomington"},
                "keyword": ["gis", "parks and recreation", "trees"],
                "theme": ["Parks & Recreation"],
                "issued": "2023-06-26",
                "modified": "2026-08-05",
                "license": "http://opendatacommons.org/licenses/pddl/1.0/",
            }
        ]
    }


def view_metadata_fixture() -> dict:
    return {
        "id": SOURCE_ID,
        "name": "TreeKeeper Inventory",
        "rowsUpdatedAt": 1_785_960_319,
        "columns": [
            {
                "name": "Site ID",
                "fieldName": "site_id",
                "dataTypeName": "number",
                "description": "Unique tree ID",
                "position": 2,
                "format": {},
            },
            {
                "name": "Condition",
                "fieldName": "condition",
                "dataTypeName": "text",
                "description": "Tree Condition",
                "position": 3,
                "format": {"dropDownList": ["Good", "Fair", "Poor"]},
            },
            {
                "name": "Geometry",
                "fieldName": "the_geom",
                "dataTypeName": "point",
                "description": "GIS geometry data field",
                "position": 4,
                "format": {},
            },
        ],
    }


def write_config(tmp_path: Path, *, page_size: int = 1_000) -> Path:
    websites_path = tmp_path / "websites.csv"
    pd.DataFrame(
        [
            {
                "Title": "City of Bloomington, Indiana Open Data Portal",
                "Provider": "City of Bloomington",
                "Spatial Coverage": "Indiana--Bloomington|Indiana",
                "Bounding Box": "-86.592,39.121,-86.471,39.221",
                "Member Of": "b1g_urbanBaseLayers",
                "ID": "01c-01",
                "Identifier": API_BASE,
                "Code": "01c-01",
            }
        ]
    ).to_csv(websites_path, index=False)
    config = {
        "version": 1,
        "job": {
            "id": "socrata-test-job",
            "work_directory": str(tmp_path / "work"),
        },
        "provider": "BTAA-GIN",
        "hub": {
            "name": "City of Bloomington Open Data",
            "landing_page": f"{API_BASE}/",
            "dcat_api": f"{API_BASE}/data.json",
            "soda_api_base": API_BASE,
            "website_reference_id": "01c-01",
            "websites_csv": str(websites_path),
        },
        "coordinate_reference_system": {
            "authority": "EPSG:2966",
            "uri": "https://spatialreference.org/ref/epsg/2966/",
        },
        "metadata": {
            "code": "b1g_18_05860",
            "member_of": "b1g_urbanBaseLayers",
            "export_date": "2026-08-23",
        },
        "file_naming": {
            "city_abbreviation": "blm",
            "download_year": "2026",
        },
        "download": {"page_size": page_size},
        "selection_criteria": {"allowed_resource_types": ["Point data"]},
        "manual_review": {
            "required_fields": [
                "filename",
                "ID",
                "Title",
                "Description",
                "Creator",
                "Publisher",
                "Provider",
                "Resource Class",
                "Rights",
                "Access Rights",
            ]
        },
        "records": [
            {
                "id": SOURCE_ID,
                "filename_theme": "trees",
                "basic_theme": "Trees",
                "temporal_year": "2026",
            }
        ],
    }
    config_path = tmp_path / "job.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return config_path


def source_requester(
    url: str,
    params: dict | None,
    headers: dict[str, str] | None,
):
    assert headers == {}
    if url == METADATA_URL:
        return deepcopy(view_metadata_fixture())
    if url == f"{API_BASE}/resource/{SOURCE_ID}.json":
        assert params == {"$select": "count(*)"}
        return [{"count": "2001"}]
    raise AssertionError(f"Unexpected URL: {url}")


def test_metadata_stage_reuses_socrata_mapping_and_records_source_identity(
    tmp_path: Path,
) -> None:
    job = load_job_config(write_config(tmp_path))

    output_path = run_metadata_stage(
        job,
        requester=source_requester,
        catalog=catalog_fixture(),
    )

    row = pd.read_csv(output_path, dtype=str, keep_default_na=False).iloc[0]
    assert row["filename"] == "blm_trees_2026.gpkg"
    assert row["ID"].startswith("b1g_")
    assert row["Code"] == "b1g_18_05860"
    assert row["Title"] == "Trees [Indiana--Bloomington] {2026}"
    assert row["Description"].startswith(
        "Historical dataset of Trees in Bloomington, Indiana as of 2026."
    )
    assert row["Provider"] == "BTAA-GIN"
    assert row["Resource Class"] == "Datasets"
    assert row["Format"] == "GeoPackage"
    assert row["Coordinate Reference System"].endswith("/epsg/2966/")
    assert row["Harvest Workflow"] == "curation_datasets"
    assert GEOJSON_URL in row["Provenance"]
    assert "2001 rows" in row["Provenance"]

    manifest = json.loads(job.manifest_path.read_text(encoding="utf-8"))
    record = manifest["records"][0]
    assert manifest["source_platform"] == "socrata"
    assert manifest["file_naming"] == {
        "city_abbreviation": "blm",
        "download_year": "2026",
    }
    assert record["source_id"] == SOURCE_ID
    assert record["filename_theme"] == "trees"
    assert record["row_count"] == 2001
    assert record["source_revision"] == 1_785_960_319
    assert record["geometry_columns"] == [
        {"field_name": "the_geom", "field_type": "point"}
    ]


def test_download_pages_past_the_default_thousand_row_limit(
    tmp_path: Path,
    monkeypatch,
) -> None:
    output_path = tmp_path / "trees.gpkg"
    page_requests: list[dict] = []
    count_requests = 0

    def requester(url: str, params: dict | None, headers: dict[str, str] | None):
        nonlocal count_requests
        assert headers == {"X-App-Token": "token"}
        if url.endswith(f"/{SOURCE_ID}.json"):
            count_requests += 1
            assert params == {"$select": "count(*)"}
            return [{"count": "2001"}]
        assert url == GEOJSON_URL
        assert params is not None
        page_requests.append(params)
        feature = {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-86.5, 39.16]},
            "properties": {"site_id": "1"},
        }
        return {
            "type": "FeatureCollection",
            "features": [feature] * int(params["$limit"]),
        }

    commands: list[list[str]] = []

    def fake_run_command(command: list[str]) -> None:
        commands.append(command)
        partial_path = next(Path(value) for value in command if value.endswith(".partial.gpkg"))
        partial_path.write_bytes(b"partial geopackage")

    monkeypatch.setattr(pipeline.shutil, "which", lambda name: "/usr/bin/ogr2ogr")
    monkeypatch.setattr(pipeline, "_run_command", fake_run_command)

    result = download_socrata_geopackage(
        API_BASE,
        SOURCE_ID,
        output_path,
        "trees",
        "EPSG:2966",
        page_size=1000,
        expected_count=2001,
        requester=requester,
        headers={"X-App-Token": "token"},
    )

    assert page_requests == [
        {"$limit": 1000, "$offset": 0, "$order": ":id"},
        {"$limit": 1000, "$offset": 1000, "$order": ":id"},
        {"$limit": 1, "$offset": 2000, "$order": ":id"},
    ]
    assert count_requests == 2
    assert len(commands) == 3
    assert "-append" not in commands[0]
    assert "-append" in commands[1]
    assert result["feature_count"] == 2001
    assert result["page_count"] == 3
    assert output_path.read_bytes() == b"partial geopackage"


def test_download_stops_when_source_revision_changed(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(pipeline.shutil, "which", lambda name: "/usr/bin/ogr2ogr")

    def requester(url: str, params: dict | None, headers: dict[str, str] | None):
        assert url == METADATA_URL
        return {"rowsUpdatedAt": 2}

    with pytest.raises(RuntimeError, match="changed after the metadata stage"):
        download_socrata_geopackage(
            API_BASE,
            SOURCE_ID,
            tmp_path / "trees.gpkg",
            "trees",
            "EPSG:2966",
            metadata_url=METADATA_URL,
            expected_revision=1,
            requester=requester,
        )


def test_dictionary_uses_socrata_column_names_types_and_descriptions(
    tmp_path: Path,
) -> None:
    job = load_job_config(write_config(tmp_path))
    run_metadata_stage(job, requester=source_requester, catalog=catalog_fixture())
    confirm_manual_review(job, confirmed=True)

    run_dictionary_stage(job, requester=source_requester)

    dictionary = pd.read_csv(job.dictionary_path("blm_trees_2026.gpkg"), dtype=str).fillna("")
    assert list(dictionary["field_name"]) == ["site_id", "condition", "the_geom"]
    assert list(dictionary["field_type"]) == ["number", "text", "point"]
    assert dictionary.iloc[0]["definition"] == "Site ID: Unique tree ID"
    assert dictionary.iloc[1]["values"] == "Good|Fair|Poor"
    assert set(dictionary["definition_source"]) == {METADATA_URL}


def test_enrich_uses_completed_geopackage_count_geometry_and_bounds(
    tmp_path: Path,
    monkeypatch,
) -> None:
    job = load_job_config(write_config(tmp_path))
    run_metadata_stage(job, requester=source_requester, catalog=catalog_fixture())
    confirm_manual_review(job, confirmed=True)
    gpkg_path = job.gpkg_path("blm_trees_2026.gpkg")
    gpkg_path.parent.mkdir(parents=True)
    gpkg_path.write_bytes(b"placeholder")
    monkeypatch.setattr(
        pipeline,
        "inspect_geopackage",
        lambda path: {
            "feature_count": 2001,
            "geometry_type": "MultiPoint",
            "resource_type": "Point data",
            "bounds": (-86.592, 39.121, -86.471, 39.221),
        },
    )

    run_enrich_stage(job)

    row = pd.read_csv(job.metadata_path, dtype=str, keep_default_na=False).iloc[0]
    assert row["Resource Type"] == "Point data"
    assert row["Bounding Box"] == "-86.5920,39.1210,-86.4710,39.2210"
    assert row["Centroid"] == "39.1710,-86.5315"
    require_confirmed_review(job)


def test_config_rejects_page_sizes_above_soda2_maximum(tmp_path: Path) -> None:
    config_path = write_config(tmp_path, page_size=50_001)

    with pytest.raises(CurationConfigError, match="between 1 and 50000"):
        load_job_config(config_path)


def test_config_rejects_filename_year_that_differs_from_export_date(
    tmp_path: Path,
) -> None:
    config_path = write_config(tmp_path)
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["file_naming"]["download_year"] = "2025"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    with pytest.raises(CurationConfigError, match="must match the year"):
        load_job_config(config_path)


def test_config_rejects_filename_theme_with_spaces(tmp_path: Path) -> None:
    config_path = write_config(tmp_path)
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config["records"][0]["filename_theme"] = "city trees"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    with pytest.raises(CurationConfigError, match="filename_theme must contain"):
        load_job_config(config_path)


def test_config_rejects_duplicate_constructed_filenames(tmp_path: Path) -> None:
    config_path = write_config(tmp_path)
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    duplicate_record = deepcopy(config["records"][0])
    duplicate_record["id"] = "abcd-1234"
    config["records"].append(duplicate_record)
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    with pytest.raises(CurationConfigError, match="Duplicate output filename"):
        load_job_config(config_path)
