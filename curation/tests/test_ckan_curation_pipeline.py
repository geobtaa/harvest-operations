from __future__ import annotations

import json
import sys
from pathlib import Path

import fiona
import pandas as pd
import pytest
import yaml


CURATION_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = CURATION_ROOT.parent
sys.path.insert(0, str(CURATION_ROOT / "src"))

from curation.ckan_curation_pipeline import (  # noqa: E402
    CurationConfigError,
    inspect_geopackage,
    load_job_config,
    run_metadata_stage,
    select_geojson_resource,
)


SOURCE_ID = "address-points"
PACKAGE_ID = "20096a60-3bd6-4c60-ad11-11190fe8a789"
RESOURCE_ID = "78e34312-ade9-480b-9b46-8bc33925acb6"
API_BASE = "https://ckan.example.org"


def package_fixture() -> dict:
    return {
        "id": PACKAGE_ID,
        "name": SOURCE_ID,
        "title": "Address Points",
        "notes": "Address locations maintained by the city.",
        "metadata_created": "2025-07-22T18:28:50.740039",
        "metadata_modified": "2026-09-27T15:01:45.736075",
        "license_title": "Public Domain",
        "license_url": "https://creativecommons.org/publicdomain/zero/1.0/",
        "private": False,
        "organization": {"name": "city-of-ann-arbor", "title": "City of Ann Arbor"},
        "tags": [{"name": "address"}, {"name": "geospatial"}],
        "groups": [],
        "resources": [
            {
                "id": RESOURCE_ID,
                "name": "AA_Address.json",
                "format": "GeoJSON",
                "url": "https://ckan.example.org/download/aa_address.geojson",
                "last_modified": "2026-09-27T15:01:41.494998",
                "size": 100,
            }
        ],
    }


def write_config(tmp_path: Path, *, resource_id: str = RESOURCE_ID) -> Path:
    websites_path = tmp_path / "websites.csv"
    pd.DataFrame(
        [
            {
                "Title": "Ann Arbor Data Catalog",
                "Provider": "City of Ann Arbor",
                "Spatial Coverage": "Michigan--Ann Arbor|Michigan",
                "Bounding Box": "-83.800,42.223,-83.676,42.324",
                "Member Of": "b1g_urbanBaseLayers",
                "ID": "07c-02",
                "Identifier": "https://data.example.org",
                "Code": "07c-02",
            }
        ]
    ).to_csv(websites_path, index=False)
    config = {
        "version": 1,
        "job": {"id": "ckan-test-job", "work_directory": str(tmp_path / "work")},
        "provider": "BTAA-GIN",
        "hub": {
            "name": "City of Ann Arbor Open Data Portal",
            "landing_page": "https://data.example.org/",
            "api_base": API_BASE,
            "website_reference_id": "07c-02",
            "websites_csv": str(websites_path),
        },
        "coordinate_reference_system": {
            "authority": "EPSG:2253",
            "uri": "https://spatialreference.org/ref/epsg/2253/",
        },
        "metadata": {
            "code": "b1g_26_03000",
            "member_of": "b1g_urbanBaseLayers",
            "export_date": "2026-10-03",
        },
        "file_naming": {"city_abbreviation": "a2", "download_year": "2026"},
        "selection_criteria": {},
        "manual_review": {
            "required_fields": [
                "filename",
                "ID",
                "Title",
                "Provider",
                "Resource Class",
                "License",
                "Access Rights",
            ]
        },
        "records": [
            {
                "id": SOURCE_ID,
                "resource_id": resource_id,
                "filename_theme": "address",
                "basic_theme": "Address points",
                "temporal_year": "2026",
            }
        ],
    }
    path = tmp_path / "job.yaml"
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return path


def test_ann_arbor_job_yaml_is_valid_and_contains_all_selected_packages() -> None:
    job = load_job_config(CURATION_ROOT / "jobs" / "ckan" / "ann-arbor-ckan-2026.yaml")

    assert job.crs_authority == "EPSG:2253"
    assert job.code == "b1g_26_03000"
    assert {record.source_id for record in job.records} == {
        "address-points",
        "zoning-districts",
        "road-centerline",
        "city-boundary",
        "building-footprints",
        "parks",
        "street-trees",
    }
    assert all(record.resource_id for record in job.records)


def test_config_rejects_non_uuid_resource_id(tmp_path: Path) -> None:
    path = write_config(tmp_path, resource_id="not-a-uuid")

    with pytest.raises(CurationConfigError, match="resource UUID"):
        load_job_config(path)


def test_geojson_resource_selection_honors_pinned_resource(tmp_path: Path) -> None:
    job = load_job_config(write_config(tmp_path))
    resource = select_geojson_resource(package_fixture(), job.records[0])

    assert resource["id"] == RESOURCE_ID


def test_metadata_stage_reuses_ckan_mapping_and_records_source_identity(tmp_path: Path) -> None:
    job = load_job_config(write_config(tmp_path))
    metadata_url = job.package_url(SOURCE_ID)

    def requester(url: str, params: dict | None):
        assert url == metadata_url
        assert params is None
        return {"success": True, "result": package_fixture()}

    output_path = run_metadata_stage(job, requester=requester)

    row = pd.read_csv(output_path, dtype=str, keep_default_na=False).iloc[0]
    assert row["filename"] == "a2_address_2026.gpkg"
    assert row["Title"] == "Address points [Michigan--Ann Arbor] {2026}"
    assert row["Publisher"] == "Ann Arbor Data Catalog"
    assert row["Coordinate Reference System"].endswith("/epsg/2253/")
    assert row["Harvest Workflow"] == "curation_datasets"
    assert "aa_address.geojson" in row["Provenance"]

    manifest = json.loads(job.manifest_path.read_text(encoding="utf-8"))
    record = manifest["records"][0]
    assert manifest["source_platform"] == "ckan"
    assert record["source_id"] == SOURCE_ID
    assert record["package_id"] == PACKAGE_ID
    assert record["resource_id"] == RESOURCE_ID
    assert record["landing_page"] == (
        "https://data.example.org/city-of-ann-arbor/address-points"
    )


def test_inspect_geopackage_infers_unknown_declared_geometry(tmp_path: Path) -> None:
    path = tmp_path / "unknown-geometry.gpkg"
    with fiona.open(
        path,
        "w",
        driver="GPKG",
        layer=path.stem,
        crs="EPSG:4326",
        schema={"geometry": "Unknown", "properties": {"ZONE": "str"}},
    ) as collection:
        collection.write(
            {
                "geometry": {
                    "type": "MultiPolygon",
                    "coordinates": [
                        [
                            [
                                (-83.8, 42.2),
                                (-83.7, 42.2),
                                (-83.7, 42.3),
                                (-83.8, 42.3),
                                (-83.8, 42.2),
                            ]
                        ]
                    ],
                },
                "properties": {"ZONE": "R1"},
            }
        )

    inspection = inspect_geopackage(path)

    assert inspection["geometry_type"] == "MultiPolygon"
    assert inspection["resource_type"] == "Polygon data"
    assert inspection["feature_count"] == 1
