from unittest.mock import patch

import time
import inspect

import pandas as pd

from harvesters.ckan import (
    CkanHarvester,
    ckan_package_search_endpoint,
    write_ckan_harvest_report,
)


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


def test_ckan_harvester_class_only_defines_template_methods() -> None:
    allowed_methods = {
        "__init__",
        "load_reference_data",
        "fetch",
        "parse",
        "flatten",
        "build_dataframe",
        "derive_fields",
        "add_defaults",
        "add_provenance",
        "clean",
        "validate",
        "write_outputs",
        "build_uploads",
        "harvest_pipeline",
    }
    defined_methods = {
        name
        for name, value in CkanHarvester.__dict__.items()
        if inspect.isfunction(value)
    }

    assert defined_methods <= allowed_methods


def test_ckan_harvester_build_uploads_enabled_by_default() -> None:
    harvester = CkanHarvester(
        {
            "base_url": "https://data.example.org",
            "output_primary_csv": "outputs/ckan_primary.csv",
            "output_distributions_csv": "outputs/ckan_distributions.csv",
        }
    )

    assert harvester.config["build_uploads"] is True


def test_ckan_fetch_paginates_and_maps_core_fields() -> None:
    config = {
        "base_url": "https://data.example.org",
        "site_title": "Example CKAN",
        "endpoint_url": "https://data.example.org/api/3/action/package_search",
        "rows": 1,
        "timeout": 30,
        "output_primary_csv": "outputs/ckan_primary.csv",
        "output_distributions_csv": "outputs/ckan_distributions.csv",
    }
    harvester = CkanHarvester(config)

    package_one = {
        "id": "pkg-1",
        "name": "roads",
        "title": "Road Centerlines 2024",
        "notes": "Centerline data",
        "metadata_created": "2024-01-01T10:00:00",
        "metadata_modified": "2024-02-02T11:00:00",
        "license_title": "CC0",
        "license_url": "https://example.com/license",
        "organization": {"title": "City of Pittsburgh"},
        "tags": [{"display_name": "roads"}, {"display_name": "transportation"}],
        "groups": [{"title": "Transportation"}],
        "resources": [
            {"url": "https://data.example.org/download/roads.geojson", "format": "GeoJSON"},
            {"url": "https://data.example.org/arcgis/rest/services/Roads/FeatureServer/0?f=pjson", "format": "FeatureServer"},
            {"url": "https://data.example.org/arcgis/rest/services/Roads/MapServer/2?f=pjson", "format": "Esri REST"},
        ],
        "private": False,
        "spatial": '{"type":"Polygon","coordinates":[[[-89.5,40.0],[-88.0,40.0],[-88.0,41.0],[-89.5,41.0],[-89.5,40.0]]]}',
        "geographic_unit": "Neighborhood",
    }
    package_two = {
        "id": "pkg-2",
        "name": "bridges",
        "title": "Bridge Locations",
        "notes": "Bridge inventory",
        "metadata_created": "2023-05-10T08:00:00",
        "metadata_modified": "2024-03-12T09:00:00",
        "license_title": "CC BY",
        "resources": [],
        "private": True,
        "organization": {"title": "Allegheny County"},
    }

    responses = [
        FakeResponse({"success": True, "result": {"count": 2, "results": [package_one]}}),
        FakeResponse({"success": True, "result": {"count": 2, "results": [package_two]}}),
    ]

    with patch("requests.Session.get", side_effect=responses) as mock_get:
        fetched = list(harvester.fetch())

    records = [item for item in fetched if isinstance(item, dict)]
    assert len(records) == 2
    assert mock_get.call_count == 2
    assert mock_get.call_args_list[0].kwargs["params"] == {"rows": 1, "start": 0}
    assert mock_get.call_args_list[1].kwargs["params"] == {"rows": 1, "start": 1}

    df = harvester.build_dataframe(records)
    df = harvester.derive_fields(df)
    df = harvester.add_defaults(df)
    df = harvester.add_provenance(df)

    first = df.loc[df["ID"] == "pkg-1"].iloc[0]
    second = df.loc[df["ID"] == "pkg-2"].iloc[0]

    assert first["Title"] == "Road Centerlines 2024"
    assert first["Creator"] == "City of Pittsburgh"
    assert first["Keyword"] == "roads|transportation"
    assert first["Subject"] == "Transportation"
    assert first["Access Rights"] == "Public"
    assert first["Spatial Coverage"] == "Pennsylvania--Pittsburgh|Pennsylvania"
    assert first["Bounding Box"] == "-89.5,40.0,-88.0,41.0"
    assert first["information"] == "https://data.example.org/dataset/roads"
    assert first["featureService"] == "https://data.example.org/arcgis/rest/services/Roads/FeatureServer/0?f=pjson"
    assert first["mapService"] == "https://data.example.org/arcgis/rest/services/Roads/MapServer/2?f=pjson"
    assert first["geo_json"] == "https://data.example.org/download/roads.geojson"
    assert first["download"] == []
    assert first["Website Platform"] == "CKAN"
    assert first["Endpoint Description"] == "CKAN API (package_search)"

    assert second["Access Rights"] == "Restricted"
    assert second["Publisher"] == "Example CKAN"
    assert second["Spatial Coverage"] == "Pennsylvania--Allegheny County|Pennsylvania"
    assert second["Bounding Box"] == "-80.36,40.19,-79.69,40.67"


def test_ckan_spatial_coverage_defaults_to_pennsylvania() -> None:
    harvester = CkanHarvester(
        {
            "base_url": "https://data.example.org",
            "site_title": "Example CKAN",
            "output_primary_csv": "outputs/ckan_primary.csv",
            "output_distributions_csv": "outputs/ckan_distributions.csv",
        }
    )

    df = harvester.build_dataframe(
        [
            {
                "id": "pkg-3",
                "name": "stormwater-outfalls",
                "title": "Stormwater Outfalls",
                "notes": "",
                "organization": {"title": "Pennsylvania Department of Environmental Protection"},
                "resources": [],
            }
        ]
    )

    df = harvester.derive_fields(df)

    assert df.loc[df["ID"] == "pkg-3", "Spatial Coverage"].iloc[0] == "Pennsylvania"
    assert df.loc[df["ID"] == "pkg-3", "Bounding Box"].iloc[0] == "-80.36,40.19,-79.69,40.67"


def test_ckan_fetch_uses_workflow_input_sites(tmp_path) -> None:
    workflow_csv = tmp_path / "py-ckan.csv"
    workflow_csv.write_text(
        "\n".join(
            [
                "Title,Code,Identifier,ID,Endpoint URL,Harvest Workflow",
                "Harvest record for Ann Arbor Data Catalog,07c-02,07c-02,harvest_07c-02,https://ckan.a2gov.org/api/3/action/package_list,py_ckan",
            ]
        ),
        encoding="utf-8",
    )
    metadata_csv = tmp_path / "websites.csv"
    metadata_csv.write_text(
        "\n".join(
            [
                "Title,Code,ID,Identifier,Publisher,Spatial Coverage,Bounding Box,Member Of",
                "Ann Arbor Data Catalog,07c-02,07c-02,https://www.a2gov.org/services/data-catalog/,City of Ann Arbor,Michigan--Ann Arbor|Michigan,\"-83.800,42.223,-83.676,42.324\",ba5cc745-21c5-4ae9-954b-72dd8db6815a",
            ]
        ),
        encoding="utf-8",
    )
    config = {
        "input_csv": str(workflow_csv),
        "hub_metadata_csv": str(metadata_csv),
        "rows": 10,
        "timeout": 30,
        "output_primary_csv": "outputs/ckan_primary.csv",
        "output_distributions_csv": "outputs/ckan_distributions.csv",
    }
    harvester = CkanHarvester(config)
    harvester.load_reference_data()

    package = {
        "id": "pkg-1",
        "name": "parcels",
        "title": "Parcels",
        "notes": "Parcel data",
        "organization": {"title": "City of Ann Arbor"},
        "resources": [],
    }

    with patch(
        "requests.Session.get",
        return_value=FakeResponse({"success": True, "result": {"count": 1, "results": [package]}}),
    ) as mock_get:
        fetched = list(harvester.fetch())

    records = [item for item in fetched if isinstance(item, dict)]
    assert len(records) == 1
    assert mock_get.call_args.args[0] == "https://ckan.a2gov.org/api/3/action/package_search"
    assert mock_get.call_args.kwargs["params"] == {"rows": 10, "start": 0}

    df = harvester.build_dataframe(records)
    df = harvester.derive_fields(df)
    df = harvester.add_defaults(df)
    df = harvester.add_provenance(df)

    harvested = df.loc[df["ID"] == "pkg-1"].iloc[0]
    harvest_record = df.loc[df["ID"] == "harvest_07c-02"].iloc[0]

    assert harvested["Publisher"] == "Ann Arbor Data Catalog"
    assert harvested["Spatial Coverage"] == "Michigan--Ann Arbor|Michigan"
    assert harvested["Bounding Box"] == "-83.800,42.223,-83.676,42.324"
    assert harvested["Member Of"] == "ba5cc745-21c5-4ae9-954b-72dd8db6815a"
    assert harvested["Code"] == "07c-02"
    assert harvested["information"] == "https://ckan.a2gov.org/dataset/parcels"
    assert harvested["Endpoint URL"] == "https://ckan.a2gov.org/api/3/action/package_search"
    assert harvested["Harvest Workflow"] == "py_ckan"
    assert harvest_record["Last Harvested"]


def test_ckan_package_search_endpoint_normalizes_package_list_urls() -> None:
    assert (
        ckan_package_search_endpoint("https://example.org/api/3/action/package_list")
        == "https://example.org/api/3/action/package_search"
    )


def test_write_ckan_harvest_report_counts_new_and_unpublished_records(tmp_path) -> None:
    previous_primary = tmp_path / "2026-05-01_ckan_primary.csv"
    current_primary = tmp_path / f"{time.strftime('%Y-%m-%d')}_ckan_primary.csv"

    pd.DataFrame(
        [
            {"ID": "old-only", "Code": "07c-02", "Resource Class": "Datasets"},
            {"ID": "same", "Code": "07c-02", "Resource Class": "Datasets"},
        ]
    ).to_csv(previous_primary, index=False)
    pd.DataFrame(
        [
            {"ID": "same", "Code": "07c-02", "Resource Class": "Datasets"},
            {"ID": "new-only", "Code": "07c-02", "Resource Class": "Datasets"},
            {"ID": "harvest_07c-02", "Code": "07c-02", "Resource Class": "Series"},
        ]
    ).to_csv(current_primary, index=False)

    report_path = write_ckan_harvest_report(
        [
            {
                "Code": "07c-02",
                "Title": "Ann Arbor Data Catalog",
                "Identifier": "07c-02",
                "Harvest Run": "success",
                "Harvest Message": "[CKAN] Fetched 2 package records from 07c-02.",
                "Total Records Found": 2,
            }
        ],
        str(current_primary),
        "outputs/ckan_primary.csv",
        str(tmp_path / "reports" / "ckan_report.csv"),
    )

    report_df = pd.read_csv(report_path, dtype=str).fillna("")
    site_row = report_df.loc[report_df["Code"] == "07c-02"].iloc[0]
    total_row = report_df.loc[report_df["Code"] == "TOTAL"].iloc[0]

    assert site_row["Total Records Found"] == "2"
    assert site_row["New Records"] == "1"
    assert site_row["Unpublished Records"] == "1"
    assert total_row["Harvest Run"] == "success: 1; error: 0"
