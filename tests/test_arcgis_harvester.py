from pathlib import Path
from datetime import date
import time
from unittest.mock import patch

import pandas as pd
import pytest

from harvesters.arcgis import (
    ArcGISHarvester,
    arcgis_clean_creator_values,
    build_arcgis_uploads_from_registry,
    write_arcgis_harvest_report,
)


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_CSV = ROOT / "tests" / "fixtures" / "arcgis_test_hubs.csv"


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


def _config() -> dict:
    return {
        "input_csv": str(FIXTURE_CSV),
        "hub_metadata_csv": "reference_data/websites.csv",
        "output_primary_csv": "outputs/arcgis_primary.csv",
        "output_distributions_csv": "outputs/arcgis_distributions.csv",
        "output_report_csv": "outputs/arcgis_report.csv",
    }


def test_arcgis_harvester_processes_fixture_hubs_and_appends_harvest_record_rows() -> None:
    harvester = ArcGISHarvester(_config())

    payload_by_url = {
        "https://opendata.minneapolismn.gov/api/feed/dcat-us/1.1.json": {
            "dataset": [
                {
                    "title": "Street Centerlines 2024",
                    "description": "Centerline features for Minneapolis streets.",
                    "publisher": {"name": "City of Minneapolis"},
                    "keyword": ["roads", "transportation"],
                    "issued": "2024-01-10T00:00:00",
                    "modified": "2024-05-01T12:00:00",
                    "license": "Open Data",
                    "identifier": "https://hub.arcgis.com/datasets?id=roads123&sublayer=2",
                    "landingPage": "https://example.org/datasets/roads123",
                    "spatial": "-93.30,44.90,-93.20,45.00",
                    "distribution": [
                        {
                            "title": "Shapefile",
                            "accessURL": "https://downloads.example.org/roads.zip",
                        },
                        {
                            "title": "ArcGIS GeoService",
                            "accessURL": "https://services.example.org/arcgis/rest/services/Roads/FeatureServer/2",
                        },
                    ],
                }
            ]
        },
        "https://holmes-county-gis-holmesgis.hub.arcgis.com/api/feed/dcat-us/1.1.json": {
            "dataset": [
                {
                    "title": "County Imagery 2019",
                    "description": "Current county imagery tiles.",
                    "publisher": "Holmes County GIS",
                    "keyword": ["imagery"],
                    "issued": "",
                    "modified": "2023-02-02T09:30:00",
                    "license": "Public Domain",
                    "identifier": "https://hub.arcgis.com/datasets?id=imagery999",
                    "landingPage": "https://example.org/datasets/imagery999",
                    "spatial": "-81.90,40.50,-81.90,40.60",
                    "distribution": [
                        {
                            "title": "ArcGIS GeoService",
                            "accessURL": "https://services.example.org/arcgis/rest/services/Imagery/ImageServer",
                        }
                    ],
                }
            ]
        },
        "https://gisdata-jeffcowa.opendata.arcgis.com/api/feed/dcat-us/1.1.json": {
            "dataset": [
                {
                    "title": "Parcels",
                    "description": "County parcel boundaries.",
                    "publisher": "Jefferson County Assessor",
                    "keyword": ["parcel", "polygon"],
                    "issued": "",
                    "modified": "2022-07-15T00:00:00",
                    "license": "Open",
                    "identifier": "https://hub.arcgis.com/datasets?id=parcel456",
                    "landingPage": "https://example.org/datasets/parcel456",
                    "spatial": "-122.600,48.370,-124.720,47.520",
                    "distribution": [
                        {
                            "title": "Shapefile",
                            "accessURL": "https://downloads.example.org/parcels.zip",
                        },
                        {
                            "title": "ArcGIS GeoService",
                            "accessURL": "https://services.example.org/arcgis/rest/services/Parcels/MapServer",
                        },
                    ],
                }
            ]
        },
    }

    def fake_get(url, timeout):
        assert timeout == 30
        return FakeResponse(payload_by_url[url])

    with patch("requests.get", side_effect=fake_get) as mock_get:
        fetched = list(harvester.fetch())

    status_messages = [item for item in fetched if isinstance(item, str)]
    fetched_rows = [item for item in fetched if isinstance(item, dict)]

    assert len(status_messages) == 3
    assert len(fetched_rows) == 3
    assert mock_get.call_count == 3

    df = harvester.flatten(fetched)
    df = harvester.build_dataframe(df)
    df = harvester.derive_fields(df)
    df = harvester.add_defaults(df)
    df = harvester.add_provenance(df)
    df = harvester.clean(df)
    df = harvester.validate(df)

    street_row = df.loc[df["ID"] == "roads123_2"].iloc[0]
    imagery_row = df.loc[df["ID"] == "imagery999"].iloc[0]
    parcels_row = df.loc[df["ID"] == "parcel456"].iloc[0]

    assert street_row["Title"] == "Street Centerlines 2024 [Minnesota--Minneapolis]"
    assert street_row["Identifier"] == "https://hub.arcgis.com/datasets/roads123_2"
    assert street_row["Creator"] == "City of Minneapolis"
    assert street_row["Date Range"] == "2024-2024"
    assert street_row["featureService"] == "https://services.example.org/arcgis/rest/services/Roads/FeatureServer/2"
    assert street_row["Format"] == "ArcGIS FeatureLayer"
    assert street_row["Bounding Box"] == "-93.300,44.900,-93.200,45.000"

    assert imagery_row["Title"] == "County Imagery 2019 [Ohio--Holmes County]"
    assert imagery_row["Identifier"] == "https://hub.arcgis.com/datasets/imagery999"
    assert imagery_row["Date Range"] == "2019-2019"
    assert imagery_row["imageService"] == "https://services.example.org/arcgis/rest/services/Imagery/ImageServer"
    assert imagery_row["Format"] == "ArcGIS ImageMapLayer"
    assert imagery_row["Bounding Box"] == "-82.221,40.444,-81.649,40.668"

    assert parcels_row["Title"] == "Parcels [Washington (State)--Jefferson County]"
    assert parcels_row["Date Range"] == "2022-2022"
    assert parcels_row["mapService"] == "https://services.example.org/arcgis/rest/services/Parcels/MapServer"
    assert parcels_row["Format"] == "ArcGIS DynamicMapLayer"
    assert parcels_row["Bounding Box"] == "-124.720,47.520,-122.600,48.370"
    assert parcels_row["Resource Type"] == "Polygon data"

    harvest_record_rows = df.loc[df["Resource Class"] == "Websites"].copy()
    today = time.strftime("%Y-%m-%d")

    assert len(harvest_record_rows) == 3
    assert set(harvest_record_rows["ID"]) == {"05c-01", "11b-39075", "16b-53031"}
    assert set(harvest_record_rows["Last Harvested"]) == {today}


def test_arcgis_harvester_enables_build_uploads_by_default() -> None:
    harvester = ArcGISHarvester(_config())
    assert harvester.config["build_uploads"] is True


def test_arcgis_harvester_allows_build_uploads_to_be_disabled() -> None:
    config = _config()
    config["build_uploads"] = False

    harvester = ArcGISHarvester(config)
    assert harvester.config["build_uploads"] is False


def test_arcgis_registry_uploads_preserve_accessioned_dates_and_prune_state(tmp_path) -> None:
    outputs_dir = tmp_path / "outputs"
    registry_dir = tmp_path / "registry"
    outputs_dir.mkdir()
    registry_dir.mkdir()
    current_primary_path = outputs_dir / "2026-07-15_arcgis_primary.csv"
    current_distributions_path = outputs_dir / "2026-07-15_arcgis_distributions.csv"
    primary_registry_path = registry_dir / "arcgis_primary_registry.csv"
    distributions_registry_path = registry_dir / "arcgis_distributions_registry.csv"
    today = date.today().isoformat()

    pd.DataFrame(
        [
            {
                "Title": "Shared Old Title",
                "Alternative Title": "Shared",
                "Creator": "Shared Creator",
                "Publisher": "Shared Publisher",
                "Resource Class": "Web services",
                "Temporal Coverage": "2020",
                "Date Issued": "2020-01-01",
                "Date Accessioned": "2026-01-08",
                "ID": "shared-id",
                "Identifier": "https://example.org/shared",
                "Code": "05c-01",
                "last_seen": "2026-07-13",
            },
            {
                "Title": "Retired Title",
                "Alternative Title": "Retired",
                "Creator": "Retired Creator",
                "Publisher": "Retired Publisher",
                "Resource Class": "Web services",
                "Temporal Coverage": "2019",
                "Date Issued": "2019-01-01",
                "Date Accessioned": "2026-01-09",
                "ID": "retired-id",
                "Identifier": "https://example.org/retired",
                "Code": "05c-01",
                "last_seen": "2026-07-13",
            },
        ]
    ).to_csv(primary_registry_path, index=False)
    pd.DataFrame(
        [
            {
                "friendlier_id": "shared-id",
                "reference_type": "download",
                "distribution_url": "https://example.org/old.zip",
                "label": "ZIP",
                "last_seen": "2026-07-13",
            },
            {
                "friendlier_id": "retired-id",
                "reference_type": "download",
                "distribution_url": "https://example.org/retired.zip",
                "label": "ZIP",
                "last_seen": "2026-07-13",
            },
        ]
    ).to_csv(distributions_registry_path, index=False)

    pd.DataFrame(
        [
            {
                "Title": "Harvest record for Site",
                "Resource Class": "Websites",
                "Publication State": "published",
                "Access Rights": "Public",
                "Date Accessioned": "2026-07-15",
                "ID": "05c-01",
                "Identifier": "https://example.org/site",
                "Code": "05c-01",
            },
            {
                "Title": "Shared New Title",
                "Alternative Title": "Shared",
                "Creator": "Shared Creator",
                "Publisher": "Shared Publisher",
                "Resource Class": "Web services",
                "Temporal Coverage": "2021",
                "Date Issued": "2021-01-01",
                "Publication State": "published",
                "Access Rights": "Public",
                "Date Accessioned": "2026-07-15",
                "ID": "shared-id",
                "Identifier": "https://example.org/shared",
                "Code": "05c-01",
            },
            {
                "Title": "New Title",
                "Alternative Title": "New",
                "Creator": "New Creator",
                "Publisher": "New Publisher",
                "Resource Class": "Web services",
                "Temporal Coverage": "2022",
                "Date Issued": "2022-01-01",
                "Publication State": "published",
                "Access Rights": "Public",
                "Date Accessioned": "2026-07-15",
                "ID": "new-id",
                "Identifier": "https://example.org/new",
                "Code": "05c-01",
            },
        ]
    ).to_csv(current_primary_path, index=False)
    pd.DataFrame(
        [
            {
                "friendlier_id": "shared-id",
                "reference_type": "download",
                "distribution_url": "https://example.org/new.zip",
                "label": "ZIP",
            },
            {
                "friendlier_id": "new-id",
                "reference_type": "download",
                "distribution_url": "https://example.org/new-record.zip",
                "label": "ZIP",
            },
        ]
    ).to_csv(current_distributions_path, index=False)

    summary = build_arcgis_uploads_from_registry(
        {
            "primary_csv": str(current_primary_path),
            "distributions_csv": str(current_distributions_path),
        },
        {
            "build_uploads": True,
            "output_primary_csv": "outputs/arcgis_primary.csv",
            "primary_registry_csv": str(primary_registry_path),
            "distributions_registry_csv": str(distributions_registry_path),
        },
    )

    assert summary["status"] == "created"
    assert summary["new_count"] == 1
    assert summary["retired_count"] == 1
    assert summary["distribution_new_count"] == 2
    assert summary["distribution_delete_count"] == 1
    assert summary["changed_distribution_ids"] == ["shared-id"]

    primary_upload = pd.read_csv(
        summary["primary_upload_csv"],
        dtype=str,
        keep_default_na=False,
    ).fillna("")
    assert set(primary_upload["ID"]) == {"05c-01", "new-id", "retired-id"}
    retired_row = primary_upload.loc[primary_upload["ID"] == "retired-id"].iloc[0]
    assert retired_row["Title"] == "Retired Title"
    assert retired_row["Date Accessioned"] == "2026-01-09"
    assert retired_row["Publication State"] == "unpublished"
    assert retired_row["Date Retired"] == today
    assert retired_row["Resource Class"] == "Web services"
    assert retired_row["Access Rights"] == "Public"

    updated_registry = pd.read_csv(
        primary_registry_path,
        dtype=str,
        keep_default_na=False,
    ).fillna("")
    assert set(updated_registry["ID"]) == {"shared-id", "new-id"}
    assert "last_seen" not in updated_registry.columns
    assert "registry_status" not in updated_registry.columns
    assert "Date Retired" not in updated_registry.columns
    shared_registry_row = updated_registry.loc[updated_registry["ID"] == "shared-id"].iloc[0]
    new_registry_row = updated_registry.loc[updated_registry["ID"] == "new-id"].iloc[0]
    assert shared_registry_row["Title"] == "Shared New Title"
    assert shared_registry_row["Date Accessioned"] == "2026-01-08"
    assert new_registry_row["Date Accessioned"] == "2026-07-15"

    updated_distributions_registry = pd.read_csv(
        distributions_registry_path,
        dtype=str,
        keep_default_na=False,
    ).fillna("")
    assert "last_seen" not in updated_distributions_registry.columns
    assert set(updated_distributions_registry["friendlier_id"]) == {"shared-id", "new-id"}
    assert set(updated_distributions_registry["distribution_url"]) == {
        "https://example.org/new.zip",
        "https://example.org/new-record.zip",
    }


def test_arcgis_harvester_writes_harvest_report_with_counts(tmp_path, monkeypatch) -> None:
    outputs_dir = tmp_path / "outputs"
    outputs_dir.mkdir()
    old_primary = outputs_dir / "2026-04-01_arcgis_primary.csv"
    current_primary = outputs_dir / "2026-04-25_arcgis_primary.csv"

    pd.DataFrame(
        [
            {"ID": "old-shared", "Is Part Of": "05c-01", "Resource Class": "Web services"},
            {"ID": "old-retired", "Is Part Of": "05c-01", "Resource Class": "Web services"},
            {"ID": "harvest_05c-01", "Is Part Of": "", "Resource Class": "Websites"},
        ]
    ).to_csv(old_primary, index=False)
    pd.DataFrame(
        [
            {"ID": "old-shared", "Is Part Of": "05c-01", "Resource Class": "Web services"},
            {"ID": "new-road", "Is Part Of": "05c-01", "Resource Class": "Web services"},
            {"ID": "new-imagery", "Is Part Of": "11b-39075", "Resource Class": "Web services"},
            {"ID": "harvest_05c-01", "Is Part Of": "", "Resource Class": "Websites"},
        ]
    ).to_csv(current_primary, index=False)

    monkeypatch.chdir(tmp_path)
    report_path = write_arcgis_harvest_report(
        [
            {
                "Code": "05c-01",
                "Title": "Harvest record one",
                "Identifier": "https://example.org/one",
                "Harvest Run": "success",
                "Harvest Message": "Fetched 05c-01",
                "Total Records Found": 2,
            },
            {
                "Code": "11b-39075",
                "Title": "Harvest record two",
                "Identifier": "https://example.org/two",
                "Harvest Run": "error",
                "Harvest Message": "Fetch failed",
                "Total Records Found": 0,
            },
        ],
        str(current_primary),
        "outputs/arcgis_primary.csv",
        "outputs/arcgis_report.csv",
    )

    report_df = pd.read_csv(report_path, dtype=str).fillna("")
    first_row = report_df.loc[report_df["Code"] == "05c-01"].iloc[0]
    second_row = report_df.loc[report_df["Code"] == "11b-39075"].iloc[0]
    total_row = report_df.loc[report_df["Code"] == "TOTAL"].iloc[0]

    assert first_row["New Records"] == "1"
    assert first_row["Unpublished Records"] == "1"
    assert second_row["New Records"] == "1"
    assert second_row["Unpublished Records"] == "0"
    assert total_row["Harvest Run"] == "success: 1; error: 1"
    assert total_row["Total Records Found"] == "2"
    assert total_row["New Records"] == "2"
    assert total_row["Unpublished Records"] == "1"


def test_arcgis_harvester_omits_created_and_updated_at_from_primary_output(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    harvester = ArcGISHarvester(
        {
            "input_csv": str(FIXTURE_CSV),
            "hub_metadata_csv": str(ROOT / "reference_data" / "websites.csv"),
            "output_primary_csv": "outputs/arcgis_primary.csv",
            "output_distributions_csv": "outputs/arcgis_distributions.csv",
            "output_report_csv": "outputs/arcgis_report.csv",
            "build_uploads": False,
        }
    )
    harvester.distribution_types = []
    primary_df = pd.DataFrame(
        [
            {
                "ID": "sample-id",
                "Title": "Sample",
                "Access Rights": "Public",
                "Resource Class": "Web services",
                "Created At": "2026-01-01",
                "Updated At": "2026-01-02",
            }
        ]
    )

    results = harvester.write_outputs(primary_df)
    written_df = pd.read_csv(results["primary_csv"], dtype=str).fillna("")

    assert "Created At" not in written_df.columns
    assert "Updated At" not in written_df.columns


def test_arcgis_clean_creator_values_blanks_template_placeholders() -> None:
    df = pd.DataFrame({"Creator": ["{{source}}", "{{ Source }}", "City GIS"]})

    cleaned_df = arcgis_clean_creator_values(df)

    assert cleaned_df["Creator"].tolist() == ["", "", "City GIS"]


def test_arcgis_harvester_reads_workflow_inputs_and_metadata_defaults(tmp_path) -> None:
    workflow_csv = tmp_path / "py-arcgis-hub.csv"
    metadata_csv = tmp_path / "arcHub_metadata.csv"

    workflow_csv.write_text(
        "\n".join(
            [
                "Title,Provenance,Publication State,Website Platform,Endpoint URL,Endpoint Description,Is Harvested,Last Harvested,Accrual Method,Accrual Periodicity,Harvest Workflow,Resource Class,Resource Type,Access Rights,ID,Identifier,Code,Tags,Admin Note",
                "Harvest record for Open Data Minneapolis,2026-03-28 / harvest,published,ArcGIS Hub,https://opendata.minneapolismn.gov/api/feed/dcat-us/1.1.json,DCAT API,,2026-03-28,Automated retrieval,Weekly,py_arcgis_hub,Websites,Data portals,Public,harvest_05c-01,05c-01,05c-01,,",
                "Harvest record for Holmes County GIS Open Data Portal,2026-03-22 / harvest,published,ArcGIS Hub,https://holmes-county-gis-holmesgis.hub.arcgis.com/api/feed/dcat-us/1.1.json,DCAT API,,2026-03-22,Automated retrieval,Weekly,py_arcgis_hub,Websites,Data portals,Public,harvest_11b-39075,11b-39075,11b-39075,,",
                "Harvest record for Jefferson County Washington Open Data Site,2026-03-28 / harvest,published,ArcGIS Hub,https://gisdata-jeffcowa.opendata.arcgis.com/api/feed/dcat-us/1.1.json,DCAT API,,2026-03-28,Automated retrieval,Weekly,py_arcgis_hub,Websites,Data portals,Public,harvest_16b-53031,16b-53031,16b-53031,,",
            ]
        ),
        encoding="utf-8",
    )
    metadata_csv.write_text(
        "\n".join(
            [
                "Title,Creator,Publisher,Provider,Subject,Spatial Coverage,Bounding Box,Member Of,ID,Identifier,Code",
                'Open Data Minneapolis,,Minnesota--Minneapolis,University of Minnesota,Municipal government records,Minnesota--Minneapolis|Minnesota,"-93.329,44.890,-93.194,45.051",ba5cc745-21c5-4ae9-954b-72dd8db6815a,05c-01,https://opendata.minneapolismn.gov,05c-01',
                'Holmes County GIS Open Data Portal,,Ohio--Holmes County,,County government records,Ohio--Holmes County|Ohio,"-82.221,40.444,-81.649,40.668",ba5cc745-21c5-4ae9-954b-72dd8db6815a,11b-39075,https://holmes-county-gis-holmesgis.hub.arcgis.com,11b-39075',
                'Jefferson County Washington Open Data Site,,Washington (State)--Jefferson County,,County government records,Washington (State)--Jefferson County|Washington (State),"-124.720,47.520,-122.600,48.370",ba5cc745-21c5-4ae9-954b-72dd8db6815a,16b-53031,https://gisdata-jeffcowa.opendata.arcgis.com,16b-53031',
            ]
        ),
        encoding="utf-8",
    )

    harvester = ArcGISHarvester(
        {
            "input_csv": str(workflow_csv),
            "hub_metadata_csv": str(metadata_csv),
            "output_primary_csv": "outputs/arcgis_primary.csv",
            "output_distributions_csv": "outputs/arcgis_distributions.csv",
            "output_report_csv": "outputs/arcgis_report.csv",
        }
    )

    payload_by_url = {
        "https://opendata.minneapolismn.gov/api/feed/dcat-us/1.1.json": {
            "dataset": [
                {
                    "title": "Street Centerlines 2024",
                    "description": "Centerline features for Minneapolis streets.",
                    "publisher": {"name": "City of Minneapolis"},
                    "keyword": ["roads", "transportation"],
                    "issued": "2024-01-10T00:00:00",
                    "modified": "2024-05-01T12:00:00",
                    "license": "Open Data",
                    "identifier": "https://hub.arcgis.com/datasets?id=roads123&sublayer=2",
                    "landingPage": "https://example.org/datasets/roads123",
                    "spatial": "-93.30,44.90,-93.20,45.00",
                    "distribution": [
                        {
                            "title": "Shapefile",
                            "accessURL": "https://downloads.example.org/roads.zip",
                        },
                        {
                            "title": "ArcGIS GeoService",
                            "accessURL": "https://services.example.org/arcgis/rest/services/Roads/FeatureServer/2",
                        },
                    ],
                }
            ]
        },
        "https://holmes-county-gis-holmesgis.hub.arcgis.com/api/feed/dcat-us/1.1.json": {
            "dataset": [
                {
                    "title": "County Imagery 2019",
                    "description": "Current county imagery tiles.",
                    "publisher": "Holmes County GIS",
                    "keyword": ["imagery"],
                    "issued": "",
                    "modified": "2023-02-02T09:30:00",
                    "license": "Public Domain",
                    "identifier": "https://hub.arcgis.com/datasets?id=imagery999",
                    "landingPage": "https://example.org/datasets/imagery999",
                    "spatial": "-81.90,40.50,-81.90,40.60",
                    "distribution": [
                        {
                            "title": "ArcGIS GeoService",
                            "accessURL": "https://services.example.org/arcgis/rest/services/Imagery/ImageServer",
                        }
                    ],
                }
            ]
        },
        "https://gisdata-jeffcowa.opendata.arcgis.com/api/feed/dcat-us/1.1.json": {
            "dataset": [
                {
                    "title": "Parcels",
                    "description": "County parcel boundaries.",
                    "publisher": "Jefferson County Assessor",
                    "keyword": ["parcel", "polygon"],
                    "issued": "",
                    "modified": "2022-07-15T00:00:00",
                    "license": "Open",
                    "identifier": "https://hub.arcgis.com/datasets?id=parcel456",
                    "landingPage": "https://example.org/datasets/parcel456",
                    "spatial": "-122.600,48.370,-124.720,47.520",
                    "distribution": [
                        {
                            "title": "Shapefile",
                            "accessURL": "https://downloads.example.org/parcels.zip",
                        },
                        {
                            "title": "ArcGIS GeoService",
                            "accessURL": "https://services.example.org/arcgis/rest/services/Parcels/MapServer",
                        },
                    ],
                }
            ]
        },
    }

    def fake_get(url, timeout):
        assert timeout == 30
        return FakeResponse(payload_by_url[url])

    with patch("requests.get", side_effect=fake_get):
        fetched = list(harvester.fetch())

    df = harvester.flatten(fetched)
    df = harvester.build_dataframe(df)
    df = harvester.derive_fields(df)
    df = harvester.add_defaults(df)
    df = harvester.add_provenance(df)
    df = harvester.clean(df)
    df = harvester.validate(df)

    street_row = df.loc[df["ID"] == "roads123_2"].iloc[0]
    minneapolis_record = df.loc[df["ID"] == "harvest_05c-01"].iloc[0]
    today = time.strftime("%Y-%m-%d")

    assert street_row["Is Part Of"] == "05c-01"
    assert street_row["Code"] == "05c-01"
    assert street_row["Publisher"] == "Open Data Minneapolis"
    assert street_row["Provider"] == "University of Minnesota"
    assert street_row["Endpoint Description"] == "DCAT API"
    assert street_row["Accrual Periodicity"] == "Weekly"
    assert street_row["Provenance"] == (
        f"The metadata for this resource was last retrieved from "
        f"Open Data Minneapolis ArcGIS Hub on {today}."
    )

    assert minneapolis_record["Title"] == "Harvest record for Open Data Minneapolis"
    assert minneapolis_record["Resource Class"] == "Websites"
    assert minneapolis_record["Resource Type"] == "Data portals"
    assert minneapolis_record["Endpoint URL"] == "https://opendata.minneapolismn.gov/api/feed/dcat-us/1.1.json"
    assert minneapolis_record["Provenance"] == "2026-03-28 / harvest"
    assert minneapolis_record["Harvest Workflow"] == "py_arcgis_hub"
    assert minneapolis_record["Last Harvested"] == today


def test_arcgis_harvester_rejects_duplicate_endpoints_for_different_codes(tmp_path) -> None:
    workflow_csv = tmp_path / "py-arcgis-hub.csv"
    metadata_csv = tmp_path / "arcHub_metadata.csv"

    workflow_csv.write_text(
        "\n".join(
            [
                "Title,Endpoint URL,ID,Identifier,Code",
                "Harvest record for One,https://example.org/api/feed/dcat-us/1.1.json,harvest_01,01,01",
                "Harvest record for Two,https://example.org/api/feed/dcat-us/1.1.json,harvest_02,02,02",
            ]
        ),
        encoding="utf-8",
    )
    metadata_csv.write_text("Title,ID,Identifier,Code\n", encoding="utf-8")

    harvester = ArcGISHarvester(
        {
            "input_csv": str(workflow_csv),
            "hub_metadata_csv": str(metadata_csv),
            "output_primary_csv": "outputs/arcgis_primary.csv",
            "output_distributions_csv": "outputs/arcgis_distributions.csv",
            "output_report_csv": "outputs/arcgis_report.csv",
        }
    )

    with patch("requests.get") as mock_get:
        with pytest.raises(ValueError, match="Duplicate Endpoint URL"):
            list(harvester.fetch())

    mock_get.assert_not_called()
