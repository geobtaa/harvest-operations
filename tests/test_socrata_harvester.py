from pathlib import Path
import time
from unittest.mock import patch

import pandas as pd

from harvesters.socrata import SocrataHarvester
from harvesters.socrata import (
    build_socrata_harvest_report_dataframe,
    filter_valid_socrata_geojson_distributions,
    keep_socrata_harvest_records_in_primary_upload,
)


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


def test_socrata_uses_workflow_lifecycle_fields_and_website_defaults(tmp_path: Path) -> None:
    workflow_csv = tmp_path / "py-socrata.csv"
    metadata_csv = tmp_path / "websites.csv"

    pd.DataFrame(
        [
            {
                "Title": "Harvest record for Example Socrata",
                "Resource Class": "Series",
                "Publication State": "draft",
                "Website Platform": "Socrata Workflow",
                "Endpoint URL": "https://data.example.org/data.json",
                "Endpoint Description": "Workflow DCAT API",
                "Is Harvested": "true",
                "Last Harvested": "2026-01-01",
                "Accrual Method": "Workflow retrieval",
                "Accrual Periodicity": "Daily",
                "Harvest Workflow": "py_socrata",
                "Access Rights": "Public",
                "Suppressed Record": "true",
                "ID": "harvest_site-1",
                "Identifier": "site-1",
                "Code": "site-1",
            }
        ]
    ).to_csv(workflow_csv, index=False)

    pd.DataFrame(
        [
            {
                "Title": "Example Socrata Portal",
                "Provider": "Example Provider",
                "Spatial Coverage": "Example County|Example State",
                "Bounding Box": "-10.000,20.000,-9.000,21.000",
                "Member Of": "example-member",
                "ID": "site-1",
                "Identifier": "https://data.example.org",
                "Code": "site-1",
            }
        ]
    ).to_csv(metadata_csv, index=False)

    config = {
        "input_csv": str(workflow_csv),
        "hub_metadata_csv": str(metadata_csv),
        "output_primary_csv": "outputs/socrata_primary.csv",
        "output_distributions_csv": "outputs/socrata_distributions.csv",
    }
    harvester = SocrataHarvester(config)
    assert harvester.config["build_uploads"] is True

    payload = {
        "dataset": [
            {
                "title": "Parcels 2024",
                "description": "GIS polygon parcels.",
                "publisher": {"name": "Dataset Creator"},
                "keyword": ["GIS", "polygon"],
                "theme": ["GIS/Maps"],
                "issued": "2024-01-01T00:00:00",
                "modified": "2024-02-02T00:00:00",
                "license": "Open",
                "identifier": "https://data.example.org/views/abcd-1234",
                "landingPage": "https://data.example.org/d/abcd-1234",
            }
        ]
    }

    with patch("requests.get", return_value=FakeResponse(payload)) as mock_get:
        fetched = list(harvester.fetch())

    assert mock_get.call_args.args == ("https://data.example.org/data.json",)
    assert mock_get.call_args.kwargs == {"timeout": 30}

    df = harvester.flatten(fetched)
    df = harvester.build_dataframe(df)
    df = harvester.derive_fields(df)
    df = harvester.add_defaults(df)
    df = harvester.add_provenance(df)

    dataset_row = df.loc[df["ID"] == "abcd-1234"].iloc[0]
    assert dataset_row["Title"] == "Parcels 2024 [Example County]"
    assert dataset_row["Publisher"] == "Example Socrata Portal"
    assert dataset_row["Provider"] == "Example Provider"
    assert dataset_row["Spatial Coverage"] == "Example County|Example State"
    assert dataset_row["Bounding Box"] == "-10.000,20.000,-9.000,21.000"
    assert dataset_row["Member Of"] == "example-member"
    assert dataset_row["Is Part Of"] == "site-1"
    assert dataset_row["Code"] == "site-1"
    assert dataset_row["Website Platform"] == "Socrata Workflow"
    assert dataset_row["Endpoint URL"] == "https://data.example.org/data.json"
    assert dataset_row["Endpoint Description"] == "Workflow DCAT API"
    assert dataset_row["Accrual Method"] == "Workflow retrieval"
    assert dataset_row["Accrual Periodicity"] == "Daily"
    assert dataset_row["Harvest Workflow"] == "py_socrata"

    harvest_record_row = df.loc[df["ID"] == "harvest_site-1"].iloc[0]
    assert harvest_record_row["Resource Class"] == "Series"
    assert harvest_record_row["Last Harvested"] == time.strftime("%Y-%m-%d")


def test_socrata_filters_invalid_geojson_distributions() -> None:
    distributions_df = pd.DataFrame(
        [
            {
                "friendlier_id": "valid",
                "reference_type": "geo_json",
                "distribution_url": "https://example.org/valid.geojson",
                "label": "",
            },
            {
                "friendlier_id": "invalid",
                "reference_type": "geo_json",
                "distribution_url": "https://example.org/invalid.geojson",
                "label": "",
            },
            {
                "friendlier_id": "doc",
                "reference_type": "documentation_external",
                "distribution_url": "https://example.org/doc",
                "label": "",
            },
        ]
    )

    with patch(
        "harvesters.socrata.socrata_check_geojson",
        side_effect=lambda url: url.endswith("/valid.geojson"),
    ):
        filtered_df = filter_valid_socrata_geojson_distributions(distributions_df)

    assert filtered_df["friendlier_id"].tolist() == ["valid", "doc"]


def test_socrata_write_outputs_writes_cleaned_distributions(tmp_path: Path, monkeypatch) -> None:
    workflow_csv = tmp_path / "py-socrata.csv"
    metadata_csv = tmp_path / "websites.csv"
    workflow_csv.write_text("ID\n", encoding="utf-8")
    metadata_csv.write_text("ID\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    harvester = SocrataHarvester(
        {
            "input_csv": str(workflow_csv),
            "hub_metadata_csv": str(metadata_csv),
            "output_primary_csv": "outputs/socrata_primary.csv",
            "output_distributions_csv": "outputs/socrata_distributions.csv",
            "output_report_csv": "reports/socrata/socrata_report.csv",
        }
    )
    harvester.distribution_types = [
        {"key": "geo_json", "variables": ["geo_json"]},
        {"key": "documentation_external", "variables": ["information"]},
    ]
    primary_df = pd.DataFrame(
        [
            {
                "ID": "valid",
                "Title": "Valid GeoJSON",
                "geo_json": "https://example.org/valid.geojson",
                "information": "https://example.org/valid",
            },
            {
                "ID": "invalid",
                "Title": "Invalid GeoJSON",
                "geo_json": "https://example.org/invalid.geojson",
                "information": "https://example.org/invalid",
            },
        ]
    )

    with patch(
        "harvesters.socrata.socrata_check_geojson",
        side_effect=lambda url: url.endswith("/valid.geojson"),
    ):
        results = harvester.write_outputs(primary_df)

    distributions_df = pd.read_csv(results["distributions_csv"], dtype=str).fillna("")
    geojson_rows = distributions_df.loc[distributions_df["reference_type"] == "geo_json"]
    doc_rows = distributions_df.loc[distributions_df["reference_type"] == "documentation_external"]

    assert geojson_rows["friendlier_id"].tolist() == ["valid"]
    assert set(doc_rows["friendlier_id"]) == {"valid", "invalid"}
    assert results["report_csv"] == f"reports/socrata/{time.strftime('%Y-%m-%d')}_socrata_report.csv"
    assert Path(results["report_csv"]).exists()


def test_socrata_harvest_report_counts_new_and_unpublished_records(tmp_path: Path) -> None:
    previous_primary = tmp_path / "2026-04-01_socrata_primary.csv"
    current_primary = tmp_path / "2026-05-01_socrata_primary.csv"

    pd.DataFrame(
        [
            {"ID": "shared", "Is Part Of": "site-1", "Resource Class": "Web services"},
            {"ID": "retired", "Is Part Of": "site-1", "Resource Class": "Web services"},
            {"ID": "harvest_site-1", "Is Part Of": "", "Resource Class": "Series"},
        ]
    ).to_csv(previous_primary, index=False)
    pd.DataFrame(
        [
            {"ID": "shared", "Is Part Of": "site-1", "Resource Class": "Web services"},
            {"ID": "new", "Is Part Of": "site-1", "Resource Class": "Web services"},
            {"ID": "harvest_site-1", "Is Part Of": "", "Resource Class": "Series"},
        ]
    ).to_csv(current_primary, index=False)

    report_df = build_socrata_harvest_report_dataframe(
        [
            {
                "Code": "site-1",
                "Title": "Harvest record for Example Socrata",
                "Identifier": "site-1",
                "Harvest Run": "success",
                "Harvest Message": "[Socrata] Fetched site-1",
                "Total Records Found": 2,
            }
        ],
        current_primary,
        "outputs/socrata_primary.csv",
    )

    site_row = report_df.loc[report_df["Code"] == "site-1"].iloc[0]
    total_row = report_df.loc[report_df["Code"] == "TOTAL"].iloc[0]

    assert site_row["New Records"] == 1
    assert site_row["Unpublished Records"] == 1
    assert total_row["Harvest Run"] == "success: 1; error: 0"


def test_socrata_primary_upload_filter_keeps_only_harvest_records(tmp_path: Path) -> None:
    upload_path = tmp_path / "2026-05-05_socrata_primary_upload.csv"
    pd.DataFrame(
        [
            {
                "ID": "harvest_site-1",
                "Title": "Harvest record for Site 1",
                "Last Harvested": "2026-05-05",
            },
            {
                "ID": "new-dataset",
                "Title": "New dataset",
                "Last Harvested": "2026-05-05",
            },
            {
                "ID": "retired-dataset",
                "Title": "Retired dataset",
                "Publication State": "unpublished",
            },
        ]
    ).to_csv(upload_path, index=False)

    kept_count = keep_socrata_harvest_records_in_primary_upload(str(upload_path))

    upload_df = pd.read_csv(upload_path, dtype=str, keep_default_na=False).fillna("")
    assert kept_count == 1
    assert upload_df["ID"].tolist() == ["harvest_site-1"]
    assert upload_df.loc[0, "Last Harvested"] == "2026-05-05"
