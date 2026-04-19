from datetime import date
from pathlib import Path

import pandas as pd

from harvesters.base import BaseHarvester


class DummyUploadHarvester(BaseHarvester):
    def __init__(self, config, primary_rows, distribution_rows):
        super().__init__(config)
        self.primary_rows = primary_rows
        self.distribution_rows = distribution_rows

    def load_reference_data(self):
        self.theme_map = {}

    def fetch(self):
        return self.primary_rows

    def build_dataframe(self, parsed_or_flattened_data):
        return pd.DataFrame(parsed_or_flattened_data)

    def clean(self, df):
        return df

    def validate(self, df):
        return df

    def write_outputs(self, primary_df, distributions_df=None):
        distributions_df = pd.DataFrame(self.distribution_rows)
        return super().write_outputs(primary_df, distributions_df)


def test_base_harvester_can_build_upload_deltas_as_final_step(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    config = {
        "output_primary_csv": "outputs/sample_primary.csv",
        "output_distributions_csv": "outputs/sample_distributions.csv",
        "build_uploads": True,
    }

    old_primary_rows = [
        {
            "ID": "shared-id",
            "Title": "Shared Record",
            "Resource Class": "Datasets",
            "Publication State": "published",
            "Display Note": "",
        },
        {
            "ID": "retired-id",
            "Title": "Retired Record",
            "Resource Class": "Datasets",
            "Publication State": "published",
            "Display Note": "",
        },
    ]
    new_primary_rows = [
        {
            "ID": "shared-id",
            "Title": "Shared Record",
            "Resource Class": "Datasets",
            "Publication State": "published",
            "Display Note": "",
        },
        {
            "ID": "new-id",
            "Title": "New Record",
            "Resource Class": "Datasets",
            "Publication State": "published",
            "Display Note": "",
        },
    ]
    old_distribution_rows = [
        {
            "friendlier_id": "shared-id",
            "reference_type": "download",
            "distribution_url": "https://example.com/old.zip",
            "label": "ZIP",
        },
        {
            "friendlier_id": "retired-id",
            "reference_type": "download",
            "distribution_url": "https://example.com/retired.zip",
            "label": "ZIP",
        },
    ]
    new_distribution_rows = [
        {
            "friendlier_id": "shared-id",
            "reference_type": "download",
            "distribution_url": "https://example.com/new.zip",
            "label": "ZIP",
        },
        {
            "friendlier_id": "new-id",
            "reference_type": "download",
            "distribution_url": "https://example.com/new-record.zip",
            "label": "ZIP",
        },
    ]

    first_results = DummyUploadHarvester(
        config,
        old_primary_rows,
        old_distribution_rows,
    ).harvest_pipeline()

    assert first_results["upload_summary"]["status"] == "skipped"

    outputs_dir = tmp_path / "outputs"
    Path(first_results["primary_csv"]).rename(outputs_dir / "2026-03-25_sample_primary.csv")
    Path(first_results["distributions_csv"]).rename(
        outputs_dir / "2026-03-25_sample_distributions.csv"
    )

    second_results = DummyUploadHarvester(
        config,
        new_primary_rows,
        new_distribution_rows,
    ).harvest_pipeline()

    upload_summary = second_results["upload_summary"]
    assert upload_summary["status"] == "created"
    assert upload_summary["source"] == "sample"
    assert upload_summary["new_count"] == 1
    assert upload_summary["retired_count"] == 1
    assert upload_summary["distribution_new_count"] == 2
    assert upload_summary["distribution_delete_count"] == 1
    assert upload_summary["changed_distribution_ids"] == ["shared-id"]

    primary_upload = pd.read_csv(
        upload_summary["primary_upload_csv"],
        dtype=str,
        keep_default_na=False,
    ).fillna("")
    distributions_new = pd.read_csv(
        upload_summary["distributions_new_csv"],
        dtype=str,
        keep_default_na=False,
    ).fillna("")
    distributions_delete = pd.read_csv(
        upload_summary["distributions_delete_csv"],
        dtype=str,
        keep_default_na=False,
    ).fillna("")

    today = date.today().isoformat()
    retired_row = primary_upload.loc[primary_upload["ID"] == "retired-id"].iloc[0]

    assert set(primary_upload["ID"]) == {"new-id", "retired-id"}
    assert retired_row["Publication State"] == "unpublished"
    assert retired_row["Date Retired"] == today
    assert set(distributions_new["friendlier_id"]) == {"shared-id", "new-id"}
    assert list(distributions_delete["friendlier_id"]) == ["shared-id"]
