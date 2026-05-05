from datetime import date

import pandas as pd

from scripts.build_uploads import build_distribution_delta_files, build_primary_upload, run_build_uploads


def test_build_distribution_delta_files_ignores_row_order():
    old_dist = pd.DataFrame(
        [
            {
                "friendlier_id": "shared-id",
                "reference_type": "download",
                "distribution_url": "https://example.com/a.zip",
                "label": "ZIP",
            },
            {
                "friendlier_id": "shared-id",
                "reference_type": "metadata_iso",
                "distribution_url": "https://example.com/a.xml",
                "label": "",
            },
        ]
    )
    new_dist = old_dist.iloc[::-1].reset_index(drop=True)

    add_df, delete_df, changed_ids = build_distribution_delta_files(
        new_dist,
        old_dist,
        new_ids=set(),
        shared_ids={"shared-id"},
    )

    assert add_df.empty
    assert delete_df.empty
    assert changed_ids == set()


def test_build_primary_upload_includes_current_harvest_records_when_unchanged():
    old_df = pd.DataFrame(
        [
            {
                "ID": "shared-id",
                "Title": "Shared Record",
                "Resource Class": "Datasets",
                "Last Harvested": "2026-04-01",
            },
            {
                "ID": "harvest_site-1",
                "Title": "Harvest record for Site 1",
                "Resource Class": "Series",
                "Last Harvested": "2026-04-01",
            },
        ]
    )
    new_df = pd.DataFrame(
        [
            {
                "ID": "shared-id",
                "Title": "Shared Record",
                "Resource Class": "Datasets",
                "Last Harvested": "2026-05-05",
            },
            {
                "ID": "harvest_site-1",
                "Title": "Harvest record for Site 1",
                "Resource Class": "Series",
                "Last Harvested": "2026-05-05",
            },
        ]
    )

    upload_df, new_only_df, old_only_df = build_primary_upload(new_df, old_df)

    assert new_only_df.empty
    assert old_only_df.empty
    assert upload_df["ID"].tolist() == ["harvest_site-1"]
    assert upload_df.loc[0, "Last Harvested"] == "2026-05-05"


def test_build_primary_upload_excludes_unchanged_website_rows():
    old_df = pd.DataFrame(
        [
            {
                "ID": "website-1",
                "Title": "Website Record",
                "Resource Class": "Websites",
                "Last Harvested": "2026-04-01",
            },
            {
                "ID": "harvest_site-1",
                "Title": "Harvest record for Site 1",
                "Resource Class": "Series",
                "Last Harvested": "2026-04-01",
            },
        ]
    )
    new_df = pd.DataFrame(
        [
            {
                "ID": "website-1",
                "Title": "Website Record",
                "Resource Class": "Websites",
                "Last Harvested": "2026-05-05",
            },
            {
                "ID": "harvest_site-1",
                "Title": "Harvest record for Site 1",
                "Resource Class": "Series",
                "Last Harvested": "2026-05-05",
            },
        ]
    )

    upload_df, new_only_df, old_only_df = build_primary_upload(new_df, old_df)

    assert new_only_df.empty
    assert old_only_df.empty
    assert upload_df["ID"].tolist() == ["harvest_site-1"]


def test_run_build_uploads_emits_distribution_add_and_delete_files(tmp_path):
    source = "sample"
    old_date = "2026-03-25"
    new_date = "2026-04-01"
    today = date.today().isoformat()

    old_primary = pd.DataFrame(
        [
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
    )
    new_primary = pd.DataFrame(
        [
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
    )
    old_dist = pd.DataFrame(
        [
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
    )
    new_dist = pd.DataFrame(
        [
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
    )

    old_primary.to_csv(tmp_path / f"{old_date}_{source}_primary.csv", index=False)
    new_primary.to_csv(tmp_path / f"{new_date}_{source}_primary.csv", index=False)
    old_dist.to_csv(tmp_path / f"{old_date}_{source}_distributions.csv", index=False)
    new_dist.to_csv(tmp_path / f"{new_date}_{source}_distributions.csv", index=False)

    summary = run_build_uploads(source, tmp_path)

    assert summary["new_count"] == 1
    assert summary["retired_count"] == 1
    assert summary["distribution_new_count"] == 2
    assert summary["distribution_delete_count"] == 1
    assert summary["changed_distribution_ids"] == {"shared-id"}

    primary_upload = pd.read_csv(
        tmp_path / "to_upload" / f"{today}_{source}_primary_upload.csv",
        dtype=str,
        keep_default_na=False,
    ).fillna("")
    dist_new_upload = pd.read_csv(
        tmp_path / "to_upload" / f"{today}_{source}_distributions_new.csv",
        dtype=str,
        keep_default_na=False,
    ).fillna("")
    dist_delete_upload = pd.read_csv(
        tmp_path / "to_upload" / f"{today}_{source}_distributions_delete.csv",
        dtype=str,
        keep_default_na=False,
    ).fillna("")

    assert set(primary_upload["ID"]) == {"new-id", "retired-id"}
    retired_row = primary_upload.loc[primary_upload["ID"] == "retired-id"].iloc[0]
    assert retired_row["Publication State"] == "unpublished"
    assert retired_row["Date Retired"] == today

    assert set(dist_new_upload["friendlier_id"]) == {"shared-id", "new-id"}
    assert set(dist_new_upload["distribution_url"]) == {
        "https://example.com/new.zip",
        "https://example.com/new-record.zip",
    }

    assert list(dist_delete_upload["friendlier_id"]) == ["shared-id"]
    assert list(dist_delete_upload["distribution_url"]) == ["https://example.com/old.zip"]
