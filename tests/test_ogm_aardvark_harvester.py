import json
from pathlib import Path

import pandas as pd

from harvesters.ogm_aardvark import OgmAardvarkHarvester


ROOT = Path(__file__).resolve().parents[1]
SAMPLE_FILES = [
    ROOT
    / "inputs/edu.utexas/metadata-aardvark/utlmaps-225fea8d-1e1c-452f-8bb0-056028f2bd85.json",
    ROOT / "inputs/edu.utexas/metadata-aardvark/utaustin_19326.json",
    ROOT / "inputs/edu.utexas/metadata-aardvark/utaustin_19263.json",
]
RESTRICTED_RECORD = {
    "id": "restricted-record",
    "dct_title_s": "Restricted Record",
    "dct_accessRights_s": "Restricted",
    "gbl_resourceClass_sm": ["Datasets"],
}


def _config(json_path: str) -> dict:
    return {
        "json_path": json_path,
        "output_primary_csv": "outputs/ogm_aardvark_primary.csv",
        "output_distributions_csv": "outputs/ogm_aardvark_distributions.csv",
        "endpoint_url": "https://github.com/OpenGeoMetadata/edu.utexas",
        "endpoint_description": "GitHub",
        "website_platform": "GeoBlacklight",
        "accrual_method": "Automated retrieval",
        "accrual_periodicity": "Irregular",
        "harvest_workflow": "py_ogm_aardvark",
    }


def _load_sample_records():
    return [
        json.loads(path.read_text(encoding="utf-8")) for path in SAMPLE_FILES
    ] + [RESTRICTED_RECORD]


def _copy_sample_tree(destination: Path) -> Path:
    input_root = destination / "inputs" / "edu.utexas" / "metadata-aardvark"
    input_root.mkdir(parents=True, exist_ok=True)

    for sample_path in SAMPLE_FILES:
        target = input_root / sample_path.name
        target.write_text(sample_path.read_text(encoding="utf-8"), encoding="utf-8")
    (input_root / "restricted-record.json").write_text(
        json.dumps(RESTRICTED_RECORD),
        encoding="utf-8",
    )

    return input_root.parent


def test_ogm_aardvark_maps_schema_fields_and_preserves_custom_fields():
    harvester = OgmAardvarkHarvester(_config(str(ROOT / "inputs" / "edu.utexas")))
    harvester.load_reference_data()

    df = harvester.build_dataframe(harvester.flatten(_load_sample_records()))
    df = harvester.derive_fields(df)
    df = harvester.add_defaults(df)
    df = harvester.add_provenance(df)
    df = harvester.clean(df)

    sanborn_row = df.loc[
        df["ID"] == "utlmaps:225fea8d-1e1c-452f-8bb0-056028f2bd85"
    ].iloc[0]
    ams_row = df.loc[df["ID"] == "utaustin_19326"].iloc[0]

    assert (
        sanborn_row["Title"]
        == "Sanborn Fire Insurance Maps [Houston, Texas, 1907, Sheet 17]"
    )
    assert sanborn_row["Provider"] == "Texas"
    assert sanborn_row["Identifier"] == "utlmaps:225fea8d-1e1c-452f-8bb0-056028f2bd85"
    assert sanborn_row["Bounding Box"] == "-95.362,29.754,-95.357,29.759"
    assert sanborn_row["Geometry"].startswith("POLYGON((")
    assert sanborn_row["Date Range"] == "1907-1907"
    assert (
        sanborn_row["download"]
        == "https://curio.lib.utexas.edu/geodata/raster/utlmaps-225fea8d-1e1c-452f-8bb0-056028f2bd85-cog.tif"
    )
    assert sanborn_row["download"] == sanborn_row["cog"]
    assert (
        sanborn_row["information"]
        == "https://collections.lib.utexas.edu/catalog/utlmaps:225fea8d-1e1c-452f-8bb0-056028f2bd85"
    )

    assert ams_row["Date Range"] == "1943-1943"
    assert (
        ams_row["iso"]
        == "https://curio.lib.utexas.edu/geodata/iso/utlmaps__ams__japan_l506__250k__6613121__zeni_su_52.xml"
    )

    assert "restricted-record" not in set(df["ID"])

    assert set(df.columns[-3:]) == {
        "dct_references_s",
        "gbl_mdModified_dt",
        "gbl_mdVersion_s",
    }
    assert sanborn_row["gbl_mdVersion_s"] == "Aardvark"


def test_ogm_aardvark_pipeline_writes_primary_and_distribution_outputs(tmp_path, monkeypatch):
    input_root = _copy_sample_tree(tmp_path)
    monkeypatch.chdir(tmp_path)
    (tmp_path / "outputs").mkdir()

    harvester = OgmAardvarkHarvester(_config(str(input_root)))
    results = harvester.harvest_pipeline()

    primary_df = pd.read_csv(
        results["primary_csv"],
        dtype=str,
        keep_default_na=False,
    ).fillna("")
    distributions_df = pd.read_csv(
        results["distributions_csv"],
        dtype=str,
        keep_default_na=False,
    ).fillna("")

    assert len(primary_df) == 3
    assert set(primary_df["ID"]) == {
        "utlmaps:225fea8d-1e1c-452f-8bb0-056028f2bd85",
        "utaustin_19326",
        "utaustin_19263",
    }
    assert "restricted-record" not in set(primary_df["ID"])
    assert "download" not in primary_df.columns
    assert "cog" not in primary_df.columns
    assert "dct_references_s" not in primary_df.columns
    assert "gbl_mdVersion_s" not in primary_df.columns
    assert "gbl_mdModified_dt" in primary_df.columns

    metadata_iso_rows = distributions_df.loc[
        distributions_df["reference_type"] == "metadata_iso"
    ]
    sanborn_rows = distributions_df.loc[
        distributions_df["friendlier_id"]
        == "utlmaps:225fea8d-1e1c-452f-8bb0-056028f2bd85"
    ]

    assert len(metadata_iso_rows) == 2
    assert "restricted-record" not in set(distributions_df["friendlier_id"])
    assert set(sanborn_rows["reference_type"]) == {
        "cog",
        "documentation_external",
        "download",
    }

    download_labels = distributions_df.loc[
        distributions_df["reference_type"] == "download",
        ["friendlier_id", "label"],
    ]
    download_label_map = dict(download_labels.to_records(index=False))
    assert (
        download_label_map["utlmaps:225fea8d-1e1c-452f-8bb0-056028f2bd85"]
        == "GeoTIFF"
    )
    assert download_label_map["utaustin_19326"] == "GeoJPEG"


def test_ogm_aardvark_derives_date_range_when_column_exists_but_row_value_is_missing():
    harvester = OgmAardvarkHarvester(_config(str(ROOT / "inputs" / "edu.utexas")))
    harvester.load_reference_data()

    records = [
        {
            "id": "record-with-existing-date-range",
            "dct_title_s": "Record With Existing Date Range",
            "dct_accessRights_s": "Public",
            "gbl_resourceClass_sm": ["Datasets"],
            "dct_temporal_sm": ["2001"],
            "gbl_indexYear_im": [2001],
            "gbl_dateRange_drsim": "2001-2001",
        },
        {
            "id": "record-missing-date-range",
            "dct_title_s": "Record Missing Date Range",
            "dct_accessRights_s": "Public",
            "gbl_resourceClass_sm": ["Datasets"],
            "dct_temporal_sm": ["1943"],
            "gbl_indexYear_im": [1943],
        },
    ]

    df = harvester.build_dataframe(records)
    df = harvester.derive_fields(df)

    date_ranges = dict(df[["ID", "Date Range"]].to_records(index=False))
    assert date_ranges["record-with-existing-date-range"] == "2001-2001"
    assert date_ranges["record-missing-date-range"] == "1943-1943"
