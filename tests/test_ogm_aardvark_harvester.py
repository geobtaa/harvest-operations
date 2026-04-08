import json
from pathlib import Path

import pandas as pd

from harvesters.ogm_aardvark import OgmAardvarkHarvester


ROOT = Path(__file__).resolve().parents[1]
SAMPLE_FILES = [
    ROOT / "inputs/ogm_aardvark/edu.uwm/metadata-aardvark/gmgs0000013_BL_Aardvark.json",
    ROOT / "inputs/ogm_aardvark/edu.uwm/metadata-aardvark/gmgs000002n_BL_Aardvark.json",
    ROOT / "inputs/ogm_aardvark/edu.uwm/metadata-aardvark/gmgsbg79drz_BL_Aardvark.json",
]


def _config(json_path: str) -> dict:
    return {
        "json_path": json_path,
        "output_primary_csv": "outputs/ogm_aardvark_primary.csv",
        "output_distributions_csv": "outputs/ogm_aardvark_distributions.csv",
        "endpoint_url": "https://github.com/OpenGeoMetadata/edu.uwm",
        "endpoint_description": "GitHub",
        "website_platform": "GeoBlacklight",
        "accrual_method": "Automated retrieval",
        "accrual_periodicity": "Irregular",
        "harvest_workflow": "py_ogm_aardvark",
    }


def _load_sample_records():
    return [json.loads(path.read_text(encoding="utf-8")) for path in SAMPLE_FILES]


def _copy_sample_tree(destination: Path) -> Path:
    input_root = destination / "inputs" / "ogm_aardvark" / "edu.uwm" / "metadata-aardvark"
    input_root.mkdir(parents=True, exist_ok=True)

    for sample_path in SAMPLE_FILES:
        target = input_root / sample_path.name
        target.write_text(sample_path.read_text(encoding="utf-8"), encoding="utf-8")

    return input_root.parent.parent


def test_ogm_aardvark_maps_schema_fields_and_preserves_custom_fields():
    harvester = OgmAardvarkHarvester(_config(str(ROOT / "inputs" / "ogm_aardvark")))
    harvester.load_reference_data()

    df = harvester.build_dataframe(harvester.flatten(_load_sample_records()))
    df = harvester.derive_fields(df)
    df = harvester.add_defaults(df)
    df = harvester.add_provenance(df)
    df = harvester.clean(df)

    regular_row = df.loc[df["ID"] == "ark:-77981-gmgs000002n"].iloc[0]
    openindex_row = df.loc[df["ID"] == "ark:-77981-gmgs0000013"].iloc[0]

    assert regular_row["Title"] == "Boundary Green County, Wisconsin 2002"
    assert regular_row["Creator"] == "Legislative Technology Services Bureau"
    assert regular_row["Theme"] == "Boundaries"
    assert regular_row["Provider"] == "American Geographical Society Library – UWM Libraries"
    assert regular_row["Identifier"] == "ark:/77981/gmgs000002n"
    assert regular_row["Bounding Box"] == "-89.839,42.500,-89.366,42.858"
    assert regular_row["Geometry"].startswith("POLYGON((")
    assert regular_row["download"] == "https://geodata.uwm.edu/public/gmgs000002n/GreenCounty_Boundary_2002.zip"
    assert regular_row["iso"] == "https://raw.githubusercontent.com/OpenGeoMetadata/edu.uwm/main/metadata-iso/gmgs000002n_ISO.xml"

    assert openindex_row["Date Range"] == "1943-1943"
    assert openindex_row["geo_json"] == "https://raw.githubusercontent.com/OpenIndexMaps/edu.uwm/main/665bA25000.geojson"
    assert openindex_row["openindexmap"] == "https://raw.githubusercontent.com/OpenIndexMaps/edu.uwm/main/665bA25000.geojson"

    assert "ark:-77981-gmgsbg79drz" not in set(df["ID"])

    assert set(df.columns[-3:]) == {
        "dct_references_s",
        "gbl_mdModified_dt",
        "gbl_mdVersion_s",
    }
    assert regular_row["gbl_mdVersion_s"] == "Aardvark"


def test_ogm_aardvark_pipeline_writes_primary_and_distribution_outputs(tmp_path, monkeypatch):
    input_root = _copy_sample_tree(tmp_path)
    monkeypatch.chdir(tmp_path)

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

    assert len(primary_df) == 2
    assert set(primary_df["ID"]) == {
        "ark:-77981-gmgs0000013",
        "ark:-77981-gmgs000002n",
    }
    assert "download" not in primary_df.columns
    assert "dct_references_s" not in primary_df.columns
    assert "gbl_mdVersion_s" not in primary_df.columns
    assert "gbl_mdModified_dt" in primary_df.columns

    metadata_iso_rows = distributions_df.loc[
        distributions_df["reference_type"] == "metadata_iso"
    ]
    openindex_rows = distributions_df.loc[
        distributions_df["friendlier_id"] == "ark:-77981-gmgs0000013"
    ]

    assert len(metadata_iso_rows) == 1
    assert "ark:-77981-gmgsbg79drz" not in set(distributions_df["friendlier_id"])
    assert set(openindex_rows["reference_type"]) == {
        "documentation_external",
        "download",
        "geo_json",
        "open_index_map",
    }

    download_labels = distributions_df.loc[
        distributions_df["reference_type"] == "download",
        ["friendlier_id", "label"],
    ]
    download_label_map = dict(download_labels.to_records(index=False))
    assert download_label_map["ark:-77981-gmgs0000013"] == "GeoJSON"
    assert download_label_map["ark:-77981-gmgs000002n"] == "Shapefile"


def test_ogm_aardvark_derives_date_range_when_column_exists_but_row_value_is_missing():
    harvester = OgmAardvarkHarvester(_config(str(ROOT / "inputs" / "ogm_aardvark")))
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
