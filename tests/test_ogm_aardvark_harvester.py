import base64
from io import BytesIO
import json
from pathlib import Path
import tarfile

import pandas as pd

from harvesters.ogm_aardvark import (
    GITHUB_API_ROOT,
    OgmAardvarkHarvester,
    fetch_github_commit_json,
    fetch_github_tarball_json,
    load_repo_defaults,
)


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
INLINE_SAMPLE_RECORDS = [
    {
        "id": "utlmaps:225fea8d-1e1c-452f-8bb0-056028f2bd85",
        "dct_title_s": "Sanborn Fire Insurance Maps [Houston, Texas, 1907, Sheet 17]",
        "dct_identifier_sm": ["utlmaps:225fea8d-1e1c-452f-8bb0-056028f2bd85"],
        "dct_accessRights_s": "Public",
        "gbl_resourceClass_sm": ["Maps"],
        "schema_provider_s": "Texas",
        "dct_temporal_sm": ["1907"],
        "gbl_indexYear_im": [1907],
        "dcat_bbox": "ENVELOPE(-95.362,-95.357,29.759,29.754)",
        "locn_geometry": "ENVELOPE(-95.362,-95.357,29.759,29.754)",
        "dct_format_s": "GeoTIFF",
        "dct_references_s": json.dumps(
            {
                "https://github.com/cogeotiff/cog-spec": (
                    "https://curio.lib.utexas.edu/geodata/raster/"
                    "utlmaps-225fea8d-1e1c-452f-8bb0-056028f2bd85-cog.tif"
                ),
                "http://schema.org/downloadUrl": (
                    "https://curio.lib.utexas.edu/geodata/raster/"
                    "utlmaps-225fea8d-1e1c-452f-8bb0-056028f2bd85-cog.tif"
                ),
                "http://schema.org/url": (
                    "https://collections.lib.utexas.edu/catalog/"
                    "utlmaps:225fea8d-1e1c-452f-8bb0-056028f2bd85"
                ),
            }
        ),
        "gbl_mdModified_dt": "2026-01-01T00:00:00Z",
        "gbl_mdVersion_s": "Aardvark",
    },
    {
        "id": "utaustin_19326",
        "dct_title_s": "AMS Japan 1:250,000 Zeni Su 52",
        "dct_accessRights_s": "Public",
        "gbl_resourceClass_sm": ["Maps"],
        "dct_temporal_sm": ["1943"],
        "gbl_indexYear_im": [1943],
        "dcat_bbox": "ENVELOPE(139,140,36,35)",
        "dct_format_s": "GeoJPEG",
        "dct_references_s": json.dumps(
            {
                "http://schema.org/downloadUrl": (
                    "https://curio.lib.utexas.edu/geodata/raster/utaustin_19326.jpg"
                ),
                "http://www.isotc211.org/schemas/2005/gmd/": (
                    "https://curio.lib.utexas.edu/geodata/iso/"
                    "utlmaps__ams__japan_l506__250k__6613121__zeni_su_52.xml"
                ),
            }
        ),
    },
    {
        "id": "utaustin_19263",
        "dct_title_s": "AMS Japan 1:250,000 Sample Sheet",
        "dct_accessRights_s": "Public",
        "gbl_resourceClass_sm": ["Maps"],
        "dct_temporal_sm": ["1943"],
        "gbl_indexYear_im": [1943],
        "dcat_bbox": "ENVELOPE(138,139,35,34)",
        "dct_references_s": json.dumps(
            {
                "http://www.isotc211.org/schemas/2005/gmd/": (
                    "https://curio.lib.utexas.edu/geodata/iso/utaustin_19263.xml"
                )
            }
        ),
    },
]


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


def _github_config(**overrides) -> dict:
    config = _config("inputs/edu.utexas")
    config.update(
        {
            "source_mode": "github_tarball",
            "github_owner": "OpenGeoMetadata",
            "github_repo": "edu.utexas",
            "github_ref": "main",
            "github_path": "metadata-aardvark",
        }
    )
    config.update(overrides)
    return config


class FakeResponse:
    def __init__(self, json_data=None, content=b""):
        self._json_data = json_data
        self.content = content

    def json(self):
        return self._json_data

    def raise_for_status(self):
        return None


class FakeGithubSession:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        key = self._key(url)
        response = self.responses[key]
        return response() if callable(response) else response

    def _key(self, url):
        if url.endswith("/commits"):
            return "commits"
        if "/contents/" in url:
            return url.split("/contents/", 1)[1]
        if "/commits/" in url:
            return url.rsplit("/", 1)[1]
        if "/tarball/" in url:
            return "tarball"
        return url


def _content_response(record):
    raw = json.dumps(record).encode("utf-8")
    encoded = base64.b64encode(raw).decode("ascii")
    return FakeResponse({"encoding": "base64", "content": encoded})


def _tarball_response(files):
    stream = BytesIO()
    with tarfile.open(fileobj=stream, mode="w:gz") as archive:
        for path, payload in files.items():
            data = payload.encode("utf-8")
            info = tarfile.TarInfo(path)
            info.size = len(data)
            archive.addfile(info, BytesIO(data))
    return FakeResponse(content=stream.getvalue())


def _load_sample_records():
    if all(path.exists() for path in SAMPLE_FILES):
        return [
            json.loads(path.read_text(encoding="utf-8")) for path in SAMPLE_FILES
        ] + [RESTRICTED_RECORD]
    return [record.copy() for record in INLINE_SAMPLE_RECORDS] + [RESTRICTED_RECORD]


def _copy_sample_tree(destination: Path) -> Path:
    input_root = destination / "inputs" / "edu.utexas" / "metadata-aardvark"
    input_root.mkdir(parents=True, exist_ok=True)

    if all(path.exists() for path in SAMPLE_FILES):
        for sample_path in SAMPLE_FILES:
            target = input_root / sample_path.name
            target.write_text(sample_path.read_text(encoding="utf-8"), encoding="utf-8")
    else:
        for record in INLINE_SAMPLE_RECORDS:
            target = input_root / f"{record['id'].replace(':', '_')}.json"
            target.write_text(json.dumps(record), encoding="utf-8")
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
    assert (
        sanborn_row["Identifier"]
        == "utlmaps:225fea8d-1e1c-452f-8bb0-056028f2bd85"
    )
    assert sanborn_row["Bounding Box"] == "-95.362,29.754,-95.357,29.759"
    assert sanborn_row["Geometry"].startswith("POLYGON((")
    assert sanborn_row["Date Range"] == "1907-1907"
    assert (
        sanborn_row["download"]
        == "https://curio.lib.utexas.edu/geodata/raster/"
        "utlmaps-225fea8d-1e1c-452f-8bb0-056028f2bd85-cog.tif"
    )
    assert sanborn_row["download"] == sanborn_row["cog"]
    assert (
        sanborn_row["information"]
        == "https://collections.lib.utexas.edu/catalog/"
        "utlmaps:225fea8d-1e1c-452f-8bb0-056028f2bd85"
    )

    assert ams_row["Date Range"] == "1943-1943"
    assert (
        ams_row["iso"]
        == "https://curio.lib.utexas.edu/geodata/iso/"
        "utlmaps__ams__japan_l506__250k__6613121__zeni_su_52.xml"
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


def test_ogm_aardvark_github_tarball_filters_to_metadata_folder():
    inside_record = _load_sample_records()[0]
    outside_record = {
        "id": "outside-record",
        "dct_title_s": "Outside Record",
        "dct_accessRights_s": "Public",
        "gbl_resourceClass_sm": ["Datasets"],
    }
    session = FakeGithubSession(
        {
            "tarball": _tarball_response(
                {
                    "OpenGeoMetadata-edu.utexas-abc/metadata-aardvark/inside.json": (
                        json.dumps(inside_record)
                    ),
                    "OpenGeoMetadata-edu.utexas-abc/other/outside.json": (
                        json.dumps(outside_record)
                    ),
                    "OpenGeoMetadata-edu.utexas-abc/README.md": "Ignore me",
                }
            )
        }
    )

    records = fetch_github_tarball_json(_github_config(), session)

    assert [record["id"] for record in records] == [inside_record["id"]]
    assert session.calls[0]["url"] == (
        f"{GITHUB_API_ROOT}/repos/OpenGeoMetadata/edu.utexas/tarball/main"
    )


def test_ogm_aardvark_github_commits_uses_repo_folder_filter():
    changed_record = _load_sample_records()[0]
    session = FakeGithubSession(
        {
            "commits": FakeResponse([{"sha": "newer-sha"}]),
            "newer-sha": FakeResponse(
                {
                    "sha": "newer-sha",
                    "commit": {"committer": {"date": "2026-06-30T12:00:00Z"}},
                    "files": [
                        {
                            "filename": "metadata-aardvark/changed.json",
                            "status": "modified",
                        },
                        {
                            "filename": "metadata-aardvark/deleted.json",
                            "status": "removed",
                        },
                        {"filename": "other/ignored.json", "status": "modified"},
                    ],
                }
            ),
            "metadata-aardvark/changed.json": _content_response(changed_record),
        }
    )

    records, deleted_files = fetch_github_commit_json(
        _github_config(source_mode="github_commits", github_recent_commits=1),
        session,
    )

    assert [record["id"] for record in records] == [changed_record["id"]]
    assert len(deleted_files) == 1
    assert deleted_files[0]["file_path"] == "metadata-aardvark/deleted.json"

    commits_call = session.calls[0]
    assert commits_call["params"]["path"] == "metadata-aardvark"
    content_calls = [call for call in session.calls if "/contents/" in call["url"]]
    assert content_calls[0]["params"] == {"ref": "newer-sha"}


def test_ogm_aardvark_commit_mode_writes_delta_outputs_and_manifest(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    harvester = OgmAardvarkHarvester(
        _github_config(
            source_mode="github_commits",
            github_recent_commits=1,
            github_repo="org.humdata",
        )
    )
    harvester.distribution_types = [
        {
            "key": "download",
            "variables": ["download"],
            "reference_uri": "http://schema.org/downloadUrl",
        }
    ]
    harvester.distribution_variables = {"download"}
    harvester.deleted_github_files = [
        {
            "file_path": "metadata-aardvark/deleted.json",
            "previous_file_path": "",
            "inferred_id": "deleted",
            "status": "removed",
            "commit_sha": "abc123",
            "commit_date": "2026-06-30T12:00:00Z",
        }
    ]
    primary_df = pd.DataFrame(
        [
            {
                "ID": "new-record",
                "Title": "New Record",
                "Access Rights": "Public",
                "Resource Class": "Datasets",
                "Bounding Box": "-90,44,-89,45",
                "Date Range": "2020-2020",
                "download": "https://example.com/new-record.zip",
            }
        ]
    )

    results = harvester.write_outputs(primary_df)

    assert results["primary_csv"].endswith("_ogm_org-humdata_commit_delta_primary.csv")
    assert results["distributions_csv"].endswith(
        "_ogm_org-humdata_commit_delta_distributions.csv"
    )
    assert results["deleted_files_csv"].endswith("_ogm_org-humdata_commit_deletions.csv")
    assert results["processed_count"] == 1
    assert results["deleted_count"] == 1

    primary_out = pd.read_csv(results["primary_csv"], dtype=str).fillna("")
    distribution_out = pd.read_csv(results["distributions_csv"], dtype=str).fillna("")
    deletions_out = pd.read_csv(results["deleted_files_csv"], dtype=str).fillna("")

    assert list(primary_out["ID"]) == ["new-record"]
    assert "download" not in primary_out.columns
    assert list(distribution_out["friendlier_id"]) == ["new-record"]
    assert list(deletions_out["inferred_id"]) == ["deleted"]


def test_ogm_aardvark_github_provenance_uses_selected_repo():
    harvester = OgmAardvarkHarvester(
        _github_config(github_repo="edu.example", endpoint_url="")
    )
    df = pd.DataFrame(
        [
            {
                "ID": "record",
                "Title": "Record",
                "Access Rights": "Public",
                "Resource Class": "Datasets",
            }
        ]
    )

    df = harvester.add_provenance(df)

    assert df.loc[0, "Endpoint URL"] == "https://github.com/OpenGeoMetadata/edu.example"


def test_ogm_aardvark_github_tarball_outputs_include_repo_name(
    tmp_path,
    monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    harvester = OgmAardvarkHarvester(
        _github_config(source_mode="github_tarball", github_repo="org.humdata")
    )
    harvester.distribution_types = [
        {
            "key": "download",
            "variables": ["download"],
            "reference_uri": "http://schema.org/downloadUrl",
        }
    ]
    harvester.distribution_variables = {"download"}
    primary_df = pd.DataFrame(
        [
            {
                "ID": "new-record",
                "Title": "New Record",
                "Access Rights": "Public",
                "Resource Class": "Datasets",
                "Bounding Box": "-90,44,-89,45",
                "Date Range": "2020-2020",
                "download": "https://example.com/new-record.zip",
            }
        ]
    )

    results = harvester.write_outputs(primary_df)

    assert results["primary_csv"].endswith("_ogm_org-humdata_primary.csv")
    assert results["distributions_csv"].endswith(
        "_ogm_org-humdata_distributions.csv"
    )


def test_ogm_aardvark_adds_repo_defaults_from_csv():
    harvester = OgmAardvarkHarvester(_github_config(github_repo="edu.utexas"))
    harvester.repo_defaults = load_repo_defaults(
        str(ROOT / "config" / "ogm-repos.csv")
    )
    df = pd.DataFrame(
        [
            {
                "ID": "record",
                "Title": "Record",
                "Access Rights": "Public",
                "Resource Class": "Datasets",
            }
        ]
    )

    df = harvester.add_defaults(df)

    assert df.loc[0, "Code"] == "20d-0007"
    assert df.loc[0, "Member Of"] == "dc8c18df-7d64-4ff4-a754-d18d0891187d"


def test_ogm_aardvark_repo_defaults_preserve_existing_values():
    harvester = OgmAardvarkHarvester(_github_config(github_repo="edu.utexas"))
    harvester.repo_defaults = load_repo_defaults(
        str(ROOT / "config" / "ogm-repos.csv")
    )
    df = pd.DataFrame(
        [
            {
                "ID": "record",
                "Title": "Record",
                "Access Rights": "Public",
                "Resource Class": "Datasets",
                "Code": "source-code",
                "Member Of": "source-member",
            }
        ]
    )

    df = harvester.add_defaults(df)

    assert df.loc[0, "Code"] == "source-code"
    assert df.loc[0, "Member Of"] == "source-member"
