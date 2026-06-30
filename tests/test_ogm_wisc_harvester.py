import base64
from io import BytesIO
import json
import tarfile

import pandas as pd

from harvesters.ogmWisc import (
    GITHUB_API_ROOT,
    OgmWiscHarvester,
    fetch_github_commit_json,
    fetch_github_tarball_json,
)


def _config(**overrides):
    config = {
        "source_mode": "local_json",
        "json_path": "inputs/ogmWisc",
        "output_primary_csv": "outputs/ogmWisc_primary.csv",
        "output_distributions_csv": "outputs/ogmWisc_distributions.csv",
        "github_owner": "OpenGeoMetadata",
        "github_repo": "edu.wisc",
        "github_ref": "main",
    }
    config.update(overrides)
    return config


def _record(slug, title):
    return {
        "dc_title_s": title,
        "dc_description_s": f"{title} description",
        "dc_creator_sm": ["Adams County"],
        "dc_rights_s": "Public",
        "dc_format_s": "Shapefile",
        "layer_slug_s": slug,
        "dct_temporal_sm": ["2020"],
        "dc_subject_sm": ["Farming"],
        "dc_type_s": "Dataset",
        "layer_geom_type_s": "Polygon",
        "solr_geom": "ENVELOPE(-90,-89,45,44)",
        "dct_references_s": json.dumps(
            {"http://schema.org/downloadUrl": f"https://example.com/{slug}.zip"}
        ),
    }


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


def test_fetch_github_commit_json_processes_changed_json_and_deleted_files():
    newer_record = _record("newer-record", "Newer Record")
    renamed_record = _record("renamed-record", "Renamed Record")
    session = FakeGithubSession(
        {
            "commits": FakeResponse([{"sha": "newer-sha"}, {"sha": "older-sha"}]),
            "newer-sha": FakeResponse(
                {
                    "sha": "newer-sha",
                    "commit": {"committer": {"date": "2026-06-30T12:00:00Z"}},
                    "files": [
                        {"filename": "metadata/newer-record.json", "status": "modified"},
                        {"filename": "metadata/deleted-record.json", "status": "removed"},
                        {"filename": "README.md", "status": "modified"},
                    ],
                }
            ),
            "older-sha": FakeResponse(
                {
                    "sha": "older-sha",
                    "commit": {"committer": {"date": "2026-06-29T12:00:00Z"}},
                    "files": [
                        {"filename": "metadata/newer-record.json", "status": "modified"},
                        {
                            "filename": "metadata/renamed-record.json",
                            "previous_filename": "metadata/old-record.json",
                            "status": "renamed",
                        },
                    ],
                }
            ),
            "metadata/newer-record.json": _content_response(newer_record),
            "metadata/renamed-record.json": _content_response(renamed_record),
        }
    )

    records, deleted_files = fetch_github_commit_json(
        _config(source_mode="github_commits", github_recent_commits=2),
        session,
    )

    assert [record["layer_slug_s"] for record in records] == [
        "newer-record",
        "renamed-record",
    ]
    assert len(deleted_files) == 1
    assert deleted_files[0]["file_path"] == "metadata/deleted-record.json"
    assert deleted_files[0]["inferred_id"] == "deleted-record"

    content_calls = [call for call in session.calls if "/contents/" in call["url"]]
    assert content_calls[0]["params"] == {"ref": "newer-sha"}
    assert content_calls[1]["params"] == {"ref": "older-sha"}


def test_fetch_github_tarball_json_reads_json_members_only():
    session = FakeGithubSession(
        {
            "tarball": _tarball_response(
                {
                    "OpenGeoMetadata-edu.wisc-abc/metadata/one.json": json.dumps(
                        _record("one", "One")
                    ),
                    "OpenGeoMetadata-edu.wisc-abc/README.md": "Ignore me",
                }
            )
        }
    )

    records = fetch_github_tarball_json(_config(source_mode="github_tarball"), session)

    assert [record["layer_slug_s"] for record in records] == ["one"]
    assert session.calls[0]["url"] == (
        f"{GITHUB_API_ROOT}/repos/OpenGeoMetadata/edu.wisc/tarball/main"
    )


def test_commit_mode_skips_build_uploads_even_when_enabled():
    harvester = OgmWiscHarvester(
        _config(source_mode="github_commits", build_uploads=True)
    )

    assert harvester.build_uploads({"primary_csv": "outputs/current.csv"}) is None


def test_commit_mode_writes_delta_outputs_and_deletion_manifest(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    harvester = OgmWiscHarvester(_config(source_mode="github_commits"))
    harvester.distribution_types = [
        {
            "key": "download",
            "variables": ["download"],
            "reference_uri": "http://schema.org/downloadUrl",
        }
    ]
    harvester.deleted_github_files = [
        {
            "file_path": "metadata/deleted-record.json",
            "previous_file_path": "",
            "inferred_id": "deleted-record",
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

    assert results["primary_csv"].endswith("_ogmWisc_commit_delta_primary.csv")
    assert results["distributions_csv"].endswith(
        "_ogmWisc_commit_delta_distributions.csv"
    )
    assert results["deleted_files_csv"].endswith("_ogmWisc_commit_deletions.csv")
    assert results["processed_count"] == 1
    assert results["deleted_count"] == 1

    primary_out = pd.read_csv(results["primary_csv"], dtype=str).fillna("")
    distribution_out = pd.read_csv(results["distributions_csv"], dtype=str).fillna("")
    deletions_out = pd.read_csv(results["deleted_files_csv"], dtype=str).fillna("")

    assert list(primary_out["ID"]) == ["new-record"]
    assert list(distribution_out["friendlier_id"]) == ["new-record"]
    assert list(deletions_out["inferred_id"]) == ["deleted-record"]
