import json
from pathlib import Path

import pytest
import yaml

from scripts.oai_download import download_set, oai_params
from scripts.oai_pmh_pipeline import execute_stage, scoped_output_filename
from utils.oai_pmh import (
    ALL_RECORDS_SET,
    OaiPmhAccessError,
    OaiPmhConfigError,
    load_configured_sets,
    raise_for_oai_status,
    validate_oai_job_config,
)


def job_config(tmp_path: Path) -> dict:
    return {
        "name": "example-university",
        "type": "oai_qdc",
        "feed_type": "oai_dc",
        "oai_base_url": "https://example.edu/oai",
        "metadata_prefix": "oai_dc",
        "provider": "Example University",
        "source_name": "Example University Libraries",
        "source_id_prefix": "example",
        "oai_download_dir": str(tmp_path / "downloads"),
        "sets": [{"set": "maps", "title": "Map Collection"}],
        "output_primary_csv": "outputs/example-university_primary.csv",
        "output_distributions_csv": "outputs/example-university_distributions.csv",
    }


def test_load_configured_sets_prefers_inline_yaml_and_deduplicates(tmp_path) -> None:
    config = job_config(tmp_path)
    config["sets"] = [
        {"set": "maps", "title": "Map Collection"},
        {"set_spec": "maps", "set_title": "Duplicate"},
        "aerials",
    ]

    assert load_configured_sets(config, tmp_path) == [
        {"set_spec": "maps", "set_title": "Map Collection"},
        {"set_spec": "aerials", "set_title": ""},
    ]


def test_validate_oai_job_rejects_reversed_date_window(tmp_path) -> None:
    config = job_config(tmp_path)
    config["oai_request"] = {"from": "2026-08-02", "until": "2026-08-01"}

    with pytest.raises(OaiPmhConfigError, match="cannot be after"):
        validate_oai_job_config(config, tmp_path, job_id="example-university")


def test_oai_params_supports_dates_and_repository_wide_harvest() -> None:
    assert oai_params(
        "oai_dc",
        ALL_RECORDS_SET,
        from_date="2026-01-01",
        until_date="2026-06-30",
    ) == {
        "verb": "ListRecords",
        "metadataPrefix": "oai_dc",
        "from": "2026-01-01",
        "until": "2026-06-30",
    }
    assert oai_params("oai_dc", resumption_token="next-page") == {
        "verb": "ListRecords",
        "resumptionToken": "next-page",
    }


def test_single_set_output_filename_does_not_replace_full_source_output() -> None:
    assert scoped_output_filename(
        "outputs/example-university_primary.csv",
        "node:3186",
    ) == "outputs/example-university-node-3186_primary.csv"


class FakeResponse:
    def __init__(self, text: str):
        self.text = text

    def raise_for_status(self) -> None:
        return None


class CloudflareChallengeResponse:
    status_code = 403
    headers = {"server": "cloudflare", "cf-mitigated": "challenge"}
    text = '<script src="/cdn-cgi/challenge-platform/scripts/jsd/main.js"></script>'

    def raise_for_status(self) -> None:
        raise AssertionError("The OAI-specific access error should be raised first.")


def test_cloudflare_challenge_has_actionable_oai_error() -> None:
    with pytest.raises(OaiPmhAccessError, match="repository owner to exempt"):
        raise_for_oai_status(CloudflareChallengeResponse())


class FakeSession:
    def __init__(self, pages: list[str]):
        self.pages = iter(pages)
        self.params = []

    def get(self, base_url, *, params, timeout):
        self.params.append(params)
        return FakeResponse(next(self.pages))


def test_download_set_atomically_removes_stale_pages(tmp_path) -> None:
    first_page = """<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
      <ListRecords><resumptionToken>page-two</resumptionToken></ListRecords>
    </OAI-PMH>"""
    second_page = """<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
      <ListRecords><resumptionToken /></ListRecords>
    </OAI-PMH>"""
    old_set_dir = tmp_path / "maps"
    old_set_dir.mkdir()
    (old_set_dir / "0003.xml").write_text("stale", encoding="utf-8")

    manifest = download_set(
        session=FakeSession([first_page, second_page]),
        base_url="https://example.edu/oai",
        metadata_prefix="oai_dc",
        set_spec="maps",
        set_title="Maps",
        output_dir=tmp_path,
        delay=0,
        timeout=30,
    )

    assert sorted(path.name for path in old_set_dir.glob("*.xml")) == [
        "0001.xml",
        "0002.xml",
    ]
    assert manifest["downloaded_files"] == [
        str(old_set_dir / "0001.xml"),
        str(old_set_dir / "0002.xml"),
    ]


def test_download_set_preserves_previous_snapshot_on_oai_error(tmp_path) -> None:
    error_page = """<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
      <error code="badResumptionToken">Expired token</error>
    </OAI-PMH>"""
    old_set_dir = tmp_path / "maps"
    old_set_dir.mkdir()
    prior_page = old_set_dir / "0001.xml"
    prior_page.write_text("known good snapshot", encoding="utf-8")

    manifest = download_set(
        session=FakeSession([error_page]),
        base_url="https://example.edu/oai",
        metadata_prefix="oai_dc",
        set_spec="maps",
        set_title="Maps",
        output_dir=tmp_path,
        delay=0,
        timeout=30,
    )

    assert prior_page.read_text(encoding="utf-8") == "known good snapshot"
    assert manifest["snapshot_replaced"] is False
    assert manifest["downloaded_files"] == []
    assert (tmp_path / "maps-failed-manifest.json").is_file()


def test_validate_stage_writes_source_scoped_status(tmp_path) -> None:
    config = job_config(tmp_path)
    config_path = tmp_path / "example-university.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    result = execute_stage(config_path, "validate")

    status_path = tmp_path / "downloads" / "job-status.json"
    status = json.loads(status_path.read_text(encoding="utf-8"))
    assert result["set_count"] == 1
    assert status["job_id"] == "example-university"
    assert status["stages"]["validate"]["status"] == "completed"
    assert status["config_sha256"]


def test_initial_validation_allows_empty_sets_for_discovery(tmp_path) -> None:
    config = job_config(tmp_path)
    config["sets"] = []
    config_path = tmp_path / "example-university.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    result = execute_stage(config_path, "validate")

    assert result["set_count"] == 0
    assert result["ready_for_harvest"] is False
    status = json.loads(
        (tmp_path / "downloads" / "job-status.json").read_text(encoding="utf-8")
    )
    assert status["stages"]["validate"]["status"] == "completed"


def test_failed_validation_is_recorded_in_source_status(tmp_path) -> None:
    config = job_config(tmp_path)
    config["source_id_prefix"] = ""
    config_path = tmp_path / "example-university.yaml"
    config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    with pytest.raises(OaiPmhConfigError, match="source_id_prefix"):
        execute_stage(config_path, "validate")

    status = json.loads(
        (tmp_path / "downloads" / "job-status.json").read_text(encoding="utf-8")
    )
    assert status["stages"]["validate"]["status"] == "failed"
    assert "source_id_prefix is required" in status["stages"]["validate"]["error"]
