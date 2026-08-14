from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_static_index_links_to_ogm_aardvark_page() -> None:
    index_html = (ROOT / "static" / "index.html").read_text(encoding="utf-8")

    assert '/static/ogm-aardvark.html' in index_html
    assert "OpenGeoMetadata Aardvark Harvester" in index_html


def test_static_ogm_aardvark_page_runs_expected_job() -> None:
    page_html = (ROOT / "static" / "ogm-aardvark.html").read_text(encoding="utf-8")

    assert 'fetch("/jobs/ogm-aardvark/run"' in page_html
    assert 'fetch("/jobs/ogm-aardvark/repositories")' in page_html
    assert 'value="github_commits" selected' in page_html
    assert 'name="github_repo"' in page_html
    assert "Loading repositories..." in page_html
    assert 'name="github_path"' in page_html
    assert 'value="metadata-aardvark"' in page_html
    assert 'name="github_recent_commits"' in page_html
    assert 'value="4"' in page_html
    assert 'name="github_since_date"' in page_html
    assert 'value="2026-06-01"' in page_html
    assert 'fetch("/jobs/ogm-aardvark/run"' in page_html


def test_static_ogm_wisc_page_accepts_github_commit_options() -> None:
    page_html = (ROOT / "static" / "ogmWisc.html").read_text(encoding="utf-8")

    assert 'fetch("/jobs/ogmWisc/run"' in page_html
    assert 'value="github_commits" selected' in page_html
    assert 'name="github_recent_commits"' in page_html
    assert 'value="4"' in page_html
    assert 'name="github_since_date"' in page_html
    assert 'value="2026-06-01"' in page_html
    assert "payload.github_since" in page_html


def test_static_hdx_page_runs_metadata_download_from_browser() -> None:
    page_html = (ROOT / "static" / "hdx.html").read_text(encoding="utf-8")

    assert "Download Metadata" in page_html
    assert "/run-hdx-download-stream" in page_html
    assert "Got to the scripts folder" not in page_html


def test_static_umedia_page_accepts_an_inclusive_date_added_cutoff() -> None:
    page_html = (ROOT / "static" / "umedia.html").read_text(encoding="utf-8")

    assert "uMedia Harvester" in page_html
    assert 'name="date_added_on_or_after"' in page_html
    assert 'type="date"' in page_html
    assert "date_added" in page_html
    assert "/run-umedia-stream?" in page_html
    assert "new URLSearchParams" in page_html
    assert "Upload deltas are" in page_html


def test_static_ckan_page_runs_stream_endpoint_from_browser() -> None:
    index_html = (ROOT / "static" / "index.html").read_text(encoding="utf-8")
    page_html = (ROOT / "static" / "ckan.html").read_text(encoding="utf-8")

    assert "/static/ckan.html" in index_html
    assert "CKAN Harvester" in page_html
    assert "/run-ckan-stream" in page_html


def test_static_socrata_page_shows_stream_progress_and_status() -> None:
    page_html = (ROOT / "static" / "socrata.html").read_text(encoding="utf-8")

    assert "/run-socrata-stream" in page_html
    assert 'id="run-status"' in page_html
    assert 'id="run-button"' in page_html
    assert "Connecting to the Socrata harvest stream" in page_html
    assert "Socrata harvest completed successfully" in page_html
    assert "button.disabled = true" in page_html


def test_static_pasda_page_runs_portal_stream_endpoint_from_browser() -> None:
    index_html = (ROOT / "static" / "index.html").read_text(encoding="utf-8")
    page_html = (ROOT / "static" / "pasda.html").read_text(encoding="utf-8")

    assert "/static/pasda.html" in index_html
    assert "PASDA Portal Harvester" in index_html
    assert "PASDA Portal Harvester" in page_html
    assert "/run-pasda-portal-stream" in page_html
    assert "/run-pasda-stream" not in page_html


def test_static_pasda_metadata_page_runs_metadata_directory_stream_endpoint() -> None:
    index_html = (ROOT / "static" / "index.html").read_text(encoding="utf-8")
    page_html = (ROOT / "static" / "pasda-metadata.html").read_text(encoding="utf-8")

    assert "/static/pasda-metadata.html" in index_html
    assert "PASDA Metadata Directory Harvester" in index_html
    assert "PASDA Metadata Directory Harvester" in page_html
    assert "/run-pasda-stream" in page_html
    assert "/run-pasda-portal-stream" not in page_html
    assert "/static/pasda.html" in page_html
    assert "This can take awhile to run" in page_html
    assert "terminal-style messages" in page_html
    assert "run-status" in page_html
