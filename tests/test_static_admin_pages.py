from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_static_index_links_to_ogm_aardvark_page() -> None:
    index_html = (ROOT / "static" / "index.html").read_text(encoding="utf-8")

    assert '/static/ogm-aardvark.html' in index_html
    assert "OpenGeoMetadata Aardvark Harvester" in index_html


def test_static_ogm_aardvark_page_runs_expected_job() -> None:
    page_html = (ROOT / "static" / "ogm-aardvark.html").read_text(encoding="utf-8")

    assert "inputs/edu.utexas" in page_html
    assert "outputs/YYYY-MM-DD_ogm_aardvark_primary.csv" in page_html
    assert 'fetch("/jobs/ogm-aardvark/run"' in page_html


def test_static_hdx_page_runs_metadata_download_from_browser() -> None:
    page_html = (ROOT / "static" / "hdx.html").read_text(encoding="utf-8")

    assert "Download Metadata" in page_html
    assert "/run-hdx-download-stream" in page_html
    assert "Got to the scripts folder" not in page_html


def test_static_ckan_page_runs_stream_endpoint_from_browser() -> None:
    index_html = (ROOT / "static" / "index.html").read_text(encoding="utf-8")
    page_html = (ROOT / "static" / "ckan.html").read_text(encoding="utf-8")

    assert "/static/ckan.html" in index_html
    assert "CKAN Harvester" in page_html
    assert "/run-ckan-stream" in page_html
