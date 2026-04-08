from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_static_index_links_to_ogm_aardvark_page() -> None:
    index_html = (ROOT / "static" / "index.html").read_text(encoding="utf-8")

    assert '/static/ogm-aardvark.html' in index_html
    assert "OpenGeoMetadata Aardvark Harvester" in index_html


def test_static_ogm_aardvark_page_runs_expected_job() -> None:
    page_html = (ROOT / "static" / "ogm-aardvark.html").read_text(encoding="utf-8")

    assert "inputs/ogm_aardvark" in page_html
    assert "outputs/YYYY-MM-DD_ogm_aardvark_primary.csv" in page_html
    assert 'fetch("/jobs/ogm-aardvark/run"' in page_html
