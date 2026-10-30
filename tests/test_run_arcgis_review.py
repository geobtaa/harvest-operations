import csv
from pathlib import Path
import shutil
import time

import pandas as pd
import pytest
import yaml

from scripts.run_arcgis_review import run_review


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("duplicate_ids", [False, True])
def test_review_harvests_only_ca_portals_and_preserves_registry(tmp_path, monkeypatch, duplicate_ids):
    for directory in ("config", "schemas", "reference_data", "registry"):
        (tmp_path / directory).mkdir()
    for relative_path in (
        "config/arcgis.yaml",
        "schemas/distribution_types.yaml",
        "reference_data/themes.csv",
        "reference_data/websites.csv",
    ):
        shutil.copyfile(ROOT / relative_path, tmp_path / relative_path)

    # Even an explicitly enabled upload setting must be overridden for review.
    config_path = tmp_path / "config/arcgis.yaml"
    config = yaml.safe_load(config_path.read_text())
    config["build_uploads"] = True
    config["duplicate_id_code_priority"] = ["18c-01", "18b-01", "18d-01", "18a-01"]
    config_path.write_text(yaml.safe_dump(config))
    registry = tmp_path / "registry"
    (registry / "arcgis_primary_registry.csv").write_text(
        "ID,Code,Resource Class\nexisting,05c-01,Web services\n"
    )
    (registry / "arcgis_distributions_registry.csv").write_text(
        "friendlier_id,reference_type,distribution_url,label\n"
        "existing,download,https://example.org/old.zip,Shapefile\n"
    )
    original_registry = {path.name: path.read_bytes() for path in registry.iterdir()}
    original_config = config_path.read_bytes()
    today = time.strftime("%Y-%m-%d")
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    regular_output = outputs / f"{today}_arcgis_primary.csv"
    regular_output.write_text("existing regular output\n")

    source_csv = ROOT / "inputs/harvest-workflow-inputs/py-arcgis-hub-ca.csv"
    original_input = source_csv.read_bytes()
    with source_csv.open(newline="", encoding="utf-8-sig") as handle:
        portals = list(csv.DictReader(handle))
    expected_urls = [portal["Endpoint URL"] for portal in portals]
    requested_urls = []

    class Response:
        def raise_for_status(self):
            pass

        def json(self):
            return {
                "dataset": [{
                    "title": "Parcels 2024",
                    "identifier": f"https://hub.arcgis.com/datasets?id=item{1 if duplicate_ids else len(requested_urls)}",
                    "landingPage": "https://example.org/datasets/parcels",
                    "publisher": {"name": "California GIS"},
                    "spatial": "-118.5,33.9,-118.1,34.2",
                    "distribution": [{
                        "title": "Shapefile",
                        "accessURL": "https://example.org/parcels.zip",
                    }, {
                        "title": "ArcGIS GeoService",
                        "accessURL": "https://example.org/arcgis/rest/services/Parcels/FeatureServer/0",
                    }],
                }]
            }

    def fake_get(url, timeout):
        assert url in expected_urls
        requested_urls.append(url)
        return Response()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("harvesters.arcgis.requests.get", fake_get)
    results = run_review(source_csv)

    assert requested_urls == expected_urls
    assert len(requested_urls) == 4
    assert "upload_summary" not in results
    assert not (outputs / "to_upload").exists()
    assert {path.name: path.read_bytes() for path in registry.iterdir()} == original_registry
    assert config_path.read_bytes() == original_config
    assert source_csv.read_bytes() == original_input
    assert regular_output.read_text() == "existing regular output\n"
    primary = pd.read_csv(results["primary_csv"], keep_default_na=False)
    assert set(primary["Code"]) == {portal["Code"] for portal in portals}
    assert len(primary) == (5 if duplicate_ids else 8)  # Datasets plus four harvest records.
    if duplicate_ids:
        assert primary.loc[primary["ID"] == "item1", "Code"].tolist() == ["18c-01"]
    assert not pd.read_csv(results["distributions_csv"]).empty
    report = pd.read_csv(results["report_csv"])
    assert report.iloc[-1]["Total Records Found"] == 4
    for path in results.values():
        assert "review_py-arcgis-hub-ca_" in Path(path).name


def test_selected_review_passes_only_mngeo_to_entire_pipeline(monkeypatch):
    source_csv = ROOT / "inputs/harvest-workflow-inputs/py-arcgis-hub.csv"
    original_input = source_csv.read_bytes()
    captured = {}

    class FakeHarvester:
        def __init__(self, config):
            captured.update(config)

        def harvest_pipeline(self):
            with open(captured["input_csv"], newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            assert len(rows) == 1
            assert rows[0]["ID"] == "harvest_05a-01"
            assert rows[0]["Endpoint URL"] == "https://gis.data.mn.gov/api/feed/dcat-us/1.1.json"
            return {}

    monkeypatch.chdir(ROOT)
    monkeypatch.setattr("scripts.run_arcgis_review.ArcGISHarvester", FakeHarvester)
    run_review(source_csv, harvest_id="harvest_05a-01")
    assert captured["build_uploads"] is False
    assert captured["parent_sublayer_context"] is True
    for key in ("output_primary_csv", "output_distributions_csv", "output_report_csv"):
        assert "review_py-arcgis-hub_harvest_05a-01_" in captured[key]
    assert not Path(captured["input_csv"]).exists()
    assert source_csv.read_bytes() == original_input


@pytest.mark.parametrize("content,count", [
    ("ID,Title\nother,Other\n", 0),
    ("ID,Title\nharvest_05a-01,One\nharvest_05a-01,Two\n", 2),
])
def test_selected_review_rejects_missing_or_ambiguous_id(tmp_path, monkeypatch, content, count):
    source_csv = tmp_path / "input.csv"
    source_csv.write_text(content)
    monkeypatch.chdir(ROOT)
    with pytest.raises(ValueError, match=f"found {count}"):
        run_review(source_csv, harvest_id="harvest_05a-01")
