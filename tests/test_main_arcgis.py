import asyncio
import csv
import os
from pathlib import Path

from main import app, create_arcgis_test_input_csv


def test_create_arcgis_test_input_csv_writes_three_sampled_rows(tmp_path: Path) -> None:
    source_csv = tmp_path / "py-arcgis-hub.csv"
    source_csv.write_text(
        "\n".join(
            [
                "Title,Endpoint URL,Harvest Workflow,ID,Identifier,Code",
                "Hub One,https://example.org/1,py_arcgis_hub,harvest_01,01,01",
                "Hub Two,https://example.org/2,py_arcgis_hub,harvest_02,02,02",
                "Hub Three,https://example.org/3,py_arcgis_hub,harvest_03,03,03",
                "Hub Four,https://example.org/4,py_arcgis_hub,harvest_04,04,04",
            ]
        ),
        encoding="utf-8",
    )

    temp_path, selected_count = create_arcgis_test_input_csv(str(source_csv))

    try:
        assert selected_count == 3

        with open(temp_path, newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            rows = list(reader)

        assert reader.fieldnames == [
            "Title",
            "Endpoint URL",
            "Harvest Workflow",
            "ID",
            "Identifier",
            "Code",
        ]
        assert len(rows) == 3
        assert {row["Title"] for row in rows}.issubset(
            {"Hub One", "Hub Two", "Hub Three", "Hub Four"}
        )
    finally:
        if os.path.exists(temp_path):
            os.unlink(temp_path)


def test_app_registers_hdx_download_stream_route() -> None:
    assert any(route.path == "/run-hdx-download-stream" for route in app.routes)


def test_app_registers_ckan_stream_route() -> None:
    assert any(route.path == "/run-ckan-stream" for route in app.routes)


def test_hdx_stream_builds_upload_files(monkeypatch) -> None:
    class FakeHdxHarvester:
        def __init__(self, config):
            self.config = config

        def load_reference_data(self):
            return None

        def fetch(self):
            return [{"id": "one"}]

        def parse(self, raw):
            return raw

        def flatten(self, parsed):
            return parsed

        def build_dataframe(self, flat):
            return flat

        def derive_fields(self, df):
            return df

        def add_defaults(self, df):
            return df

        def add_provenance(self, df):
            return df

        def clean(self, df):
            return df

        def validate(self, df):
            return df

        def write_outputs(self, df):
            return {
                "primary_csv": "outputs/2026-05-29_hdx_primary.csv",
                "distributions_csv": "outputs/2026-05-29_hdx_distributions.csv",
            }

        def build_uploads(self, results):
            return {
                "status": "created",
                "primary_upload_csv": "outputs/to_upload/2026-05-29_hdx_primary_upload.csv",
                "distributions_new_csv": "outputs/to_upload/2026-05-29_hdx_distributions_new.csv",
                "distributions_delete_csv": "outputs/to_upload/2026-05-29_hdx_distributions_delete.csv",
            }

    import harvesters.hdx

    monkeypatch.setattr(harvesters.hdx, "HdxHarvester", FakeHdxHarvester)

    import main

    async def collect_stream() -> str:
        response = await main.run_hsx_stream()
        chunks = []
        async for chunk in response.body_iterator:
            chunks.append(chunk.decode("utf-8") if isinstance(chunk, bytes) else chunk)
        return "".join(chunks)

    body = asyncio.run(collect_stream())

    assert "Built upload files:" in body
    assert "outputs/to_upload/2026-05-29_hdx_primary_upload.csv" in body
