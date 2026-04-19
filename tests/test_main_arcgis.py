import csv
import os
from pathlib import Path

from main import create_arcgis_test_input_csv


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
