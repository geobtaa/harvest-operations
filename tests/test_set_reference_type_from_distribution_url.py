import csv

from scripts.set_reference_type_from_distribution_url import (
    derive_reference_type,
    update_reference_types,
    write_rows,
)


def test_derive_reference_type_uses_expected_rules() -> None:
    assert derive_reference_type("https://example.com/rest/services/demo/MapServer/0") == (
        "arcgis_dynamic_map_layer"
    )
    assert derive_reference_type("https://example.com/rest/services/demo/FeatureServer/1") == (
        "arcgis_feature_layer"
    )
    assert derive_reference_type("https://example.com/rest/services/demo/ImageServer") == (
        "arcgis_image_map_layer"
    )
    assert derive_reference_type("https://example.com/docs") == "documentation_external"


def test_derive_reference_type_is_case_insensitive() -> None:
    assert derive_reference_type("https://example.com/rest/services/demo/featureserver/1") == (
        "arcgis_feature_layer"
    )


def test_update_reference_types_rewrites_reference_type_column(tmp_path) -> None:
    rows = [
        {
            "friendlier_id": "row-1",
            "reference_type": "",
            "distribution_url": "https://example.com/rest/services/demo/MapServer/0",
            "label": "",
        },
        {
            "friendlier_id": "row-2",
            "reference_type": "wrong_value",
            "distribution_url": "https://example.com/rest/services/demo/FeatureServer/1",
            "label": "",
        },
        {
            "friendlier_id": "row-3",
            "reference_type": "",
            "distribution_url": "https://example.com/rest/services/demo/ImageServer",
            "label": "",
        },
        {
            "friendlier_id": "row-4",
            "reference_type": "",
            "distribution_url": "https://example.com/about",
            "label": "",
        },
    ]

    updated_rows = update_reference_types(rows)

    assert [row["reference_type"] for row in updated_rows] == [
        "arcgis_dynamic_map_layer",
        "arcgis_feature_layer",
        "arcgis_image_map_layer",
        "documentation_external",
    ]

    output_csv = tmp_path / "output.csv"
    fieldnames = ["friendlier_id", "reference_type", "distribution_url", "label"]
    write_rows(output_csv, fieldnames, updated_rows)

    with output_csv.open("r", newline="", encoding="utf-8") as handle:
        written_rows = list(csv.DictReader(handle))

    assert [row["reference_type"] for row in written_rows] == [
        "arcgis_dynamic_map_layer",
        "arcgis_feature_layer",
        "arcgis_image_map_layer",
        "documentation_external",
    ]
