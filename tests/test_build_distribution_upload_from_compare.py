from pathlib import Path

import pandas as pd

from scripts.build_distribution_upload_from_compare import build_new_rows_for_upload


def write_distribution_csv(path: Path, rows: list[dict[str, str]]) -> None:
    pd.DataFrame(rows).to_csv(path, index=False)


def test_build_new_rows_for_upload_returns_exact_row_additions() -> None:
    new_df = pd.DataFrame(
        [
            {
                "friendlier_id": "row-1",
                "reference_type": "arcgis_dynamic_map_layer",
                "distribution_url": "https://example.com/MapServer/0",
                "label": "",
            },
            {
                "friendlier_id": "row-2",
                "reference_type": "arcgis_feature_layer",
                "distribution_url": "https://example.com/FeatureServer/1",
                "label": "",
            },
            {
                "friendlier_id": "row-2",
                "reference_type": "arcgis_feature_layer",
                "distribution_url": "https://example.com/FeatureServer/1",
                "label": "",
            },
        ]
    )
    current_df = pd.DataFrame(
        [
            {
                "friendlier_id": "row-1",
                "reference_type": "arcgis_dynamic_map_layer",
                "distribution_url": "https://example.com/MapServer/0",
                "label": "",
            }
        ]
    )

    upload_df = build_new_rows_for_upload(new_df, current_df)

    assert upload_df.to_dict(orient="records") == [
        {
            "friendlier_id": "row-2",
            "reference_type": "arcgis_feature_layer",
            "distribution_url": "https://example.com/FeatureServer/1",
            "label": "",
        }
    ]


def test_build_new_rows_for_upload_treats_changed_reference_type_as_new_row() -> None:
    new_df = pd.DataFrame(
        [
            {
                "friendlier_id": "row-1",
                "reference_type": "arcgis_dynamic_map_layer",
                "distribution_url": "https://example.com/MapServer/0",
                "label": "",
            }
        ]
    )
    current_df = pd.DataFrame(
        [
            {
                "friendlier_id": "row-1",
                "reference_type": "",
                "distribution_url": "https://example.com/MapServer/0",
                "label": "",
            }
        ]
    )

    upload_df = build_new_rows_for_upload(new_df, current_df)

    assert upload_df.to_dict(orient="records") == [
        {
            "friendlier_id": "row-1",
            "reference_type": "arcgis_dynamic_map_layer",
            "distribution_url": "https://example.com/MapServer/0",
            "label": "",
        }
    ]
