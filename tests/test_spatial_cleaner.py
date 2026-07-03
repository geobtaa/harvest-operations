import pandas as pd

from utils.spatial_cleaner import spatial_cleaning


def test_spatial_cleaning_uses_envelope_for_full_world_geometry():
    df = pd.DataFrame(
        [
            {
                "Bounding Box": "-180,-90,180,90",
                "Geometry": (
                    "POLYGON((-180 90, 180 90, 180 -90, "
                    "-180 -90, -180 90))"
                ),
            }
        ]
    )

    cleaned = spatial_cleaning(df)

    assert cleaned.loc[0, "Bounding Box"] == "-179.999,-89.999,179.999,89.999"
    assert cleaned.loc[0, "Geometry"] == "ENVELOPE(-180,180,90,-90)"


def test_spatial_cleaning_keeps_non_global_world_width_polygon():
    df = pd.DataFrame(
        [
            {
                "Bounding Box": "-180,-80,180,80",
                "Geometry": (
                    "POLYGON((-180 80, 180 80, 180 -80, "
                    "-180 -80, -180 80))"
                ),
            }
        ]
    )

    cleaned = spatial_cleaning(df)

    assert cleaned.loc[0, "Bounding Box"] == "-179.999,-80.000,179.999,80.000"
    assert cleaned.loc[0, "Geometry"] == (
        "POLYGON((-179.999 80.000, 179.999 80.000, "
        "179.999 -80.000, -179.999 -80.000, -179.999 80.000))"
    )


def test_spatial_cleaning_expands_degenerate_bbox_after_rounding():
    df = pd.DataFrame(
        [
            {
                "Bounding Box": "-90,44,-90,44",
                "Geometry": "POLYGON((-90 44, -90 44, -90 44, -90 44, -90 44))",
            }
        ]
    )

    cleaned = spatial_cleaning(df)

    assert cleaned.loc[0, "Bounding Box"] == "-90.000,44.000,-89.999,44.001"
    assert cleaned.loc[0, "Geometry"] == (
        "POLYGON((-90.000 44.001, -89.999 44.001, "
        "-89.999 44.000, -90.000 44.000, -90.000 44.001))"
    )


def test_spatial_cleaning_preserves_non_rectangular_geometry():
    geometry = (
        "POLYGON((-122.95 48.27, -122.95 48.35, -122.86 48.37, "
        "-122.85 48.23, -122.95 48.27))"
    )
    df = pd.DataFrame(
        [
            {
                "Bounding Box": "-124.720,47.520,-122.600,48.370",
                "Geometry": geometry,
            }
        ]
    )

    cleaned = spatial_cleaning(df)

    assert cleaned.loc[0, "Bounding Box"] == "-124.720,47.520,-122.600,48.370"
    assert cleaned.loc[0, "Geometry"] == geometry
