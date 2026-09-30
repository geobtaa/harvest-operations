import pandas as pd

from utils.validation import geometry_validation_error, validation_pipeline


def test_blank_bbox_and_geometry_are_valid():
    assert geometry_validation_error("", "") == ""
    assert geometry_validation_error(pd.NA, pd.NA) == ""


def test_geometry_must_be_blank_without_bbox():
    assert geometry_validation_error("", "ENVELOPE(-90,-89,41,40)") == (
        "Geometry must be blank when Bounding Box is blank."
    )


def test_geometry_is_required_with_bbox():
    assert geometry_validation_error("-90,40,-89,41", "") == (
        "Geometry is required when Bounding Box is populated."
    )


def test_matching_envelope_is_valid():
    assert geometry_validation_error("-90,40,-89,41", "ENVELOPE(-90,-89,41,40)") == ""


def test_envelope_must_match_bbox():
    assert (
        geometry_validation_error("-90,40,-89,41", "ENVELOPE(-90,-88,41,40)")
        == "ENVELOPE coordinates do not match Bounding Box."
    )


def test_clockwise_complex_polygon_is_invalid():
    error = geometry_validation_error("0,0,4,4", "POLYGON((0 0,0 4,4 3,4 0,0 0))")

    assert error == "Polygon exterior rings must be counter-clockwise."


def test_counter_clockwise_complex_polygon_is_valid():
    assert geometry_validation_error("0,0,4,4", "POLYGON((0 0,4 0,4 3,0 4,0 0))") == ""


def test_validation_pipeline_accepts_blank_spatial_fields(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    dataframe = pd.DataFrame(
        [
            {
                "ID": "no-spatial-data",
                "Title": "Example",
                "Access Rights": "Public",
                "Resource Class": "Datasets",
                "Bounding Box": "",
                "Geometry": "",
            }
        ]
    )

    result = validation_pipeline(dataframe)

    assert result is dataframe
    assert not (tmp_path / "outputs" / "invalid_bboxes.csv").exists()
    assert not (tmp_path / "outputs" / "invalid_geometry.csv").exists()
