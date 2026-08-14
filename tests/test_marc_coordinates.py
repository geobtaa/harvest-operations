import pytest

from scripts.convert_bboxes import convert_bbox_value
from utils.marc_coordinates import (
    first_marc_bbox,
    marc_bbox_to_decimal,
)


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        (
            "W0931600 W0931500 N0450100 N0445900",
            "-93.266667,44.983333,-93.25,45.016667",
        ),
        (
            "(W 93°16'--W 93°15'/N 45°01'--N 44°59')",
            "-93.266667,44.983333,-93.25,45.016667",
        ),
        (
            "E0250000 W1691900 N0795900 S0200000",
            "-169.316667,-20,25,79.983333",
        ),
        (
            "-26.50 60.00 40.333333 -40.333333",
            "-26.5,-40.333333,60,40.333333",
        ),
    ],
)
def test_marc_bbox_to_decimal_supports_source_coordinate_formats(source, expected):
    assert marc_bbox_to_decimal(source) == expected


def test_first_marc_bbox_uses_the_first_parseable_coordinate_value():
    assert first_marc_bbox(
        [
            "Scale 1:15,840",
            "W0740145 W0735716 N0404500 N0404158",
        ]
    ) == "-74.029167,40.699444,-73.954444,40.75"


def test_marc_bbox_to_decimal_rejects_non_coordinates_and_invalid_dms():
    assert marc_bbox_to_decimal("Not given.") == ""
    assert marc_bbox_to_decimal("W0939900 W0931500 N0450100 N0445900") == ""


def test_chicago_luna_script_uses_shared_converter_without_changing_statuses():
    assert convert_bbox_value("W 96deg56'00\"-W 89deg42'00\"/N 16deg20'00\"-N 12deg13'00\"") == (
        "-96.933333,12.216667,-89.7,16.333333",
        "converted",
    )
    assert convert_bbox_value("2 maps") == ("2 maps", "preserved")
