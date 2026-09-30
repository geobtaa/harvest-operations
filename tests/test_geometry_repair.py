import csv

from shapely import wkt
from shapely.geometry import MultiPolygon, Polygon

from scripts.repair_geometry import repair_csv
from utils.geometry_repair import repair_geometry_fields


def test_blank_geometry_becomes_envelope_from_bbox():
    result = repair_geometry_fields("-90,40,-89,41", "")

    assert result.geometry == "ENVELOPE(-90,-89,41,40)"
    assert result.action == "create_envelope_from_bbox"
    assert not result.requires_review


def test_clockwise_rectangle_becomes_envelope():
    result = repair_geometry_fields(
        "-90,40,-89,41",
        "POLYGON((-90 41, -89 41, -89 40, -90 40, -90 41))",
    )

    assert result.geometry == "ENVELOPE(-90,-89,41,40)"
    assert result.action == "replace_rectangle_with_envelope"


def test_reversed_bbox_is_normalized_before_envelope_is_created():
    result = repair_geometry_fields(
        "-89,40,-90,41",
        "POLYGON((-89 41, -90 41, -90 40, -89 40, -89 41))",
    )

    assert result.bounding_box == "-90,40,-89,41"
    assert result.geometry == "ENVELOPE(-90,-89,41,40)"
    assert "Reordered west/east" in result.note


def test_wide_rectangle_uses_envelope_instead_of_antimeridian_review():
    result = repair_geometry_fields(
        "-179,-40,179,40",
        "POLYGON((-179 40, 179 40, 179 -40, -179 -40, -179 40))",
    )

    assert result.geometry == "ENVELOPE(-179,179,40,-40)"
    assert result.action == "replace_rectangle_with_envelope"
    assert not result.requires_review


def test_antimeridian_bbox_becomes_split_multipolygon():
    result = repair_geometry_fields("169.32,-11.65,-149.99,4.9", "")

    repaired = wkt.loads(result.geometry)
    assert result.bounding_box == "169.32,-11.65,-149.99,4.9"
    assert result.action == "create_antimeridian_multipolygon_from_bbox"
    assert isinstance(repaired, MultiPolygon)
    assert len(repaired.geoms) == 2
    assert all(polygon.exterior.is_ccw for polygon in repaired.geoms)


def test_complex_polygon_and_hole_are_reoriented():
    result = repair_geometry_fields(
        "0,0,4,4",
        "POLYGON((0 0, 0 4, 4 4, 4 0, 0 0), (1 1, 3 1, 3 3, 1 3, 1 1))",
    )

    repaired = wkt.loads(result.geometry)
    assert isinstance(repaired, Polygon)
    assert repaired.exterior.is_ccw
    assert all(not ring.is_ccw for ring in repaired.interiors)
    assert result.action == "rewind_complex_geometry"


def test_multipolygon_is_reoriented_without_losing_components():
    source = "MULTIPOLYGON (((0 0, 0 2, 2 1, 0 0)), ((3 0, 3 2, 5 1, 3 0)))"
    result = repair_geometry_fields("0,0,5,2", source)

    repaired = wkt.loads(result.geometry)
    assert isinstance(repaired, MultiPolygon)
    assert len(repaired.geoms) == 2
    assert all(polygon.exterior.is_ccw for polygon in repaired.geoms)


def test_invalid_complex_geometry_uses_envelope_and_requires_review():
    result = repair_geometry_fields(
        "0,0,2,2",
        "POLYGON((0 0, 2 2, 0 2, 2 0, 0 0))",
    )

    assert result.geometry == "ENVELOPE(0,2,2,0)"
    assert result.action == "replace_invalid_topology_with_envelope"
    assert result.requires_review


def test_missing_bbox_and_geometry_are_valid_blank_spatial_fields():
    result = repair_geometry_fields("", "")

    assert result.geometry == ""
    assert result.action == "retain_blank_spatial_fields"
    assert not result.requires_review


def test_geometry_without_bbox_is_cleared_and_requires_review():
    result = repair_geometry_fields("", "ENVELOPE(-90,-89,41,40)")

    assert result.bounding_box == ""
    assert result.geometry == ""
    assert result.action == "clear_geometry_without_bbox"
    assert result.requires_review


def test_csv_repair_changes_only_spatial_fields(tmp_path):
    input_path = tmp_path / "records.csv"
    output_path = tmp_path / "records-fixed.csv"
    with input_path.open("w", encoding="utf-8", newline="") as destination:
        writer = csv.DictWriter(
            destination,
            fieldnames=["ID", "Title", "Bounding Box", "Geometry", "Rights"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "ID": "one",
                "Title": "Example",
                "Bounding Box": "-89,40,-90,41",
                "Geometry": "POLYGON((-89 41, -90 41, -90 40, -89 40, -89 41))",
                "Rights": "Public domain",
            }
        )

    reports, counts = repair_csv(input_path, output_path)

    with output_path.open(encoding="utf-8", newline="") as source:
        repaired = next(csv.DictReader(source))
    assert repaired == {
        "ID": "one",
        "Title": "Example",
        "Bounding Box": "-90,40,-89,41",
        "Geometry": "ENVELOPE(-90,-89,41,40)",
        "Rights": "Public domain",
    }
    assert reports[0]["action"] == "replace_rectangle_with_envelope"
    assert counts["changed_rows"] == 1
