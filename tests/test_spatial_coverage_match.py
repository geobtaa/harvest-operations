from shapely import wkt

from scripts.spatial_coverage_match import combine_geometries


def test_combine_geometries_keeps_single_envelope():
    geometry = "ENVELOPE(-90,-89,45,44)"

    assert combine_geometries(geometry) == geometry


def test_combine_geometries_converts_envelopes_to_oriented_multipolygon():
    combined = combine_geometries("ENVELOPE(-90,-89,45,44)|ENVELOPE(-88,-87,43,42)")

    geometry = wkt.loads(combined)
    assert geometry.geom_type == "MultiPolygon"
    assert len(geometry.geoms) == 2
    assert all(polygon.exterior.is_ccw for polygon in geometry.geoms)
