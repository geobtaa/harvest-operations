from scripts.pgc_cog_pilot import format_union_bounds


def test_format_union_bounds_combines_extents():
    assert format_union_bounds([(1, 2, 3, 4), (-2, 1, 2, 5)]) == (
        "-2.000000,1.000000,3.000000,5.000000"
    )


def test_format_union_bounds_normalizes_antimeridian_crossing():
    assert format_union_bounds([(153.5, -87, -171.0, -84)]) == (
        "-180.000000,-87.000000,180.000000,-84.000000"
    )
