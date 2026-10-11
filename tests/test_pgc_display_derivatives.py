import numpy as np
import pytest

from scripts.pgc_display_derivatives import (
    WEB_MERCATOR_HALF_WORLD,
    format_union_bounds,
    planned_dimensions,
    planned_projected_dimensions,
    to_byte,
    web_mercator_bounds,
)


def test_planned_dimensions_leaves_display_sized_raster_unchanged():
    assert planned_dimensions(9_000, 10_000, 100_000_000, 12_000) == (9_000, 10_000)


def test_planned_dimensions_honors_pixel_and_dimension_limits():
    width, height = planned_dimensions(34_000, 32_000, 100_000_000, 12_000)

    assert width <= 12_000
    assert height <= 12_000
    assert width * height <= 100_000_000
    assert width / height == pytest.approx(34_000 / 32_000, rel=0.001)


def test_format_union_bounds_normalizes_antimeridian_crossing():
    assert format_union_bounds([(153.5, -87, -171.0, -84)]) == (
        "-180.000000,-87.000000,180.000000,-84.000000"
    )


def test_to_byte_stretches_non_byte_data():
    source = np.ma.array([[0, 100], [200, 300]], dtype=np.uint16)

    result = to_byte(source)

    assert result.dtype == np.uint8
    assert result.min() == 0
    assert result.max() == 255


def test_web_mercator_bounds_clip_a_pole_centered_map():
    result = web_mercator_bounds((-180.0, -90.0, 180.0, -60.0))

    assert result[0] == pytest.approx(-WEB_MERCATOR_HALF_WORLD)
    assert result[1] == pytest.approx(-WEB_MERCATOR_HALF_WORLD)
    assert result[2] == pytest.approx(WEB_MERCATOR_HALF_WORLD)
    assert result[3] == pytest.approx(-8_399_737.89, abs=1)


def test_planned_projected_dimensions_uses_square_pixels():
    bounds = (-4_000_000.0, -1_000_000.0, 4_000_000.0, 1_000_000.0)

    width, height = planned_projected_dimensions(
        bounds, source_pixels=10_000_000, maximum_pixels=10_000_000,
        maximum_dimension=12_000
    )

    assert width / height == pytest.approx(4.0, rel=0.001)
    assert width * height <= 10_010_000
