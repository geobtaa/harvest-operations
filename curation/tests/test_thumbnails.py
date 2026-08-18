from __future__ import annotations

import sys
from pathlib import Path

import geopandas as gpd
import pytest
from PIL import Image
from shapely.geometry import LineString, box

CURATION_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CURATION_ROOT / "src"))

from curation.thumbnails import (  # noqa: E402
    BACKGROUND_COLOR,
    DEFAULT_MINIMUM_SIDE,
    LINE_COLOR,
    MAX_POINT_MARKER_AREA,
    MIN_POINT_MARKER_AREA,
    POINT_COLOR,
    POLYGON_EDGE_COLOR,
    POLYGON_FILL_COLOR,
    _marker_area_for_point_count,
    create_vector_thumbnail,
)


def test_thumbnail_short_side_is_600_pixels(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dataframe = gpd.GeoDataFrame(geometry=[box(0, 0, 2, 1)])
    monkeypatch.setattr(gpd, "read_file", lambda _path: dataframe)
    output_path = tmp_path / "nested" / "thumbnail.png"

    create_vector_thumbnail(Path("dataset.gpkg"), output_path)

    with Image.open(output_path) as image:
        assert min(image.size) == DEFAULT_MINIMUM_SIDE
        assert image.mode == "RGBA"


def test_thumbnail_uses_the_documented_palette(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dataframe = gpd.GeoDataFrame(geometry=[box(0, 0, 1, 1)])
    plot_arguments: dict[str, object] = {}
    original_plot = dataframe.plot

    def capture_plot_arguments(*args: object, **kwargs: object) -> object:
        plot_arguments.update(kwargs)
        return original_plot(*args, **kwargs)

    monkeypatch.setattr(dataframe, "plot", capture_plot_arguments)
    monkeypatch.setattr(gpd, "read_file", lambda _path: dataframe)

    create_vector_thumbnail(Path("dataset.gpkg"), tmp_path / "thumbnail.png")

    assert plot_arguments["color"] == POLYGON_FILL_COLOR
    assert plot_arguments["edgecolor"] == POLYGON_EDGE_COLOR
    assert BACKGROUND_COLOR is None


def test_thumbnail_rejects_non_positive_minimum_side(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="at least 1 pixel"):
        create_vector_thumbnail(
            Path("dataset.gpkg"),
            tmp_path / "thumbnail.png",
            minimum_side=0,
        )


def test_point_marker_area_decreases_as_point_count_grows() -> None:
    assert _marker_area_for_point_count(100) == MAX_POINT_MARKER_AREA
    assert _marker_area_for_point_count(20_000) == 0.2
    assert _marker_area_for_point_count(300_000) == MIN_POINT_MARKER_AREA


def test_point_thumbnail_uses_solid_markers_without_bubbly_edges(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dataframe = gpd.GeoDataFrame(geometry=gpd.points_from_xy([0, 1], [0, 1]))
    plot_arguments: dict[str, object] = {}
    original_plot = dataframe.plot

    def capture_plot_arguments(*args: object, **kwargs: object) -> object:
        plot_arguments.update(kwargs)
        return original_plot(*args, **kwargs)

    monkeypatch.setattr(dataframe, "plot", capture_plot_arguments)
    monkeypatch.setattr(gpd, "read_file", lambda _path: dataframe)

    create_vector_thumbnail(Path("points.gpkg"), tmp_path / "points.png")

    assert plot_arguments["color"] == POINT_COLOR
    assert plot_arguments["edgecolor"] == "none"
    assert plot_arguments["linewidth"] == 0
    assert plot_arguments["markersize"] == MAX_POINT_MARKER_AREA


def test_line_thumbnail_uses_line_color(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dataframe = gpd.GeoDataFrame(geometry=[LineString([(0, 0), (1, 1)])])
    plot_arguments: dict[str, object] = {}
    original_plot = dataframe.plot

    def capture_plot_arguments(*args: object, **kwargs: object) -> object:
        plot_arguments.update(kwargs)
        return original_plot(*args, **kwargs)

    monkeypatch.setattr(dataframe, "plot", capture_plot_arguments)
    monkeypatch.setattr(gpd, "read_file", lambda _path: dataframe)

    create_vector_thumbnail(Path("lines.gpkg"), tmp_path / "lines.png")

    assert plot_arguments["color"] == LINE_COLOR
    assert plot_arguments["edgecolor"] == "none"


def test_thumbnail_can_use_a_solid_background(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    dataframe = gpd.GeoDataFrame(geometry=[box(0, 0, 1, 1)])
    monkeypatch.setattr(gpd, "read_file", lambda _path: dataframe)
    output_path = tmp_path / "thumbnail.png"

    create_vector_thumbnail(
        Path("dataset.gpkg"),
        output_path,
        background_color="#FFFFFF",
    )

    with Image.open(output_path) as image:
        assert image.getpixel((0, 0)) == (255, 255, 255, 255)
