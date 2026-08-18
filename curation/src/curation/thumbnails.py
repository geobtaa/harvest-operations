"""Create consistently sized PNG previews of curated vector datasets.
"""

from __future__ import annotations

import logging
from pathlib import Path


# Feature colors
POLYGON_FILL_COLOR = "#1F6FB2"
POLYGON_EDGE_COLOR = "#17324D" 
LINE_COLOR = "#1F6FB2"
POINT_COLOR = "#1F6FB2"
BACKGROUND_COLOR: str | None = None  # None keeps the PNG background transparent.


# side of DEFAULT_MINIMUM_SIDE pixels.
WORKING_FIGURE_SIZE_INCHES = 2
WORKING_DPI = 100
DEFAULT_MINIMUM_SIDE = 600

# Matplotlib expresses scatter marker size as area in typographic points squared.
# Point layers use an inverse-density scale: sparse layers can use a visible dot,
# while very dense layers approach a roughly one-pixel mark in the final image.
MIN_POINT_MARKER_AREA = 0.05
MAX_POINT_MARKER_AREA = 2.0
POINT_MARKER_AREA_BUDGET = 4_000


def create_vector_thumbnail(
    vector_path: Path,
    thumbnail_path: Path,
    *,
    minimum_side: int = DEFAULT_MINIMUM_SIDE,
    polygon_fill_color: str = POLYGON_FILL_COLOR,
    polygon_edge_color: str = POLYGON_EDGE_COLOR,
    line_color: str = LINE_COLOR,
    point_color: str = POINT_COLOR,
    background_color: str | None = BACKGROUND_COLOR,
    point_marker_area: float | None = None,
) -> None:
    """Render a tightly cropped PNG preview of a vector dataset.

    ``minimum_side`` refers to the final image after transparent margins are
    cropped.  The other side remains proportional and may therefore be longer.
    Each geometry family has its own color argument, and ``background_color``
    accepts a Matplotlib color or ``None`` for transparency. Point layers
    automatically use smaller markers as their point count grows; pass
    ``point_marker_area`` to override that calculated marker area.
    """
    if minimum_side < 1:
        raise ValueError("minimum_side must be at least 1 pixel")
    if point_marker_area is not None and point_marker_area <= 0:
        raise ValueError("point_marker_area must be greater than 0")

    # Matplotlib scans macOS system fonts on first use and logs harmless failures
    # for reserved/private font files. Thumbnails contain no text, so hide that noise.
    logging.getLogger("matplotlib.font_manager").setLevel(logging.WARNING)
    try:
        import geopandas as gpd
        import matplotlib.pyplot as plt
    except ImportError as exc:
        raise RuntimeError(
            "Thumbnail creation requires geopandas and matplotlib from the curation environment"
        ) from exc

    dataframe = gpd.read_file(vector_path)
    if dataframe.empty:
        raise RuntimeError(f"Cannot create a thumbnail for an empty dataset: {vector_path}")

    # Create the destination lazily, after the source has been read successfully.
    thumbnail_path.parent.mkdir(parents=True, exist_ok=True)
    figure, axis = plt.subplots(
        figsize=(WORKING_FIGURE_SIZE_INCHES, WORKING_FIGURE_SIZE_INCHES),
        dpi=WORKING_DPI,
    )
    try:
        plot_options: dict[str, object] = {
            "ax": axis,
            "color": polygon_fill_color,
            "edgecolor": polygon_edge_color,
            "linewidth": 0.25,
        }

        geometry_types = set(dataframe.geom_type.dropna())
        is_point_layer = bool(geometry_types) and geometry_types <= {"Point", "MultiPoint"}
        is_line_layer = bool(geometry_types) and geometry_types <= {
            "LineString",
            "LinearRing",
            "MultiLineString",
        }
        if is_point_layer:
            # At thumbnail scale a border makes tiny point symbols look like
            # bubbles. Draw solid dots instead, and shrink them as density rises.
            point_count = int(dataframe.geometry.count_coordinates().sum())
            plot_options.update(
                color=point_color,
                edgecolor="none",
                linewidth=0,
                markersize=(
                    point_marker_area
                    if point_marker_area is not None
                    else _marker_area_for_point_count(point_count)
                ),
            )
        elif is_line_layer:
            # Lines have no interior-versus-edge distinction, so they receive
            # one color and retain the standard thumbnail line width.
            plot_options.update(color=line_color, edgecolor="none")

        dataframe.plot(**plot_options)
        axis.set_axis_off()

        # ``bbox_inches="tight"`` below removes unused canvas space.  Because
        # that crop changes the pixel dimensions, measure its physical size and
        # choose a save DPI that makes the shorter side exactly 600 px by default.
        figure.canvas.draw()
        crop = figure.get_tightbbox(figure.canvas.get_renderer())
        shortest_side_inches = min(crop.width, crop.height)
        if shortest_side_inches <= 0:
            raise RuntimeError(f"Cannot determine thumbnail bounds for: {vector_path}")
        output_dpi = minimum_side / shortest_side_inches

        figure.savefig(
            thumbnail_path,
            bbox_inches="tight",
            pad_inches=0,
            transparent=background_color is None,
            facecolor=background_color if background_color is not None else "none",
            dpi=output_dpi,
        )
    finally:
        # Explicitly close the figure so a batch run does not accumulate memory.
        plt.close(figure)


def _marker_area_for_point_count(point_count: int) -> float:
    """Return a legible marker area that decreases for denser point layers."""
    unconstrained_area = POINT_MARKER_AREA_BUDGET / max(point_count, 1)
    return max(MIN_POINT_MARKER_AREA, min(MAX_POINT_MARKER_AREA, unconstrained_area))
