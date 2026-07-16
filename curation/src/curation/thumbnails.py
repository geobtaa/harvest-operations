"""Thumbnail creation helpers for curated vector datasets."""

from __future__ import annotations

import logging
from pathlib import Path


def create_vector_thumbnail(
    vector_path: Path,
    thumbnail_path: Path,
    *,
    width: int = 2,
    height: int = 2,
    dpi: int = 100,
) -> None:
    """Render a compact, transparent-background thumbnail for a vector dataset."""
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

    thumbnail_path.parent.mkdir(parents=True, exist_ok=True)
    figure, axis = plt.subplots(figsize=(width, height), dpi=dpi)
    try:
        dataframe.plot(ax=axis, color="#526d82", edgecolor="#263746", linewidth=0.25)
        axis.set_axis_off()
        figure.savefig(
            thumbnail_path,
            bbox_inches="tight",
            pad_inches=0,
            transparent=True,
        )
    finally:
        plt.close(figure)
