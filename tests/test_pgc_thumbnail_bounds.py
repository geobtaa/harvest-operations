from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
from PIL import Image
from rasterio.transform import from_origin

from scripts.pgc_thumbnail_bounds import (
    create_thumbnail,
    migrate_legacy_thumbnail_layout,
    thumbnail_dimensions,
)


def test_thumbnail_dimensions_make_shortest_side_the_configured_minimum():
    assert thumbnail_dimensions(1200, 800, 600) == (900, 600)
    assert thumbnail_dimensions(800, 1200, 600) == (600, 900)


def test_create_thumbnail_writes_lossless_rgb_png(tmp_path: Path):
    source = tmp_path / "source.tif"
    destination = tmp_path / "thumbnail.png"
    data = np.zeros((3, 800, 1200), dtype=np.uint8)
    data[0] = 180
    data[1] = 120
    data[2] = 60
    with rasterio.open(
        source,
        "w",
        driver="GTiff",
        width=1200,
        height=800,
        count=3,
        dtype="uint8",
        crs="EPSG:4326",
        transform=from_origin(-10, 10, 0.01, 0.01),
    ) as dataset:
        dataset.write(data)

    dimensions = create_thumbnail(source, destination, minimum_side=600, quality=95)

    assert dimensions == (900, 600)
    with Image.open(destination) as thumbnail:
        assert thumbnail.size == (900, 600)
        assert thumbnail.mode == "RGB"
        assert thumbnail.format == "PNG"


def test_migrate_legacy_thumbnail_layout_flattens_names_and_manifest(tmp_path: Path):
    old_directory = tmp_path / "thumbnails" / "antarctica" / "standard"
    old_directory.mkdir(parents=True)
    old_thumbnail = old_directory / "ANT-TEST-001__standard.png"
    old_thumbnail.write_bytes(b"png")
    manifests = tmp_path / "manifests"
    manifests.mkdir()
    manifest = manifests / "thumbnail_bounds_standard_results.csv"
    pd.DataFrame(
        [{"thumbnail_path": str(old_thumbnail), "status": "complete"}]
    ).to_csv(manifest, index=False)

    migrate_legacy_thumbnail_layout(tmp_path)

    expected = tmp_path / "thumbnails" / "Antarctica" / "ANT-TEST-001.png"
    assert expected.read_bytes() == b"png"
    assert not old_directory.exists()
    result = pd.read_csv(manifest, dtype=str).fillna("")
    assert result.loc[0, "thumbnail_path"] == str(expected)
