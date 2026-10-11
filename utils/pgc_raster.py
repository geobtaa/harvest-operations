"""Spatial helpers shared by the PGC raster derivative workflows."""

from __future__ import annotations

from pathlib import Path

import rasterio
from rasterio.warp import transform as transform_coordinates
from rasterio.warp import transform_bounds
from shapely.geometry import MultiPoint, Point


TARGET_CRS = "EPSG:3857"
WEB_MERCATOR_MAX_LATITUDE = 85.0511287798066
WEB_MERCATOR_HALF_WORLD = 20037508.342789244


def derive_wgs84_bounds(source: Path) -> tuple[float, float, float, float]:
    """Derive geographic bounds from a raster CRS or its ground-control points."""
    with rasterio.open(source) as dataset:
        if dataset.crs:
            return transform_bounds(
                dataset.crs, "EPSG:4326", *dataset.bounds, densify_pts=41
            )
        gcps, gcp_crs = dataset.gcps
    if not gcps or not gcp_crs:
        raise RuntimeError(f"raster has no usable georeferencing: {source.name}")
    target_points = [(gcp.x, gcp.y) for gcp in gcps]
    longitude, latitude = transform_coordinates(
        gcp_crs,
        "EPSG:4326",
        [point[0] for point in target_points],
        [point[1] for point in target_points],
    )
    if MultiPoint(target_points).convex_hull.contains(Point(0, 0)):
        _, pole_latitude = transform_coordinates(gcp_crs, "EPSG:4326", [0], [0])
        latitude.append(pole_latitude[0])
        return -180.0, min(latitude), 180.0, max(latitude)
    return min(longitude), min(latitude), max(longitude), max(latitude)


def web_mercator_bounds(
    bounds: tuple[float, float, float, float],
) -> tuple[float, float, float, float]:
    """Project geographic bounds to the finite EPSG:3857 display domain."""
    west, south, east, north = bounds
    south = max(south, -WEB_MERCATOR_MAX_LATITUDE)
    north = min(north, WEB_MERCATOR_MAX_LATITUDE)
    if south >= north:
        raise RuntimeError("source extent falls outside the Web Mercator limits")
    if east < west or (west <= -180.0 and east >= 180.0):
        projected_west = -WEB_MERCATOR_HALF_WORLD
        projected_east = WEB_MERCATOR_HALF_WORLD
    else:
        projected_west, _, projected_east, _ = transform_bounds(
            "EPSG:4326", TARGET_CRS, west, south, east, north, densify_pts=41
        )
    _, projected_south, _, projected_north = transform_bounds(
        "EPSG:4326", TARGET_CRS, -180.0, south, 180.0, north, densify_pts=41
    )
    return (
        max(-WEB_MERCATOR_HALF_WORLD, projected_west),
        max(-WEB_MERCATOR_HALF_WORLD, projected_south),
        min(WEB_MERCATOR_HALF_WORLD, projected_east),
        min(WEB_MERCATOR_HALF_WORLD, projected_north),
    )


def format_union_bounds(bounds: list[tuple[float, float, float, float]]) -> str:
    """Format the union of one or more bounds as west,south,east,north."""
    normalized = [
        (-180.0, item[1], 180.0, item[3]) if item[2] < item[0] else item
        for item in bounds
    ]
    return (
        f"{min(item[0] for item in normalized):.6f},"
        f"{min(item[1] for item in normalized):.6f},"
        f"{max(item[2] for item in normalized):.6f},"
        f"{max(item[3] for item in normalized):.6f}"
    )
