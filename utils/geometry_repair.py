"""Validate and repair OGM Aardvark Geometry and Bounding Box values."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import math
import re

from shapely import to_wkt, wkt
from shapely.geometry import MultiPolygon, Polygon
from shapely.geometry.polygon import orient


ENVELOPE_PATTERN = re.compile(r"^ENVELOPE\s*\(([^)]+)\)$", re.IGNORECASE)


@dataclass(frozen=True)
class GeometryRepair:
    bounding_box: str
    geometry: str
    action: str
    requires_review: bool = False
    note: str = ""

    def as_dict(self) -> dict[str, str | bool]:
        return asdict(self)


def repair_geometry_fields(bounding_box: object, geometry: object) -> GeometryRepair:
    """Return normalized bbox and Geometry values without changing spatial meaning."""
    original_bbox = _clean_text(bounding_box)
    original_geometry = _clean_text(geometry)
    bbox_parts, bbox_note = normalize_bbox(original_bbox)
    repaired_bbox = ",".join(bbox_parts) if bbox_parts else original_bbox

    envelope_parts = parse_envelope(original_geometry)
    if envelope_parts is not None:
        notes = _join_notes(bbox_note, "Existing ENVELOPE retained.")
        return GeometryRepair(
            bounding_box=repaired_bbox,
            geometry=envelope_from_parts(envelope_parts),
            action="retain_envelope",
            note=notes,
        )

    if not original_geometry:
        if bbox_parts:
            if bbox_crosses_antimeridian(bbox_parts):
                return GeometryRepair(
                    bounding_box=repaired_bbox,
                    geometry=antimeridian_multipolygon_from_bbox_parts(bbox_parts),
                    action="create_antimeridian_multipolygon_from_bbox",
                    note=bbox_note,
                )
            return GeometryRepair(
                bounding_box=repaired_bbox,
                geometry=envelope_from_bbox_parts(bbox_parts),
                action="create_envelope_from_bbox",
                note=bbox_note,
            )
        return GeometryRepair(
            bounding_box=repaired_bbox,
            geometry="",
            action="unrepairable_missing_coordinates",
            requires_review=True,
            note=_join_notes(
                bbox_note, "Geometry and usable Bounding Box are both missing."
            ),
        )

    try:
        parsed = wkt.loads(original_geometry)
    except Exception as exc:
        return _fallback_to_bbox(
            repaired_bbox,
            bbox_parts,
            "replace_invalid_wkt_with_envelope",
            _join_notes(bbox_note, f"Invalid WKT: {exc}"),
            original_geometry,
        )

    if not isinstance(parsed, (Polygon, MultiPolygon)):
        return _fallback_to_bbox(
            repaired_bbox,
            bbox_parts,
            "replace_unsupported_geometry_with_envelope",
            _join_notes(bbox_note, f"Unsupported geometry type: {parsed.geom_type}."),
            original_geometry,
        )

    if not parsed.is_valid:
        return _fallback_to_bbox(
            repaired_bbox,
            bbox_parts,
            "replace_invalid_topology_with_envelope",
            _join_notes(bbox_note, "WKT parses but has invalid topology."),
            original_geometry,
            requires_review=True,
        )

    if _is_rectangle(parsed):
        if bbox_parts is None:
            bbox_parts = bbox_parts_from_bounds(parsed.bounds)
            repaired_bbox = ",".join(bbox_parts)
            bbox_note = _join_notes(
                bbox_note, "Bounding Box derived from rectangular Geometry."
            )
        if bbox_crosses_antimeridian(bbox_parts):
            return GeometryRepair(
                bounding_box=repaired_bbox,
                geometry=antimeridian_multipolygon_from_bbox_parts(bbox_parts),
                action="replace_rectangle_with_antimeridian_multipolygon",
                note=bbox_note,
            )
        return GeometryRepair(
            bounding_box=repaired_bbox,
            geometry=envelope_from_bbox_parts(bbox_parts),
            action="replace_rectangle_with_envelope",
            note=bbox_note,
        )

    if _crosses_antimeridian(parsed):
        return GeometryRepair(
            bounding_box=repaired_bbox,
            geometry=original_geometry,
            action="review_antimeridian_geometry",
            requires_review=True,
            note=_join_notes(
                bbox_note,
                "A ring segment crosses the antimeridian; the geometry was not changed.",
            ),
        )

    if _has_correct_orientation(parsed):
        return GeometryRepair(
            bounding_box=repaired_bbox,
            geometry=original_geometry,
            action="retain_oriented_complex_geometry",
            note=bbox_note,
        )

    oriented = _orient_geometry(parsed)
    return GeometryRepair(
        bounding_box=repaired_bbox,
        geometry=to_wkt(oriented, rounding_precision=-1, trim=True),
        action="rewind_complex_geometry",
        note=bbox_note,
    )


def normalize_bbox(value: object) -> tuple[tuple[str, str, str, str] | None, str]:
    """Normalize a decimal bbox to west,south,east,north, preserving input precision."""
    text = _clean_text(value)
    if not text:
        return None, "Bounding Box is blank."

    parts = tuple(part.strip() for part in text.split(","))
    if len(parts) != 4:
        return None, "Bounding Box does not contain four coordinates."

    try:
        numbers = tuple(float(part) for part in parts)
    except ValueError:
        return None, "Bounding Box contains a non-numeric coordinate."

    if not all(math.isfinite(number) for number in numbers):
        return None, "Bounding Box contains a non-finite coordinate."

    west, south, east, north = numbers
    if not all(
        (
            -180 <= west <= 180,
            -90 <= south <= 90,
            -180 <= east <= 180,
            -90 <= north <= 90,
        )
    ):
        return None, "Bounding Box contains an out-of-range coordinate."

    normalized = list(parts)
    notes = []
    if east < west:
        if west - east > 180:
            notes.append("Antimeridian-crossing Bounding Box retained.")
        else:
            normalized[0], normalized[2] = normalized[2], normalized[0]
            notes.append("Reordered west/east Bounding Box coordinates.")
    if north < south:
        normalized[1], normalized[3] = normalized[3], normalized[1]
        notes.append("Reordered south/north Bounding Box coordinates.")

    return tuple(normalized), " ".join(notes)


def parse_envelope(value: object) -> tuple[str, str, str, str] | None:
    """Parse and validate ENVELOPE(W,E,N,S), returning coordinate strings."""
    text = _clean_text(value)
    match = ENVELOPE_PATTERN.fullmatch(text)
    if not match:
        return None

    parts = tuple(part.strip() for part in match.group(1).split(","))
    if len(parts) != 4:
        return None
    try:
        west, east, north, south = (float(part) for part in parts)
    except ValueError:
        return None
    if not all(
        (
            math.isfinite(west),
            math.isfinite(east),
            math.isfinite(north),
            math.isfinite(south),
            -180 <= west <= east <= 180,
            -90 <= south <= north <= 90,
        )
    ):
        return None
    return parts


def envelope_from_bbox_parts(parts: tuple[str, str, str, str]) -> str:
    west, south, east, north = parts
    return envelope_from_parts((west, east, north, south))


def bbox_crosses_antimeridian(parts: tuple[str, str, str, str]) -> bool:
    west, _, east, _ = (float(part) for part in parts)
    return west > east and west - east > 180


def antimeridian_multipolygon_from_bbox_parts(
    parts: tuple[str, str, str, str],
) -> str:
    west, south, east, north = parts
    return (
        "MULTIPOLYGON (("
        f"({west} {south},180 {south},180 {north},{west} {north},{west} {south})"
        "),("
        f"(-180 {south},{east} {south},{east} {north},-180 {north},-180 {south})"
        "))"
    )


def envelope_from_parts(parts: tuple[str, str, str, str]) -> str:
    return f"ENVELOPE({','.join(parts)})"


def bbox_parts_from_bounds(
    bounds: tuple[float, float, float, float],
) -> tuple[str, str, str, str]:
    return tuple(format(number, ".15g") for number in bounds)  # type: ignore[return-value]


def _fallback_to_bbox(
    repaired_bbox: str,
    bbox_parts: tuple[str, str, str, str] | None,
    action: str,
    note: str,
    original_geometry: str,
    *,
    requires_review: bool = True,
) -> GeometryRepair:
    if bbox_parts:
        if bbox_crosses_antimeridian(bbox_parts):
            return GeometryRepair(
                bounding_box=repaired_bbox,
                geometry=antimeridian_multipolygon_from_bbox_parts(bbox_parts),
                action=action.replace(
                    "with_envelope", "with_antimeridian_multipolygon"
                ),
                requires_review=requires_review,
                note=note,
            )
        return GeometryRepair(
            bounding_box=repaired_bbox,
            geometry=envelope_from_bbox_parts(bbox_parts),
            action=action,
            requires_review=requires_review,
            note=note,
        )
    return GeometryRepair(
        bounding_box=repaired_bbox,
        geometry=original_geometry,
        action="unrepairable_geometry",
        requires_review=True,
        note=_join_notes(note, "No usable Bounding Box is available for fallback."),
    )


def _is_rectangle(geometry: Polygon | MultiPolygon) -> bool:
    if not isinstance(geometry, Polygon) or geometry.interiors:
        return False
    points = list(geometry.exterior.coords)
    if len(points) != 5 or points[0] != points[-1]:
        return False
    corners = points[:-1]
    return (
        len({point[0] for point in corners}) == 2
        and len({point[1] for point in corners}) == 2
    )


def _has_correct_orientation(geometry: Polygon | MultiPolygon) -> bool:
    polygons = [geometry] if isinstance(geometry, Polygon) else list(geometry.geoms)
    return all(
        polygon.exterior.is_ccw
        and all(not interior.is_ccw for interior in polygon.interiors)
        for polygon in polygons
    )


def _orient_geometry(geometry: Polygon | MultiPolygon) -> Polygon | MultiPolygon:
    if isinstance(geometry, Polygon):
        return orient(geometry, sign=1.0)
    return MultiPolygon([orient(polygon, sign=1.0) for polygon in geometry.geoms])


def _crosses_antimeridian(geometry: Polygon | MultiPolygon) -> bool:
    polygons = [geometry] if isinstance(geometry, Polygon) else list(geometry.geoms)
    for polygon in polygons:
        for ring in [polygon.exterior, *polygon.interiors]:
            points = list(ring.coords)
            if any(
                abs(start[0] - end[0]) > 180 for start, end in zip(points, points[1:])
            ):
                return True
    return False


def _clean_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _join_notes(*notes: str) -> str:
    return " ".join(note.strip() for note in notes if note and note.strip())
