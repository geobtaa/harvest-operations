######## SPATIAL FIELDS CLEANING ##############
import re

# Third-party
import pandas as pd


COORDINATE_PRECISION = 3
COORDINATE_STEP = 10 ** -COORDINATE_PRECISION
MAX_LONGITUDE = 180 - COORDINATE_STEP
MAX_LATITUDE = 90 - COORDINATE_STEP
WORLD_ENVELOPE = "ENVELOPE(-180,180,90,-90)"

def round_coordinates(df):
    """
    Rounds coordinates in the 'Bounding Box' field to 3 decimal places.
    """
    def clean_row(x):
        if pd.isna(x) or not isinstance(x, str):
            return x
        try:
            coords = x.split(',')
            rounded = [_format_coordinate(float(c)) for c in coords]
            return ','.join(rounded)
        except Exception as e:
            print(f"[CLEAN] Skipped rounding invalid bbox: {e}")
            return x
    df['Bounding Box'] = df['Bounding Box'].apply(clean_row)
    return df

def correct_bounding_box(df):
    """
    Ensures 'Bounding Box' coordinates are in proper order:
    west <= east, south <= north.
    """
    def clean_row(x):
        if pd.isna(x) or not isinstance(x, str):
            return x
        try:
            west, south, east, north = map(float, x.split(','))
            corrected = False

            if east < west:
                west, east = min(west, east), max(west, east)
                corrected = True

            if north < south:
                south, north = min(south, north), max(south, north)
                corrected = True

            corrected_bbox = f"{west:.3f},{south:.3f},{east:.3f},{north:.3f}"
            if corrected:
                print(f"[CLEAN] Corrected bbox order: {corrected_bbox}")
            return corrected_bbox
        except Exception as e:
            print(f"[CLEAN] Skipped correcting invalid bbox: {e}")
            return x
    df['Bounding Box'] = df['Bounding Box'].apply(clean_row)
    return df

def clean_bounding_box(df):
    """
    Adjusts extreme or degenerate 'Bounding Box' coordinates.
    """
    def clean_row(x):
        if pd.isna(x) or not isinstance(x, str):
            return x
        try:
            west, south, east, north = map(float, x.split(','))

            # Clamp extreme longitudes and latitudes
            west = _clamp(west, -MAX_LONGITUDE, MAX_LONGITUDE)
            east = _clamp(east, -MAX_LONGITUDE, MAX_LONGITUDE)
            south = _clamp(south, -MAX_LATITUDE, MAX_LATITUDE)
            north = _clamp(north, -MAX_LATITUDE, MAX_LATITUDE)

            # Expand boxes that would collapse after final 3-decimal formatting.
            west, east = _ensure_formatted_span(
                west,
                east,
                -MAX_LONGITUDE,
                MAX_LONGITUDE,
            )
            south, north = _ensure_formatted_span(
                south,
                north,
                -MAX_LATITUDE,
                MAX_LATITUDE,
            )

            return _format_bbox((west, south, east, north))
        except Exception as e:
            print(f"[CLEAN] Skipped cleaning invalid bbox: {e}")
            return x
    df['Bounding Box'] = df['Bounding Box'].apply(clean_row)
    return df

def clean_geometry(df):
    """
    Keep bbox-derived rectangular geometries in sync with the cleaned bbox.
    """
    if "Geometry" not in df.columns:
        return df

    def clean_row(row):
        geometry = row.get("Geometry", "")
        bbox = row.get("Bounding Box", "")
        if pd.isna(geometry) or not isinstance(geometry, str) or not geometry.strip():
            return geometry
        bbox_coords = _parse_bbox(bbox)
        if bbox_coords is None:
            return geometry
        if not _geometry_needs_bbox_sync(geometry, bbox_coords):
            return geometry
        if _is_full_world_bbox(bbox_coords):
            return WORLD_ENVELOPE
        return _bbox_to_polygon(bbox_coords)

    df["Geometry"] = df.apply(clean_row, axis=1)
    return df

def spatial_cleaning(df):
    """
    Apply all spatial cleaning steps to the DataFrame.
    """
    df = round_coordinates(df)
    df = correct_bounding_box(df)
    df = clean_bounding_box(df)
    df = clean_geometry(df)
    return df


def _parse_bbox(value):
    if pd.isna(value) or not isinstance(value, str):
        return None

    try:
        coords = tuple(float(part.strip()) for part in value.split(","))
    except ValueError:
        return None

    if len(coords) != 4:
        return None
    return coords


def _clamp(value, minimum, maximum):
    return max(minimum, min(maximum, value))


def _ensure_formatted_span(minimum_value, maximum_value, lower_bound, upper_bound):
    if _rounded(maximum_value) > _rounded(minimum_value):
        return minimum_value, maximum_value

    if maximum_value + COORDINATE_STEP <= upper_bound:
        maximum_value += COORDINATE_STEP
    elif minimum_value - COORDINATE_STEP >= lower_bound:
        minimum_value -= COORDINATE_STEP
    else:
        minimum_value = lower_bound
        maximum_value = min(upper_bound, lower_bound + COORDINATE_STEP)

    return minimum_value, maximum_value


def _rounded(value):
    return round(value, COORDINATE_PRECISION)


def _format_bbox(coords):
    return ",".join(_format_coordinate(coord) for coord in coords)


def _format_coordinate(value):
    formatted = f"{value:.{COORDINATE_PRECISION}f}"
    return "0.000" if formatted == "-0.000" else formatted


def _is_full_world_bbox(coords):
    west, south, east, north = coords
    return (
        west <= -MAX_LONGITUDE
        and south <= -MAX_LATITUDE
        and east >= MAX_LONGITUDE
        and north >= MAX_LATITUDE
    )


def _geometry_needs_bbox_sync(value, bbox_coords):
    text = value.strip()
    if _is_envelope(text):
        return True

    points = _parse_rectangular_polygon_points(text)
    if points is None:
        return False

    xs = [point[0] for point in points[:-1]]
    ys = [point[1] for point in points[:-1]]
    if len(set(xs)) < 2 or len(set(ys)) < 2:
        return True
    if any(abs(x) >= 180 for x in xs) or any(abs(y) >= 90 for y in ys):
        return True

    polygon_bbox = (min(xs), min(ys), max(xs), max(ys))
    return _format_bbox(polygon_bbox) != _format_bbox(bbox_coords)


def _is_envelope(text):
    return text.upper().startswith("ENVELOPE(") and text.endswith(")")


def _parse_rectangular_polygon_points(text):
    if not text.upper().startswith("POLYGON((") or not text.endswith("))"):
        return None

    inner = text[text.index("((") + 2 : -2]
    if "),(" in inner:
        return None

    try:
        points = [_parse_wkt_point(part) for part in inner.split(",")]
    except ValueError:
        return None

    if len(points) != 5 or points[0] != points[-1]:
        return None

    xs = {point[0] for point in points[:-1]}
    ys = {point[1] for point in points[:-1]}
    if 1 <= len(xs) <= 2 and 1 <= len(ys) <= 2:
        return points
    return None


def _parse_wkt_point(value):
    numbers = re.findall(r"[-+]?\d+(?:\.\d+)?", value)
    if len(numbers) != 2:
        raise ValueError
    return tuple(float(number) for number in numbers)


def _bbox_to_polygon(coords):
    west, south, east, north = (_format_coordinate(coord) for coord in coords)
    return (
        f"POLYGON(({west} {north}, {east} {north}, {east} {south}, "
        f"{west} {south}, {west} {north}))"
    )
