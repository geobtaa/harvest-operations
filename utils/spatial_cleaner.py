######## SPATIAL FIELDS CLEANING ##############
# Third-party
import pandas as pd

from utils.geometry_repair import repair_geometry_fields


COORDINATE_PRECISION = 3
COORDINATE_STEP = 10**-COORDINATE_PRECISION
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
            coords = x.split(",")
            rounded = [_format_coordinate(float(c)) for c in coords]
            return ",".join(rounded)
        except Exception as e:
            print(f"[CLEAN] Skipped rounding invalid bbox: {e}")
            return x

    df["Bounding Box"] = df["Bounding Box"].apply(clean_row)
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
            west, south, east, north = map(float, x.split(","))
            corrected = False

            if east < west and west - east <= 180:
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

    df["Bounding Box"] = df["Bounding Box"].apply(clean_row)
    return df


def clean_bounding_box(df):
    """
    Adjusts extreme or degenerate 'Bounding Box' coordinates.
    """

    def clean_row(x):
        if pd.isna(x) or not isinstance(x, str):
            return x
        try:
            west, south, east, north = map(float, x.split(","))

            # Clamp extreme longitudes and latitudes
            west = _clamp(west, -MAX_LONGITUDE, MAX_LONGITUDE)
            east = _clamp(east, -MAX_LONGITUDE, MAX_LONGITUDE)
            south = _clamp(south, -MAX_LATITUDE, MAX_LATITUDE)
            north = _clamp(north, -MAX_LATITUDE, MAX_LATITUDE)

            # Expand boxes that would collapse after final 3-decimal formatting.
            if not (west > east and west - east > 180):
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

    df["Bounding Box"] = df["Bounding Box"].apply(clean_row)
    return df


def clean_geometry(df):
    """
    Normalize Geometry against the cleaned bbox using OGM-safe ring orientation.
    """
    if "Bounding Box" not in df.columns:
        return df

    if "Geometry" not in df.columns:
        df["Geometry"] = ""

    def clean_row(row):
        geometry = row.get("Geometry", "")
        bbox = row.get("Bounding Box", "")
        bbox_coords = _parse_bbox(bbox)
        if bbox_coords is not None and _is_full_world_bbox(bbox_coords):
            return WORLD_ENVELOPE
        return repair_geometry_fields(bbox, geometry).geometry

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
