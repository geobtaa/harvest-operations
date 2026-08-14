import re


COORDINATE_PATTERN = re.compile(
    r"([NSEW])\s*([0-9]{1,3})"
    r"(?:[^0-9A-Za-z/+-]+([0-9]{1,2}))?"
    r"(?:[^0-9A-Za-z/+-]+([0-9]{1,2}))?",
    flags=re.IGNORECASE,
)
COMPACT_COORDINATE_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])([NSEW])\s*([0-9]{4,7})(?![0-9])",
    flags=re.IGNORECASE,
)
DECIMAL_NUMBER_PATTERN = re.compile(r"[-+]?\d+(?:\.\d+)?")


def is_decimal_bbox(value):
    parts = [part.strip() for part in str(value or "").split(",")]
    if len(parts) != 4:
        return False

    try:
        coordinates = [float(part) for part in parts]
    except ValueError:
        return False

    return valid_bbox_coordinates(coordinates)


def dms_to_decimal(hemisphere, degrees_text, minutes_text=None, seconds_text=None):
    degrees = int(degrees_text)
    minutes = int(minutes_text or 0)
    seconds = int(seconds_text or 0)

    if minutes >= 60 or seconds >= 60:
        raise ValueError(
            f"Invalid DMS component: {degrees_text}, {minutes_text}, {seconds_text}"
        )
    if hemisphere.upper() in {"E", "W"} and degrees > 180:
        raise ValueError(f"Invalid longitude degrees: {degrees_text}")
    if hemisphere.upper() in {"N", "S"} and degrees > 90:
        raise ValueError(f"Invalid latitude degrees: {degrees_text}")

    decimal_value = degrees + (minutes / 60) + (seconds / 3600)
    if hemisphere.upper() in {"W", "S"}:
        decimal_value *= -1
    return normalize_zero(decimal_value)


def marc_bbox_to_decimal(value):
    """
    Convert a decimal, human-readable DMS, or compact MARC coordinate bounding
    box to west,south,east,north decimal degrees.

    Returns an empty string when the source cannot be safely parsed.
    """
    raw_value = str(value or "").strip().strip("()")
    if not raw_value:
        return ""

    decimal_bbox = decimal_bbox_from_value(raw_value)
    if decimal_bbox:
        return decimal_bbox

    try:
        compact_coordinates = parse_compact_coordinates(raw_value)
        if compact_coordinates:
            return format_bbox(compact_coordinates)

        dms_coordinates = parse_dms_coordinates(raw_value)
        if dms_coordinates:
            return format_bbox(dms_coordinates)
    except ValueError:
        return ""

    return ""


def first_marc_bbox(values):
    """Return the first safely parseable MARC/decimal bounding box in values."""
    if values is None:
        return ""
    if isinstance(values, (list, tuple, set)):
        candidates = values
    else:
        candidates = [values]

    for candidate in candidates:
        converted = marc_bbox_to_decimal(candidate)
        if converted:
            return converted
    return ""


def decimal_bbox_from_value(value):
    numbers = DECIMAL_NUMBER_PATTERN.findall(value)
    if len(numbers) != 4:
        return ""
    if any(hemisphere in value.upper() for hemisphere in "NSEW"):
        return ""

    coordinates = [float(number) for number in numbers]
    if "," in value:
        return format_bbox(coordinates)

    first_longitude, second_longitude, first_latitude, second_latitude = coordinates
    marc_order_bbox = [
        min(first_longitude, second_longitude),
        min(first_latitude, second_latitude),
        max(first_longitude, second_longitude),
        max(first_latitude, second_latitude),
    ]
    return format_bbox(marc_order_bbox)


def parse_compact_coordinates(value):
    matches = list(COMPACT_COORDINATE_PATTERN.finditer(value))
    if len(matches) != 4:
        return None

    coordinates = [
        compact_coordinate_to_decimal(match.group(1), match.group(2))
        for match in matches
    ]
    return coordinates_to_bbox(coordinates, matches)


def compact_coordinate_to_decimal(hemisphere, digits):
    degree_digits = len(digits) - 4
    if degree_digits not in {1, 2, 3}:
        raise ValueError(f"Invalid compact MARC coordinate: {hemisphere}{digits}")

    degrees = digits[:degree_digits]
    minutes = digits[degree_digits : degree_digits + 2]
    seconds = digits[degree_digits + 2 :] or "0"
    if len(seconds) > 2:
        raise ValueError(f"Invalid compact MARC coordinate: {hemisphere}{digits}")
    return dms_to_decimal(hemisphere, degrees, minutes, seconds)


def parse_dms_coordinates(value):
    normalized_value = re.sub(r"deg(?:rees?)?", "°", value, flags=re.IGNORECASE)
    matches = list(COORDINATE_PATTERN.finditer(normalized_value))
    if len(matches) != 4:
        return None

    coordinates = [
        dms_to_decimal(
            match.group(1),
            match.group(2),
            match.group(3),
            match.group(4),
        )
        for match in matches
    ]
    return coordinates_to_bbox(coordinates, matches)


def coordinates_to_bbox(coordinates, matches):
    longitudes = [
        coordinate
        for coordinate, match in zip(coordinates, matches)
        if match.group(1).upper() in {"E", "W"}
    ]
    latitudes = [
        coordinate
        for coordinate, match in zip(coordinates, matches)
        if match.group(1).upper() in {"N", "S"}
    ]
    if len(longitudes) != 2 or len(latitudes) != 2:
        return None

    bbox = [min(longitudes), min(latitudes), max(longitudes), max(latitudes)]
    return bbox if valid_bbox_coordinates(bbox) else None


def valid_bbox_coordinates(coordinates):
    if len(coordinates) != 4:
        return False
    west, south, east, north = coordinates
    return (
        -180 <= west <= 180
        and -90 <= south <= 90
        and -180 <= east <= 180
        and -90 <= north <= 90
        and west <= east
        and south <= north
    )


def normalize_zero(value):
    return 0.0 if abs(value) < 1e-12 else value


def format_decimal(value):
    return f"{normalize_zero(value):.6f}".rstrip("0").rstrip(".")


def format_bbox(coordinates):
    if not coordinates or not valid_bbox_coordinates(coordinates):
        return ""
    return ",".join(format_decimal(value) for value in coordinates)
