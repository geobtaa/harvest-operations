from pathlib import Path

import pandas as pd
from shapely import wkt
from shapely.geometry import MultiPolygon, Polygon, box

from utils.geometry_repair import (
    bbox_crosses_antimeridian,
    normalize_bbox,
    parse_envelope,
)


REQUIRED_COLUMNS = ["ID", "Title", "Access Rights", "Resource Class"]
VALID_ACCESS_RIGHTS = {"Public", "Restricted"}
VALID_RESOURCE_CLASSES = {
    "Collections",
    "Datasets",
    "Imagery",
    "Maps",
    "Web services",
    "Websites",
    "Series",
    "Other",
}


def validate_required_columns(df, required_columns=None):
    """
    Ensure required columns are present in the DataFrame.
    Raises ValueError if any required columns are missing.
    """
    if required_columns is None:
        required_columns = REQUIRED_COLUMNS

    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"[VALIDATION] Missing required columns: {', '.join(missing)}")
    print(f"[VALIDATION] All required columns present: {', '.join(required_columns)}")
    return df


def validate_access_rights(df):
    """
    Validates that 'Access Rights' values are within allowed list.
    Raises ValueError if any invalid values are found.
    """
    invalid = df.loc[
        ~df["Access Rights"].isin(VALID_ACCESS_RIGHTS), "Access Rights"
    ].unique()
    if len(invalid) > 0:
        raise ValueError(f"[VALIDATION] Invalid Access Rights values: {invalid}")
    print("[VALIDATION] All Access Rights values are valid.")
    return df


def validate_resource_class(df):
    def is_valid(cell):
        if pd.isnull(cell):
            return False
        classes = [c.strip() for c in str(cell).split("|")]
        return any(c in VALID_RESOURCE_CLASSES for c in classes)

    invalid_rows = df[~df["Resource Class"].apply(is_valid)]
    if not invalid_rows.empty:
        raise ValueError("[VALIDATION] Found rows with invalid Resource Class values.")
    print("[VALIDATION] All Resource Class values are valid.")
    return df


def validate_bounding_box(df):
    """
    Checks Bounding Box column for numeric coordinate validity.
    Warns if coordinates fall outside plausible ranges.
    """

    def check_bbox(x):
        if _is_blank(x):
            return True
        try:
            coords = list(map(float, x.split(",")))
            return (
                len(coords) == 4
                and -180 <= coords[0] <= 180
                and -90 <= coords[1] <= 90
                and -180 <= coords[2] <= 180
                and -90 <= coords[3] <= 90
                and coords[1] <= coords[3]
            )
        except Exception:
            return False

    invalid_bboxes = df.loc[~df["Bounding Box"].apply(check_bbox)]

    if not invalid_bboxes.empty:
        print("[VALIDATION] Found rows with invalid Bounding Boxes:")
        print(invalid_bboxes[["ID", "Bounding Box"]])
        output_path = Path("outputs/invalid_bboxes.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        invalid_bboxes.to_csv(output_path, index=False)
        print(
            "[VALIDATION] Invalid Bounding Boxes written to outputs/invalid_bboxes.csv"
        )
    else:
        print("[VALIDATION] All Bounding Box coordinates are within valid ranges.")

    return df


def geometry_validation_error(bounding_box, geometry) -> str:
    """Return why an Aardvark Geometry value is invalid, or an empty string."""
    bbox_text = _clean_value(bounding_box)
    geometry_text = _clean_value(geometry)

    if not bbox_text:
        if geometry_text:
            return "Geometry must be blank when Bounding Box is blank."
        return ""

    bbox_parts, bbox_note = normalize_bbox(bbox_text)
    if bbox_parts is None:
        return (
            f"Geometry cannot be validated because Bounding Box is invalid: {bbox_note}"
        )
    if not geometry_text:
        return "Geometry is required when Bounding Box is populated."

    envelope_parts = parse_envelope(geometry_text)
    if envelope_parts is not None:
        expected = (
            float(bbox_parts[0]),
            float(bbox_parts[2]),
            float(bbox_parts[3]),
            float(bbox_parts[1]),
        )
        actual = tuple(float(part) for part in envelope_parts)
        if not _coordinates_match(actual, expected):
            return "ENVELOPE coordinates do not match Bounding Box."
        return ""

    try:
        parsed = wkt.loads(geometry_text)
    except Exception:
        return "Geometry must be valid ENVELOPE, POLYGON, or MULTIPOLYGON syntax."

    if not isinstance(parsed, (Polygon, MultiPolygon)):
        return "Geometry WKT must be a POLYGON or MULTIPOLYGON."
    if parsed.is_empty:
        return "Geometry must not be empty."
    if not parsed.is_valid:
        return "Geometry has invalid topology."
    if not _coordinates_in_range(parsed.bounds):
        return "Geometry coordinates fall outside longitude/latitude ranges."
    if isinstance(parsed, Polygon) and parsed.equals(box(*parsed.bounds)):
        return "Rectangular Geometry must use ENVELOPE syntax."

    polygons = [parsed] if isinstance(parsed, Polygon) else list(parsed.geoms)
    if any(not polygon.exterior.is_ccw for polygon in polygons):
        return "Polygon exterior rings must be counter-clockwise."
    if any(ring.is_ccw for polygon in polygons for ring in polygon.interiors):
        return "Polygon interior rings must be clockwise."

    if not bbox_crosses_antimeridian(bbox_parts):
        bbox_bounds = tuple(float(part) for part in bbox_parts)
        if not _coordinates_match(parsed.bounds, bbox_bounds):
            return "Geometry bounds do not match Bounding Box."
    return ""


def validate_geometry(df):
    """Report invalid Geometry/Bounding Box combinations without rejecting blank pairs."""
    if "Bounding Box" not in df.columns and "Geometry" not in df.columns:
        return df

    bounding_boxes = df.get("Bounding Box", pd.Series("", index=df.index))
    geometries = df.get("Geometry", pd.Series("", index=df.index))
    errors = pd.Series(
        (
            geometry_validation_error(bounding_box, geometry)
            for bounding_box, geometry in zip(bounding_boxes, geometries)
        ),
        index=df.index,
    )
    invalid_geometry = df.loc[errors.ne("")].copy()
    if not invalid_geometry.empty:
        if "Bounding Box" not in invalid_geometry:
            invalid_geometry["Bounding Box"] = ""
        if "Geometry" not in invalid_geometry:
            invalid_geometry["Geometry"] = ""
        invalid_geometry["Geometry Validation Error"] = errors[errors.ne("")]
        print("[VALIDATION] Found rows with invalid Geometry values:")
        print(
            invalid_geometry[
                ["ID", "Bounding Box", "Geometry", "Geometry Validation Error"]
            ]
        )
        output_path = Path("outputs/invalid_geometry.csv")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        invalid_geometry.to_csv(output_path, index=False)
        print(f"[VALIDATION] Invalid Geometry rows written to {output_path}")
    else:
        print("[VALIDATION] All Geometry values are valid.")
    return df


def _clean_value(value) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _is_blank(value) -> bool:
    return not _clean_value(value)


def _coordinates_match(
    actual: tuple[float, ...], expected: tuple[float, ...], tolerance: float = 1e-9
) -> bool:
    return len(actual) == len(expected) and all(
        abs(left - right) <= tolerance for left, right in zip(actual, expected)
    )


def _coordinates_in_range(bounds: tuple[float, float, float, float]) -> bool:
    west, south, east, north = bounds
    return -180 <= west <= east <= 180 and -90 <= south <= north <= 90


def validation_pipeline(df):
    """
    Run all validations on the DataFrame using method chaining.
    """
    return (
        df.pipe(validate_required_columns)
        .pipe(validate_access_rights)
        .pipe(validate_resource_class)
        .pipe(validate_bounding_box)
        .pipe(validate_geometry)
    )
