import pandas as pd


REQUIRED_COLUMNS = ['ID', 'Title', 'Access Rights', 'Resource Class']
VALID_ACCESS_RIGHTS = {'Public', 'Restricted'}
VALID_RESOURCE_CLASSES = {
    'Collections', 'Datasets', 'Imagery', 'Maps',
    'Web services', 'Websites', 'Series', 'Other'
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
    invalid = df.loc[~df['Access Rights'].isin(VALID_ACCESS_RIGHTS), 'Access Rights'].unique()
    if len(invalid) > 0:
        raise ValueError(f"[VALIDATION] Invalid Access Rights values: {invalid}")
    print("[VALIDATION] All Access Rights values are valid.")
    return df

def validate_resource_class(df):
    def is_valid(cell):
        if pd.isnull(cell): return False
        classes = [c.strip() for c in str(cell).split('|')]
        return any(c in VALID_RESOURCE_CLASSES for c in classes)

    invalid_rows = df[~df['Resource Class'].apply(is_valid)]
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
        try:
            coords = list(map(float, x.split(',')))
            return (
                -180 <= coords[0] <= 180 and
                -90 <= coords[1] <= 90 and
                -180 <= coords[2] <= 180 and
                -90 <= coords[3] <= 90
            )
        except Exception:
            return False

    invalid_bboxes = df.loc[~df['Bounding Box'].apply(lambda x: check_bbox(x) if isinstance(x, str) else False)]
    
    if not invalid_bboxes.empty:
        print("[VALIDATION] Found rows with invalid Bounding Boxes:")
        print(invalid_bboxes[["ID", "Bounding Box"]])
        invalid_bboxes.to_csv("outputs/invalid_bboxes.csv", index=False)
        print("[VALIDATION] Invalid Bounding Boxes written to outputs/invalid_bboxes.csv")
    else:
        print("[VALIDATION] All Bounding Box coordinates are within valid ranges.")
    
    return df


def validation_pipeline(df):
    """
    Run all validations on the DataFrame using method chaining.
    """
    return (
        df.pipe(validate_required_columns)
          .pipe(validate_access_rights)
          .pipe(validate_resource_class)
          .pipe(validate_bounding_box)
    )
