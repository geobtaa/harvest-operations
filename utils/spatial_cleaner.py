######## SPATIAL FIELDS CLEANING ##############
# Third-party
import pandas as pd
import numpy as np

def round_coordinates(df):
    """
    Rounds coordinates in the 'Bounding Box' field to 3 decimal places.
    """
    def clean_row(x):
        if pd.isna(x) or not isinstance(x, str):
            return x
        try:
            coords = x.split(',')
            rounded = [f"{float(c):.3f}" for c in coords]
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
            if abs(west) >= 180: west = np.sign(west) * 179.999
            if abs(east) >= 180: east = np.sign(east) * 179.999
            if north >= 90: north = 89.999
            if south <= -90: south = -89.999

            # Expand zero-width boxes slightly
            if abs(east - west) < 0.0001:
                east += 0.0001
            if abs(north - south) < 0.0001:
                north += 0.0001

            cleaned_bbox = f"{west:.3f},{south:.3f},{east:.3f},{north:.3f}"
            return cleaned_bbox
        except Exception as e:
            print(f"[CLEAN] Skipped cleaning invalid bbox: {e}")
            return x
    df['Bounding Box'] = df['Bounding Box'].apply(clean_row)
    return df

def spatial_cleaning(df):
    """
    Apply all spatial cleaning steps to the DataFrame.
    """
    df = round_coordinates(df)
    df = correct_bounding_box(df)
    df = clean_bounding_box(df)
    return df

