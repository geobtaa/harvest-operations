import pandas as pd
import time

today = time.strftime('%Y-%m-%d')

def apply_derived_values(df: pd.DataFrame, derived: dict) -> pd.DataFrame:
    """
    Apply harvested or derived column mappings.
    
    Args:
        df (pd.DataFrame): The DataFrame to modify.
        derived (dict): A dictionary mapping new column names to existing df columns.
                        Example: {'Identifier': 'information'}
    
    Returns:
        pd.DataFrame: The modified DataFrame with derived columns set.
    """
    for new_col, source_col in derived.items():
        df[new_col] = df[source_col]
    return df


def apply_default_values(df: pd.DataFrame, defaults: dict) -> pd.DataFrame:
    """
    Apply a dictionary of hardcoded default values to every row of a DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame to modify.
        defaults (dict): A dictionary of column: value pairs to set.

    Returns:
        pd.DataFrame: The modified DataFrame with default values set.
    """
    for col, val in defaults.items():
        df[col] = val
    return df
