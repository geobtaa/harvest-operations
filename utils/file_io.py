import yaml
import pandas as pd
import os
    
def load_yaml_file(path):
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)
    
def load_local_schema(schema_path="schemas/geobtaa_schema.csv") -> dict:
    """
    Load the local metadata schema from CSV.
    Returns a dictionary with 'primaryKey' and 'fields' for compatibility.
    """
    df = pd.read_csv(schema_path, dtype=str).fillna("")
    df["order"] = df["order"].astype(int)

    fields = df.sort_values("order").to_dict(orient="records")
    primary_keys = ["ID"]  # Customize if needed

    return {
        "primaryKey": primary_keys,
        "fields": fields
    }

def write_csv(records: list[dict], output_path: str):
    """
    Write normalized records to CSV using Pandas.
    """
    df = pd.DataFrame(records)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8")
