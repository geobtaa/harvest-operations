import pandas as pd
import yaml

def load_field_order_from_schemas(
    primary_schema_path="schemas/geobtaa_schema.csv",
    dist_schema_path="schemas/distribution_types.yaml",
) -> list:
    """
    Loads canonical field order from geobtaa_schema.csv and distribution_types.yaml.
    """
    df = pd.read_csv(primary_schema_path, dtype=str).fillna("")
    df["order"] = df["order"].astype(int)
    field_names = df.sort_values("order")["name"].tolist()

    # Avoid duplicates and preserve order
    seen = set(field_names)
    combined_order = field_names.copy()

    # Add distribution variables
    with open(dist_schema_path, "r", encoding="utf-8") as f:
        dist_schema = yaml.safe_load(f)

    for dist in dist_schema.get("distribution_types", []):
        for var in dist.get("variables", []):
            if var not in seen:
                combined_order.append(var)
                seen.add(var)

    return combined_order

FIELD_ORDER = load_field_order_from_schemas()


def load_primary_field_order(schema_path="schemas/geobtaa_schema.csv") -> list:
    """
    Load only the ordered primary metadata field names (excludes distribution fields).
    """
    df = pd.read_csv(schema_path, dtype=str).fillna("")
    df["order"] = df["order"].astype(int)
    return df.sort_values("order")["name"].tolist()

PRIMARY_FIELD_ORDER = load_primary_field_order()
