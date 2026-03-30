import yaml
import pandas as pd

with open("schemas/geobtaa_schema.yaml", "r", encoding="utf-8") as f:
    schema = yaml.safe_load(f)

fields = schema.get("fields", [])
df = pd.DataFrame(fields)
df.to_csv("schemas/schema_fields.csv", index=False)