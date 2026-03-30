import json
import yaml
import os

# Set your input/output paths
input_json_path = "geobtaa_schema.json"
output_yaml_path = "geobtaa_schema.yaml"

# Read the JSON schema
with open(input_json_path, "r", encoding="utf-8") as f:
    schema_json = json.load(f)

# Write the YAML schema
with open(output_yaml_path, "w", encoding="utf-8") as f:
    yaml.dump(
        schema_json,
        f,
        default_flow_style=False,  # Use block style, not inline lists/dicts
        sort_keys=False,           # Keep your original key order
        allow_unicode=True         # Allow non-ASCII characters if you have them
    )

print(f"✅ Converted {input_json_path} → {output_yaml_path}")
