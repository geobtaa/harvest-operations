#!/usr/bin/env python3
import csv
import requests
import sys
import os
import json
from datetime import date

def check_geojson(url, max_bytes=10_000_000, timeout=15, retries=2):
    """
    Returns True if the URL points to a valid GeoJSON with at least one
    non-null geometry. Otherwise returns False.
    """
    try:
        resp = requests.get(url, stream=True, timeout=timeout)
        resp.raise_for_status()

        # Bail early if response is too large
        size = int(resp.headers.get("Content-Length", 0))
        if size and size > max_bytes:
            print(f"Skipping (too large): {url}")
            return False

        # Download content in chunks to avoid memory bloat
        content = b""
        for chunk in resp.iter_content(1024 * 1024):  # 1MB chunks
            content += chunk
            if len(content) > max_bytes:
                print(f"Skipping (too large while downloading): {url}")
                return False

        # Parse as JSON
        data = json.loads(content.decode("utf-8"))

        # Must be a FeatureCollection
        if data.get("type") != "FeatureCollection":
            print(f"Not a FeatureCollection: {url}")
            return False

        features = data.get("features", [])
        if not features:
            print(f"No features: {url}")
            return False

        # Ensure at least one feature has a valid geometry
        if not any(f.get("geometry") for f in features if isinstance(f, dict)):
            print(f"All geometries are null: {url}")
            return False

        return True

    except Exception as e:
        print(f"Error for {url}: {e}")
        return False

def main():
    today = date.today().strftime("%Y-%m-%d")
    input_csv = f"outputs/{today}_socrata_distributions.csv"
    output_csv = f"outputs/{today}_socrata_distributions_cleaned.csv"

    if not os.path.exists(input_csv):
        print(f"Input file not found: {input_csv}")
        sys.exit(1)

    with open(input_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    cleaned_rows = []
    for row in rows:
        if row.get("reference_type") == "geo_json":
            url = row.get("distribution_url")
            if not url or not check_geojson(url):
                continue  # Drop bad or missing GeoJSONs
        cleaned_rows.append(row)

    # Write cleaned file
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(cleaned_rows)

    print(f"Cleaned CSV written to {output_csv}")
    print(f"   {len(rows)} → {len(cleaned_rows)} rows kept")

if __name__ == "__main__":
    main()
