#!/usr/bin/env python3
"""
Download HDX datasets with has_geodata:true and save to inputs/hdx_has_geodata.json
This script requires hdx-python-api. Use `pip install hdx-python-api` if needed
"""

import os
import json
import time
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset

# -------------------------------------------------------------------
# Settings
# -------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "inputs")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "hdx_geodata.json")\

# Optional knobs
ROWS = 200          # page size
Q = ""              # optional free-text search, e.g. "roads"
FQ_EXTRA = ""       # optional CKAN filter, e.g. "organization:world-bank"
THROTTLE = 0.1      # seconds to sleep between pages

# -------------------------------------------------------------------
# Init HDX API (read-only)
# -------------------------------------------------------------------
Configuration.create(hdx_site="prod", user_agent="BTAA_Geoportal", hdx_read_only=True)

def main():
    # Ensure output dir exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    fq_parts = ["has_geodata:true"]
    if FQ_EXTRA:
        fq_parts.append(f"({FQ_EXTRA})")
    fq = " AND ".join(fq_parts)

    all_records = []
    start = 0

    while True:
        batch = Dataset.search_in_hdx(q=Q or None, fq=fq, start=start, rows=ROWS)
        if not batch:
            break

        # Normalize to dicts
        for item in batch:
            ds = item.data if hasattr(item, "data") else item
            all_records.append(ds)

        start += len(batch)
        print(f"Retrieved {len(batch)} records (total so far: {len(all_records)})")

        if len(batch) < ROWS:
            break
        if THROTTLE > 0:
            time.sleep(THROTTLE)

    # Write JSON
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_records, f, indent=2, ensure_ascii=False)

    print(f"\nSaved {len(all_records)} datasets with has_geodata:true to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
