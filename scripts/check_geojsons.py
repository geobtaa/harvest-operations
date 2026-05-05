#!/usr/bin/env python3
import sys
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from harvesters.socrata import filter_valid_socrata_geojson_distributions

def main():
    today = date.today().strftime("%Y-%m-%d")
    input_csv = Path(f"outputs/{today}_socrata_distributions.csv")
    output_csv = Path(f"outputs/{today}_socrata_distributions_cleaned.csv")

    if not input_csv.exists():
        print(f"Input file not found: {input_csv}")
        sys.exit(1)

    rows = pd.read_csv(input_csv, dtype=str).fillna("")
    cleaned_rows = filter_valid_socrata_geojson_distributions(rows)
    cleaned_rows.to_csv(output_csv, index=False, encoding="utf-8")

    print(f"Cleaned CSV written to {output_csv}")
    print(f"   {len(rows)} -> {len(cleaned_rows)} rows kept")

if __name__ == "__main__":
    main()
