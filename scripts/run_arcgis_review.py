"""Run the regular ArcGIS pipeline for review without building uploads or registries.

Run from the repository root with ``python -m scripts.run_arcgis_review --input-csv ...``.
"""

import argparse
import csv
from pathlib import Path
import re
import tempfile

import yaml

from harvesters.arcgis import ArcGISHarvester


def run_review(input_csv: Path, harvest_id: str | None = None) -> dict:
    if not input_csv.is_file():
        raise FileNotFoundError(f"ArcGIS input CSV not found: {input_csv}")

    with open("config/arcgis.yaml", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)

    # BaseHarvester writes primary/distribution files directly under outputs/,
    # using the configured basename. Separate basenames protect regular outputs.
    prefix = f"review_{input_csv.stem}"
    selected_rows = None
    if harvest_id is not None:
        with input_csv.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            fieldnames = reader.fieldnames
            if not fieldnames or "ID" not in fieldnames:
                raise ValueError(f"ArcGIS input CSV is missing the ID column: {input_csv}")
            selected_rows = [row for row in reader if row["ID"].strip() == harvest_id]
        if len(selected_rows) != 1:
            raise ValueError(
                f"Expected exactly one row for ID {harvest_id!r}; found {len(selected_rows)}."
            )
        prefix += "_" + re.sub(r"[^A-Za-z0-9_-]", "_", harvest_id)
    config.update(
        input_csv=str(input_csv),
        build_uploads=False,
        output_primary_csv=f"outputs/{prefix}_primary.csv",
        output_distributions_csv=f"outputs/{prefix}_distributions.csv",
        output_report_csv=f"reports/arcgis/review/{prefix}_report.csv",
    )
    scope = f"ID {harvest_id} from" if harvest_id is not None else "portals in"
    print(f"[Review] Harvesting only {scope} {input_csv}.")
    print("[Review] Upload generation disabled; registries will not be changed.")
    # All pipeline stages, including appended harvest records, see only the
    # selected input. Keep the source CSV intact and remove the temporary copy.
    with tempfile.TemporaryDirectory(prefix="arcgis-review-") as temp_dir:
        if selected_rows is not None:
            selected_csv = Path(temp_dir) / "selected.csv"
            with selected_csv.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(selected_rows)
            config["input_csv"] = str(selected_csv)
        results = ArcGISHarvester(config).harvest_pipeline()
    for kind, path in results.items():
        print(f"[Review] {kind}: {path}")
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-csv", type=Path, required=True)
    parser.add_argument("--harvest-id", help="Harvest only the row with this exact ID.")
    args = parser.parse_args()
    run_review(args.input_csv, harvest_id=args.harvest_id)


if __name__ == "__main__":
    main()
