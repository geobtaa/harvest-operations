"""Run the regular ArcGIS pipeline for review without building uploads or registries.

Run from the repository root with ``python -m scripts.run_arcgis_review --input-csv ...``.
"""

import argparse
from pathlib import Path

import yaml

from harvesters.arcgis import ArcGISHarvester


def run_review(input_csv: Path) -> dict:
    if not input_csv.is_file():
        raise FileNotFoundError(f"ArcGIS input CSV not found: {input_csv}")

    with open("config/arcgis.yaml", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)

    # BaseHarvester writes primary/distribution files directly under outputs/,
    # using the configured basename. Separate basenames protect regular outputs.
    prefix = f"review_{input_csv.stem}"
    config.update(
        input_csv=str(input_csv),
        build_uploads=False,
        output_primary_csv=f"outputs/{prefix}_primary.csv",
        output_distributions_csv=f"outputs/{prefix}_distributions.csv",
        output_report_csv=f"reports/arcgis/review/{prefix}_report.csv",
    )
    print(f"[Review] Harvesting only portals in {input_csv}.")
    print("[Review] Upload generation disabled; registries will not be changed.")
    results = ArcGISHarvester(config).harvest_pipeline()
    for kind, path in results.items():
        print(f"[Review] {kind}: {path}")
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-csv", type=Path, required=True)
    args = parser.parse_args()
    run_review(args.input_csv)


if __name__ == "__main__":
    main()
