#!/usr/bin/env python3
"""Reconcile U. Michigan resource identities and retain sparse manual enrichments."""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.metadata_reconciliation import deleted_keys, read_csv, reconcile  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--harvested-primary", type=Path, required=True)
    parser.add_argument("--enrichments", type=Path, required=True)
    parser.add_argument(
        "--exclude-enrichment-field",
        action="append",
        default=["Subject"],
        help="Keep the harvested value for this field; Subject is always excluded by default.",
    )
    parser.add_argument("--existing-distributions", type=Path, required=True)
    parser.add_argument("--harvested-distributions", type=Path)
    parser.add_argument("--xml-dir", type=Path)
    parser.add_argument(
        "--aliases",
        type=Path,
        help="identity_aliases.csv retained from an earlier accepted run",
    )
    parser.add_argument(
        "--decisions",
        type=Path,
        help="Reviewed CSV with harvested_id, action, canonical_id",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    outputs = reconcile(
        read_csv(args.harvested_primary, ("ID", "Identifier")),
        read_csv(args.enrichments, ("ID",)),
        read_csv(args.existing_distributions, ("friendlier_id", "distribution_url")),
        aliases=read_csv(args.aliases, ("canonical_id", "match_key"))
        if args.aliases
        else None,
        deletions=deleted_keys(args.xml_dir),
        harvested_distributions=read_csv(
            args.harvested_distributions, ("friendlier_id", "distribution_url")
        )
        if args.harvested_distributions
        else None,
        decisions=read_csv(args.decisions, ("harvested_id", "action", "canonical_id"))
        if args.decisions
        else None,
        excluded_enrichment_fields=tuple(args.exclude_enrichment_field),
    )
    input_paths = [
        path
        for name, path in vars(args).items()
        if isinstance(path, Path) and name not in {"output_dir", "xml_dir"}
    ]
    if args.xml_dir:
        input_paths.extend(sorted(args.xml_dir.rglob("*.xml")))
    destinations = [args.output_dir / f"{name}.csv" for name in outputs] + [
        args.output_dir / "summary.json"
    ]
    if {p.resolve() for p in input_paths} & {p.resolve() for p in destinations}:
        raise ValueError(
            "Output paths must not overwrite input files; choose a new output directory"
        )
    summary = {
        "harvested_records": len(outputs["reconciled_primary"]),
        "match_status_counts": dict(Counter(outputs["crosswalk"]["match_status"])),
        "missing_existing_counts": dict(
            Counter(outputs["missing_existing"]["reconciliation_status"])
        ),
        "enriched_records": outputs["enrichment_audit"]["canonical_id"].nunique(),
        "enriched_cells": len(outputs["enrichment_audit"]),
        "inputs": [
            {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            for path in input_paths
        ],
        "blank_enrichment_policy": "Keep harvested value",
        "excluded_enrichment_fields": args.exclude_enrichment_field,
        "new_status_meaning": "No match in supplied inventory; not proof of recent source publication",
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for name, frame in outputs.items():
        frame.to_csv(args.output_dir / f"{name}.csv", index=False)
    (args.output_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(
        json.dumps(
            {key: value for key, value in summary.items() if key != "inputs"}, indent=2
        )
    )
    print(f"Outputs: {args.output_dir}")


if __name__ == "__main__":
    main()
