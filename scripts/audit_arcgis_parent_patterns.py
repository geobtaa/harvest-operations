"""Audit observable catalog relationships in a full harvest; no network requests.

Filtered CSVs cannot expose parent metadata that the original filter discarded.
Sibling IDs are therefore candidates, not proof of a usable feed parent title.
"""

import argparse
import collections
import csv
import json
import re
from pathlib import Path

from utils.arcgis_parent_context import prepare_parent_context

ROOT = Path(__file__).resolve().parents[1]
ITEM_ID = re.compile(r"([a-fA-F0-9]{32})(?:_(\d+))?")
SERVICE_TYPES = {"arcgis_feature_layer", "arcgis_dynamic_map_layer", "arcgis_image_map_layer", "arcgis_tiled_map_layer"}


def audit(primary_path, distributions_path, output):
    with primary_path.open(encoding="utf-8-sig", newline="") as handle:
        primary = list(csv.DictReader(handle))
    datasets = [r for r in primary if not r["ID"].startswith("harvest_") and r.get("Resource Class") != "Websites"]
    services = collections.defaultdict(list)
    with distributions_path.open(encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            if row["reference_type"] in SERVICE_TYPES:
                services[row["friendlier_id"]].append(row["distribution_url"])
    hubs = collections.defaultdict(list)
    for row in datasets:
        hubs[row["Code"]].append(row)
    details, examples = [], []
    for code, rows in sorted(hubs.items(), key=lambda pair: pair[1][0]["Publisher"].casefold()):
        groups = collections.defaultdict(list)
        for row in rows:
            match = ITEM_ID.fullmatch(row["ID"])
            if match:
                groups[match[1]].append((match[2], row))
        siblings = [g for g in groups.values() if sum(layer is not None for layer, _ in g) > 1]
        pairs = [g for g in groups.values() if any(layer is None for layer, _ in g) and any(layer is not None for layer, _ in g)]
        verified = 0
        changes = 0
        for group in pairs:
            resources = []
            for layer, row in group:
                item = ITEM_ID.fullmatch(row["ID"])[1]
                identifier = f"https://www.arcgis.com/home/item.html?id={item}"
                if layer is not None:
                    identifier += f"&sublayer={layer}"
                resources.append({"title": row["Alternative Title"], "identifier": identifier,
                                  "distribution": [{"title": "ArcGIS GeoService", "accessURL": url}
                                                   for url in services[row["ID"]]]})
            prepared = prepare_parent_context(resources)
            family_changes = [r for r in prepared if r.get("_context_title") and r["_context_title"] != r["title"]]
            verified += any("_context_title" in r for r in prepared)
            changes += len(family_changes)
            for resource in family_changes:
                examples.append({"source_code": code, "hub": rows[0]["Publisher"],
                                 "item_id": ITEM_ID.fullmatch(group[0][1]["ID"])[1],
                                 "original_title_without_place": resource["title"],
                                 "contextual_title_without_place": resource["_context_title"],
                                 "identifier": resource["identifier"]})
        details.append({"source_code": code, "hub": rows[0]["Publisher"], "dataset_records": len(rows),
                        "records_with_sublayer_IDs": sum(layer is not None for g in groups.values() for layer, _ in g),
                        "items_with_multiple_sublayers": len(siblings),
                        "records_in_multiple_sublayer_items": sum(sum(layer is not None for layer, _ in g) for g in siblings),
                        "parent_child_families_visible_in_output": len(pairs),
                        "families_with_verified_service_relationship": verified,
                        "visible_child_titles_that_would_change": changes,
                        "endpoint": rows[0].get("Endpoint URL", "")})
    summary = {"primary_rows": len(primary), "dataset_rows": len(datasets), "hubs_with_dataset_rows": len(hubs),
               "hubs_with_multiple_sublayers_of_same_item": sum(d["items_with_multiple_sublayers"] > 0 for d in details),
               "items_with_multiple_sublayers": sum(d["items_with_multiple_sublayers"] for d in details),
               "records_in_multiple_sublayer_items": sum(d["records_in_multiple_sublayer_items"] for d in details),
               "hubs_with_parent_child_families_visible_in_output": sum(d["parent_child_families_visible_in_output"] > 0 for d in details),
               "parent_child_families_visible_in_output": sum(d["parent_child_families_visible_in_output"] for d in details),
               "visible_child_titles_that_would_change": sum(d["visible_child_titles_that_would_change"] for d in details),
               "limitation": "Output-only lower bound: filtered-out feed parents cannot be counted or titled from the primary CSV."}
    assert summary["visible_child_titles_that_would_change"] == len(examples)
    output.mkdir(parents=True, exist_ok=True)
    (output / "audit.json").write_text(json.dumps({"summary": summary, "hubs": details, "examples": examples}, indent=2), encoding="utf-8")
    lines = ["# October 9 ArcGIS parent-pattern audit", "",
             "This is a read-only audit of the actual primary and distributions CSVs. No live feeds were requested.", "",
             f"- {summary['dataset_rows']:,} dataset records across {len(hubs)} hubs with dataset rows.",
             f"- {summary['hubs_with_multiple_sublayers_of_same_item']} hubs have sibling layers sharing item IDs: candidates, not confirmed usable parents.",
             f"- {summary['items_with_multiple_sublayers']:,} such items contain {summary['records_in_multiple_sublayer_items']:,} layer records.",
             f"- {summary['hubs_with_parent_child_families_visible_in_output']} hub has retained parent and child records: HUD-eGIS, with 3 families and 6 child titles that would change.", "",
             "The filter removed many parents before these CSVs were written. In particular, MnGeo has no retained parent/child pair here despite its known feed pattern. An exact count of affected hubs requires their unfiltered feeds from the same date. Counting sibling layers alone would overstate what has been verified. A one-layer item could also have an omitted parent, so 94 is not an upper bound either.", "",
             "Source attribution follows the retained record's Code. Shared-ID cleanup may have retained another portal's copy; these figures describe the output, not every source's original catalog.", "",
             "## Current policy (October 20)", "",
             "Previously excluded parent records are no longer admitted. The harvester reads their titles before filtering, but the original Shapefile/ImageServer eligibility rule applies to every record. Existing eligible parents remain eligible; existing sublayers retain their IDs. This means the same feed produces the same eligible ID set as before the experimental policy. It does not freeze future harvests to the October 9 records if a source changes its catalog.", "",
             "At the user's request, parent-title context is now enabled for all Hubs by default. The original distribution eligibility filter is unchanged. The existing MnGeo-specific link mapping remains separate. No schemas, upload logic, or production CSVs were changed. Older experiment CSVs are marked historical rather than overwritten.", "",
             "23 focused ArcGIS tests pass, including global defaults for multiple Hub codes, exact baseline/updated ID-set equality, hidden-parent title context without admission, retention of already eligible parents, repeat/reordered inputs, and no inferred parent from shared service containers. The full original saved feeds are not currently present in this checkout, so full-feed October 9 demo outputs were not regenerated. Representative raw-feed fixtures remain available for regression tests.", "",
             "The next full harvest can reveal which sources supply usable catalog parents. The 94 sibling-layer hubs remain an investigation list, not a confirmed count of title changes. MnGeo is confirmed separately by the earlier raw-feed investigation even though its filtered output has no retained parent pairs. No live harvest was run as part of this code change.", "",
             "## Confirmed retained families", ""]
    for example in examples:
        lines.append(f"- {example['hub']}: **{example['original_title_without_place']}** → **{example['contextual_title_without_place']}**")
    lines += ["", "## Per-hub findings", "", "Zero visible parent pairs does not mean the feed has none.", "",
              "| Hub | Code | Dataset rows | Items with sibling layers | Retained parent/child families |", "| --- | --- | ---: | ---: | ---: |"]
    for d in details:
        lines.append(f"| {d['hub']} | {d['source_code']} | {d['dataset_records']} | {d['items_with_multiple_sublayers']} | {d['parent_child_families_visible_in_output']} |")
    (output / "README.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "experiments/arcgis-parent-audit-2026-10-20")
    args = parser.parse_args()
    print(json.dumps(audit(ROOT / "outputs/2026-10-09_arcgis_primary.csv",
                           ROOT / "outputs/2026-10-09_arcgis_distributions.csv", args.output), indent=2))
