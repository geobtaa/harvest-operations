"""Analyze saved Hub feeds against actual harvest CSVs; never fetch or upload.

Writes JSON intermediates for the comparison CSV builder. The experimental
policy is enabled in memory only, including for the Baltimore County comparison.
"""

import argparse
import collections
import contextlib
import csv
import json
import re
from pathlib import Path

import pandas as pd
import yaml

from harvesters.arcgis import ArcGISHarvester, arcgis_harvest_identifier_and_id
from utils.arcgis_parent_context import item_layer, prepare_parent_context, service_urls
from harvesters.arcgis import arcgis_filter_rows
from utils.distribution_writer import generate_secondary_table

ROOT = Path(__file__).resolve().parents[1]


def service_container(resource):
    for url in service_urls(resource):
        match = re.match(r"(.*/([^/]+)/(?:FeatureServer|MapServer|ImageServer))(?:/\d+)?/?$", url)
        if match:
            return match.group(1), match.group(2)
    return "", ""


def updated_tables(catalog, code):
    config = yaml.safe_load((ROOT / "config/arcgis.yaml").read_text())
    config["source_policies"] = {code: {"parent_sublayer_context": True}}
    config["build_uploads"] = False
    config["hub_metadata_csv"] = str(ROOT / config["hub_metadata_csv"])
    h = ArcGISHarvester(config)
    h.load_reference_data()
    websites = pd.read_csv(config["hub_metadata_csv"], dtype=str, keep_default_na=False)
    defaults = websites.loc[websites.Code == code].iloc[0].to_dict()
    flat = h.flatten([{"workflow": {"Code": code}, "hub_defaults": defaults,
                       "fetched_catalog": catalog}])
    df = h.build_dataframe(flat).pipe(h.derive_fields).pipe(h.add_defaults)
    df = df.pipe(h.clean).pipe(h.validate)
    return df, generate_secondary_table(df, h.distribution_types)


def analyze(output, harvest_date="2026-10-09"):
    primary_path = ROOT / "outputs" / f"{harvest_date}_arcgis_primary.csv"
    dist_path = ROOT / "outputs" / f"{harvest_date}_arcgis_distributions.csv"
    with primary_path.open(encoding="utf-8-sig", newline="") as handle:
        primary = list(csv.DictReader(handle))
    with dist_path.open(encoding="utf-8-sig", newline="") as handle:
        distribution_rows = list(csv.DictReader(handle))
    all_current = {r["ID"]: r for r in primary if not r["ID"].startswith("harvest_")}
    assert len(all_current) == sum(not r["ID"].startswith("harvest_") for r in primary)
    current_links = collections.defaultdict(set)
    for row in distribution_rows:
        current_links[row["friendlier_id"]].add(row["distribution_url"])
    output.mkdir(parents=True, exist_ok=True)
    summaries = []
    for source, code, filename in (
        ("mngeo", "05a-01", "MnGeoHubDCAT1.1feed.json"),
        ("baltimore_county", "04b-24005", "BaltimoreCoHubDCAT1.1.json"),
    ):
        catalog = json.loads((ROOT / "example-feeds" / filename).read_text(encoding="utf-8-sig"))
        proposed, proposed_dist = updated_tables(catalog, code)
        reversed_primary, reversed_dist = updated_tables({**catalog, "dataset": list(reversed(catalog["dataset"]))}, code)
        for first, second, key in ((proposed, reversed_primary, "ID"),
                                    (proposed_dist, reversed_dist, "friendlier_id")):
            cols = [key] if key == "ID" else list(first.columns)
            pd.testing.assert_frame_equal(first.fillna("").sort_values(cols).reset_index(drop=True),
                                           second.fillna("").sort_values(cols).reset_index(drop=True))
        assert proposed.ID.is_unique and proposed.Identifier.is_unique
        assert set(proposed_dist.friendlier_id) <= set(proposed.ID)
        current_rows = [r for r in primary if r.get("Code") == code
                        and not r["ID"].startswith("harvest_") and r.get("Resource Class") != "Websites"]
        current = {r["ID"]: r for r in current_rows}
        assert len(current) == len(current_rows)
        updated = proposed.set_index("ID").to_dict("index")
        proposed_links = collections.defaultdict(set)
        for row in proposed_dist.to_dict("records"):
            proposed_links[row["friendlier_id"]].add(row["distribution_url"])
        groups = collections.defaultdict(list)
        containers = collections.defaultdict(set)
        prepared = prepare_parent_context(catalog["dataset"], arcgis_filter_rows)
        for r in catalog["dataset"]:
            item, _ = item_layer(r)
            groups[item].append(r)
            root, _ = service_container(r)
            if root:
                containers[root].add(item)
        records = []
        selected_families = set()
        for item, members in sorted(groups.items()):
            parents = [r for r in members if item_layer(r)[1] is None]
            parent = parents[0] if len(parents) == 1 and len(members) > 1 else None
            if source == "mngeo":
                if not parent or any("parcel" in r.get("title", "").casefold() for r in members):
                    continue
                selected_families.add(item)
            for resource in sorted(members, key=lambda r: (item_layer(r)[1] is not None, item_layer(r)[1] or "")):
                _, layer = item_layer(resource)
                _, identifier = arcgis_harvest_identifier_and_id(resource["identifier"])
                old = all_current.get(identifier, {})
                new = updated.get(identifier, {})
                root, container_name = service_container(resource)
                notes = []
                if old and old.get("Code") != code:
                    notes.append(f"Already harvested through another portal: {old.get('Code')}")
                if layer is not None and not parent:
                    notes.append("No explicit same-item parent in feed; title unchanged by context rule")
                if parent and parent["title"].casefold() in resource["title"].casefold() and layer is not None:
                    notes.append("Parent name already present; no repeated suffix")
                if container_name and len(containers[root]) > 1:
                    notes.append("Service container shared by different ArcGIS items")
                if resource["title"] == "Tree Inventory":
                    notes.append("Transportation is a service URL label, not an explicit parent dataset title")
                if parent:
                    parent_years = set(re.findall(r"\b(?:19|20)\d{2}\b", parent["title"]))
                    child_years = set(re.findall(r"\b(?:19|20)\d{2}\b", resource["title"]))
                    if layer is not None and parent_years and child_years and parent_years != child_years:
                        notes.append("Parent and layer title years differ; retain source wording for review")
                if identifier not in updated:
                    notes.append("Still excluded by existing filter or parent eligibility checks")
                change = ("not included by updated rules" if not new else
                          "new relative to current harvest" if not old else
                          "same as current harvest" if old["Title"] == new["Title"] else "title changed")
                records.append({
                    "current_title": old.get("Title", ""), "updated_title": new.get("Title", ""),
                    "feed_title": resource["title"], "parent_title_from_feed": parent["title"] if parent else "",
                    "role": "parent" if parent is resource else "sublayer" if layer is not None else "standalone_item",
                    "change": change, "review_notes": "; ".join(notes), "source": source,
                    "ID": identifier, "arcgis_item_ID": item, "sublayer_number": layer or "",
                    "parent_ID": item if parent and layer is not None else "",
                    "currently_harvested": "yes" if old else "no", "included_with_updated_rules": "yes" if new else "no",
                    "current_source_code": old.get("Code", ""),
                    "currently_harvested_via_this_source": "yes" if identifier in current else "no",
                    "service_container_name_from_URL": container_name,
                    "illustrative_service_suffix_NOT_current_policy": (
                        f"{resource['title'].strip()} - {container_name}"
                        if source == "baltimore_county" and container_name and layer is not None
                        and container_name.casefold() not in resource["title"].casefold() else ""),
                    "items_sharing_service_container": len(containers[root]) if root else 0,
                    "service_root_URL": root, "feed_identifier": resource["identifier"],
                    "landing_page": resource.get("landingPage", ""),
                    "current_harvest_links": "\n".join(sorted(current_links[identifier])) if old else "",
                    "updated_links": "\n".join(sorted(proposed_links[identifier])),
                    "feed_file": f"example-feeds/{filename}",
                    "current_primary_file": str(primary_path.relative_to(ROOT)),
                    "current_distributions_file": str(dist_path.relative_to(ROOT)),
                })
        assert len({r["ID"] for r in records}) == len(records)
        if source == "mngeo":
            assert len(selected_families) >= 50
            assert all("parcel" not in r["feed_title"].casefold() for r in records)
        else:
            assert len(records) == len(catalog["dataset"])
            assert all(not r["parent_ID"] for r in records)
        summary = {"source": source, "saved_feed_records": len(catalog["dataset"]),
                   "current_harvest_records": len(current), "updated_saved_feed_records": len(proposed),
                   "explicit_parent_families": sum(any(item_layer(r)[1] is None for r in rs) and len(rs) > 1 for rs in groups.values()),
                   "included_explicit_parents": sum(bool(r.get("_include_parent")) for r in prepared),
                   "comparison_rows": len(records), "comparison_families": len(selected_families),
                   "comparison_changes": dict(collections.Counter(r["change"] for r in records)),
                   "shared_service_containers": sum(len(v) > 1 for v in containers.values()),
                   "current_IDs_missing_from_saved_feed_output": len(set(current) - set(updated)),
                   "saved_output_IDs_absent_from_current_harvest": len(set(updated) - set(current))}
        summary["saved_output_IDs_absent_from_all_current_sources"] = len(set(updated) - set(all_current))
        summary["comparison_records_currently_in_other_sources"] = sum(
            r["currently_harvested"] == "yes" and r["currently_harvested_via_this_source"] == "no" for r in records)
        summaries.append(summary)
        (output / f"{source}_comparison_data.json").write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "summary_data.json").write_text(json.dumps(summaries, indent=2), encoding="utf-8")
    return summaries


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "experiments/hub-title-review-2026-10-09/support")
    args = parser.parse_args()
    with (ROOT / "experiments" / "hub-title-review-analysis.log").open("w", encoding="utf-8") as log:
        with contextlib.redirect_stdout(log):
            result = analyze(args.output.resolve())
    print(json.dumps(result, indent=2))
