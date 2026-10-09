"""Offline proof of concept. Run from repository root; never builds uploads."""

import argparse
import contextlib
import json
import os
from pathlib import Path

import pandas as pd
import yaml

from harvesters.arcgis import ArcGISHarvester, arcgis_harvest_identifier_and_id
from utils.distribution_writer import generate_secondary_table
from utils.field_order import PRIMARY_FIELD_ORDER
from utils.arcgis_parent_context import prepare_parent_context
from harvesters.arcgis import arcgis_filter_rows

ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = {
    "parcels": "27d9f56cfec6454688110d08ae6826a0",
    "precipitation": "9d1d85f25800420f9d4ceb1cc572b039",
    "temperature": "1d89e0fbe00f44cfb544d78f47cd0d2a",
    "lakeshore": "5d8d7cb16fa34a2291b3312e68f15971",
}


def transform(catalog, code, enabled=True, write_directory=None):
    config = yaml.safe_load((ROOT / "config/arcgis.yaml").read_text())
    for key in ("input_csv", "hub_metadata_csv"):
        config[key] = str(ROOT / config[key])
    config["build_uploads"] = False
    if not enabled:
        config.pop("source_policies", None)
    harvester = ArcGISHarvester(config)
    harvester.load_reference_data()
    defaults = pd.read_csv(config["hub_metadata_csv"], dtype=str, keep_default_na=False)
    defaults = defaults.loc[defaults["Code"] == code].iloc[0].to_dict()
    workflow = {"Code": code, "Endpoint URL": defaults["Endpoint URL"],
                "Website Platform": "ArcGIS Hub", "Endpoint Description": "DCAT API"}
    flat = harvester.flatten([{"workflow": workflow, "hub_defaults": defaults, "fetched_catalog": catalog}])
    df = harvester.build_dataframe(flat)
    df = harvester.derive_fields(df)
    df = harvester.add_defaults(df)
    # Dataset-only comparison: omit administrative website rows, retain normal cleanup.
    df["Date Accessioned"] = "2026-10-08"
    df = harvester.clean(df)
    df = harvester.validate(df)
    primary = df.reindex(columns=[c for c in PRIMARY_FIELD_ORDER if c in df.columns])
    distributions = generate_secondary_table(df, harvester.distribution_types)
    if write_directory is not None:
        write_directory.mkdir(parents=True, exist_ok=True)
        previous = Path.cwd()
        try:
            os.chdir(write_directory)
            # Exercise the actual ArcGIS and base CSV writers; never build uploads.
            paths = harvester.write_outputs(df)
            written_primary = pd.read_csv(paths["primary_csv"], dtype=str, keep_default_na=False)
            written_distributions = pd.read_csv(paths["distributions_csv"], dtype=str, keep_default_na=False)
            pd.testing.assert_frame_equal(primary.fillna("").astype(str), written_primary)
            pd.testing.assert_frame_equal(distributions.fillna("").astype(str), written_distributions)
        finally:
            os.chdir(previous)
    return primary, distributions


def canonical(df):
    return df.fillna("").sort_values(list(df.columns)).reset_index(drop=True)


def generate(destination):
    destination.mkdir(parents=True, exist_ok=True)
    counts, comparisons, unmapped = [], [], []
    for name, filename, code in (("mngeo", "MnGeoHubDCAT1.1feed.json", "05a-01"),
                                  ("minneapolis", "MplsHubDCAT1.1.json", "05c-01")):
        catalog = json.loads((ROOT / "feed_examples" / filename).read_text(encoding="utf-8-sig"))
        before, bd = transform(catalog, code, False, destination / f"{name}_before_writer")
        after, ad = transform(catalog, code, True, destination / f"{name}_after_writer")
        reverse, rd = transform({**catalog, "dataset": list(reversed(catalog["dataset"]))}, code, True)
        repeat, repeat_d = transform(catalog, code, True)
        pd.testing.assert_frame_equal(canonical(after), canonical(reverse))
        pd.testing.assert_frame_equal(canonical(ad), canonical(rd))
        pd.testing.assert_frame_equal(canonical(after), canonical(repeat))
        pd.testing.assert_frame_equal(canonical(ad), canonical(repeat_d))
        assert after["ID"].is_unique and after["Identifier"].is_unique
        assert set(before.ID) <= set(after.ID)
        assert set(ad.friendlier_id) <= set(after.ID)
        shared = before.set_index("ID").index
        for column in ("Description", "Bounding Box", "Identifier"):
            pd.testing.assert_series_equal(before.set_index("ID").loc[shared, column],
                                           after.set_index("ID").loc[shared, column])
        if name == "minneapolis":
            pd.testing.assert_frame_equal(before, after)
            pd.testing.assert_frame_equal(bd, ad)
        for phase, primary, dist in (("before", before, bd), ("after", after, ad)):
            primary.to_csv(destination / f"{name}_{phase}_primary.csv", index=False)
            dist.to_csv(destination / f"{name}_{phase}_distributions.csv", index=False)
            assert list(pd.read_csv(destination / f"{name}_{phase}_primary.csv", nrows=0).columns) == list(before.columns)
            counts.append({"source": name, "phase": phase, "records": len(primary),
                           "unique_items": primary.ID.str.split("_").str[0].nunique(),
                           "distribution_rows": len(dist),
                           "renamed_existing": int((before.set_index("ID").Title != after.set_index("ID").loc[before.ID, "Title"]).sum()) if phase == "after" else 0})
        if name == "mngeo":
            by_id = after.set_index("ID")
            old = before.set_index("ID")
            prepared = prepare_parent_context(catalog["dataset"], arcgis_filter_rows)
            for resource in prepared:
                if resource.get("_include_parent"):
                    _, parent_id = arcgis_harvest_identifier_and_id(resource["identifier"])
                    assert (after.ID == parent_id).sum() == 1
            parcel_titles = after.loc[after.Title.str.contains("Metropolitan 7-County Parcel"), "Title"]
            assert parcel_titles.is_unique
            for year in (2021, 2022, 2023, 2024, 2025):
                for geometry in ("Points", "Polygons"):
                    assert parcel_titles.str.contains(f"Metropolitan 7-County Parcel {geometry} - {year}", regex=False).any()
            for resource in catalog["dataset"]:
                _, identifier = arcgis_harvest_identifier_and_id(resource["identifier"])
                item = identifier.split("_")[0]
                if identifier not in by_id.index:
                    continue
                # Every recognized HTTP download and GeoService must survive into CSV.
                urls = set(ad.loc[ad.friendlier_id == identifier, "distribution_url"])
                for d in resource.get("distribution", []):
                    if d.get("title") in {"ArcGIS GeoService", "Service URL", "CSV", "Shapefile", "GeoJSON", "KML", "File Geodatabase", "Geopackage", "GeoPackage", "Feature Collection", "Excel", "SQLite Geodatabase"}:
                        url = d.get("downloadURL") or d.get("accessURL")
                        if url and url.startswith(("http://", "https://")):
                            assert url in urls, (identifier, url)
                    url = d.get("downloadURL") or d.get("accessURL")
                    if url and url not in urls:
                        unmapped.append({"ID": identifier, "distribution_title": d.get("title"),
                                         "url": url, "format": d.get("format"),
                                         "reason": "Unclassified source link; review required"})
                family = next((label for label, value in EXAMPLES.items() if value == item), None)
                if family or "Metropolitan 7-County Parcel" in by_id.loc[identifier, "Title"]:
                    comparisons.append({"example": family or "other_parcel_year_or_geometry", "ID": identifier,
                                        "feed_identifier": resource["identifier"], "Identifier": by_id.loc[identifier, "Identifier"],
                                        "parent_ID": item if "_" in identifier else "", "role": "sublayer" if "_" in identifier else "parent",
                                        "feed_title": resource["title"], "before_title": old.loc[identifier, "Title"] if identifier in old.index else "",
                                        "after_title": by_id.loc[identifier, "Title"], "links": json.dumps(sorted(urls))})
    pd.DataFrame(counts).to_csv(destination / "counts.csv", index=False)
    pd.DataFrame(comparisons).to_csv(destination / "representative_comparison.csv", index=False)
    pd.DataFrame(unmapped).to_csv(destination / "unmapped_links_review.csv", index=False)
    print(pd.DataFrame(counts).to_string(index=False))
    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "experiments/mngeo-parent-context")
    args = parser.parse_args()
    with contextlib.redirect_stdout(open(os.devnull, "w")):
        result = generate(args.output.resolve())
    print(json.dumps(result, indent=2))
