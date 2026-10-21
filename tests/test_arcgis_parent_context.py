import copy
import json
from pathlib import Path

import pandas as pd
import pytest

from harvesters.arcgis import ArcGISHarvester, arcgis_filter_rows
from scripts.demo_mngeo_parent_context import canonical, transform
from utils.arcgis_parent_context import distribution_fields, item_layer, prepare_parent_context

FIXTURES = Path(__file__).parent / "fixtures"


def catalog(name="mngeo"):
    return json.loads((FIXTURES / f"{name}_parent_context.json").read_text(encoding="utf-8"))


def parcels():
    records = [r for r in catalog()["dataset"] if item_layer(r)[0] == "27d9f56cfec6454688110d08ae6826a0"]
    return sorted(records, key=lambda r: (item_layer(r)[1] is not None, item_layer(r)[1] or ""))


@pytest.fixture(scope="module")
def results():
    source = catalog()
    return source, transform(source, "05a-01", False), transform(source, "05a-01", True)


def test_parents_children_identifiers_and_metadata(results):
    source, (before, _), (after, distributions) = results
    assert len(before) == 13
    assert len(after) == 13
    assert after.ID.is_unique and after.Identifier.is_unique
    assert set(before.ID) == set(after.ID)
    for item in {item_layer(r)[0] for r in source["dataset"]}:
        assert item not in set(after.ID)
    for field in ("Description", "Bounding Box", "Identifier"):
        pd.testing.assert_series_equal(before.set_index("ID")[field],
                                       after.set_index("ID").loc[before.ID, field])
    assert set(distributions.friendlier_id) <= set(after.ID)
    row = after.set_index("ID").loc["27d9f56cfec6454688110d08ae6826a0_3"]
    assert row.Title == "Hennepin County Parcels - Metropolitan 7-County Parcel Polygons - 2025 [Minnesota]"
    assert row["Temporal Coverage"] == "2025"
    urls = set(distributions.loc[distributions.friendlier_id == row.name, "distribution_url"])
    assert "https://arcgis.metc.state.mn.us/data1/rest/services/parcels/Parcels_2025/FeatureServer/3" in urls
    assert "https://gis.data.mn.gov/api/download/v1/items/27d9f56cfec6454688110d08ae6826a0/shapefile?layers=3" in urls


def test_repeat_reorder_no_input_mutation(results):
    source, _, after = results
    original = copy.deepcopy(source)
    for records in (source["dataset"], list(reversed(source["dataset"]))):
        actual = transform({"dataset": records}, "05a-01")
        for expected_df, actual_df in zip(after, actual):
            pd.testing.assert_frame_equal(canonical(expected_df), canonical(actual_df))
    assert source == original
    prepared = prepare_parent_context(source["dataset"])
    assert prepare_parent_context(prepared) == prepared


def test_minneapolis_unchanged():
    source = catalog("minneapolis")
    for before, after in zip(transform(source, "05c-01", False), transform(source, "05c-01", True)):
        pd.testing.assert_frame_equal(before, after)


def test_missing_or_mismatched_parent_does_not_admit_or_rename():
    records = parcels()
    orphan = prepare_parent_context(records[1:])
    assert all("_context_title" not in r for r in orphan)
    records = copy.deepcopy(records)
    records[0]["distribution"][1]["accessURL"] = "https://example.org/unrelated/FeatureServer"
    prepared = prepare_parent_context(records)
    assert "_include_parent" not in prepared[0]
    assert all("_context_title" not in r for r in prepared)


def test_duplicate_conflicts_and_duplicate_titles():
    records = parcels()
    assert len(prepare_parent_context(records + [copy.deepcopy(records[0])])) == 8
    conflict = copy.deepcopy(records[0])
    conflict["title"] = "Conflicting parent"
    with pytest.raises(ValueError, match="Conflicting ArcGIS identifier"):
        prepare_parent_context(records + [conflict])
    records[1]["title"] = records[2]["title"] = records[0]["title"]
    prepared = prepare_parent_context(records)
    assert prepared[1]["_context_title"].endswith("Layer 0")
    assert prepared[2]["_context_title"].endswith("Layer 1")


def test_multi_service_and_download_lists_use_existing_writer():
    from utils.distribution_writer import build_secondary_table
    dists = [{"title": "ArcGIS GeoService", "accessURL": f"https://example.org/FeatureServer/{i}"} for i in (1, 0)]
    dists += [{"title": "Shapefile", "accessURL": "https://example.org/data.zip"}]
    fields = distribution_fields(dists + dists, {})
    df = pd.DataFrame([{**fields, "ID": "item"}])
    written = build_secondary_table(df, [{"key": "arcgis_feature_layer", "variables": ["featureService"]},
                                          {"key": "download", "variables": ["download"]}])
    assert len(written) == 3
    assert written.loc[written.reference_type == "download", "label"].tolist() == ["Shapefile"]


def test_legacy_filter_stays_narrow():
    resource = {"title": "Valid", "distribution": [{"title": "ArcGIS GeoService", "accessURL": "https://example.org/FeatureServer"}]}
    assert arcgis_filter_rows(pd.DataFrame([{"resource": resource}])).empty
    resource["distribution"][0]["accessURL"] = "https://example.org/ImageServer"
    assert len(arcgis_filter_rows(pd.DataFrame([{"resource": resource}]))) == 1
    resource["title"] = "{{title}}"
    assert arcgis_filter_rows(pd.DataFrame([{"resource": resource}])).empty


def test_existing_eligible_parent_is_retained_and_hidden_parent_only_supplies_title():
    records = parcels()
    # Originally ineligible root: title context must not admit it.
    before = transform({"dataset": records}, "05a-01", False)[0]
    after = transform({"dataset": records}, "05a-01", True)[0]
    assert set(before.ID) == set(after.ID)
    assert item_layer(records[0])[0] not in set(after.ID)
    # A parent that already passes the original filter must still be kept once.
    records[0]["distribution"].append({"title": "Shapefile", "accessURL": "https://example.org/parent.zip"})
    before = transform({"dataset": records}, "05a-01", False)[0]
    after = transform({"dataset": records}, "05a-01", True)[0]
    parent_id = item_layer(records[0])[0]
    assert set(before.ID) == set(after.ID)
    assert (after.ID == parent_id).sum() == 1


def test_obsolete_parent_marker_cannot_bypass_filter():
    resource = copy.deepcopy(parcels()[0])
    resource["_include_parent"] = True
    assert arcgis_filter_rows(pd.DataFrame([{"resource": resource}])).empty


@pytest.mark.parametrize("code", ["05a-01", "05c-01", "1000-0001", "unconfigured-hub"])
def test_parent_context_defaults_on_for_every_hub_without_changing_eligibility_or_links(code):
    config = {"input_csv": "unused.csv", "hub_metadata_csv": "unused.csv",
              "output_report_csv": "unused.csv"}
    source = [{"workflow": {"Code": code}, "hub_defaults": {},
               "fetched_catalog": {"dataset": parcels()}}]
    legacy = ArcGISHarvester({**config, "parent_sublayer_context": False})
    current = ArcGISHarvester(config)
    before = legacy.build_dataframe(legacy.flatten(source))
    after = current.build_dataframe(current.flatten(source))
    assert len(before) == len(after) == 7
    assert set(before.identifier_raw) == set(after.identifier_raw)
    assert before.iloc[3]["Alternative Title"] == "Hennepin County Parcels"
    assert after.iloc[3]["Alternative Title"] == "Hennepin County Parcels - Metropolitan 7-County Parcel Polygons - 2025"
    for column in ("Description", "spatial", "information", "distributions", "featureService", "mapService", "Format"):
        pd.testing.assert_series_equal(before[column], after[column])
    # The input catalog still contains its original wording.
    assert source[0]["fetched_catalog"]["dataset"][4]["title"] == "Hennepin County Parcels"


def test_shared_service_name_with_different_item_ids_does_not_supply_catalog_parent():
    parent, child = copy.deepcopy(parcels()[:2])
    parent["title"] = "Transportation"
    child["title"] = "Tree Inventory"
    child["identifier"] = child["identifier"].replace(item_layer(parent)[0], "a" * 32)
    prepared = prepare_parent_context([parent, child])
    assert all("_context_title" not in r for r in prepared)


def test_sublayer_flag_means_parent_suffix_was_actually_added():
    records = parcels()
    records[1]["title"] = records[0]["title"]  # Already has the parent wording.
    records[2]["title"] = "Carver County Parcels - " + records[0]["title"]
    prepared = prepare_parent_context(records)
    assert not prepared[0].get("_inherited_parent_suffix", False)
    assert not prepared[1]["_inherited_parent_suffix"]  # Layer-number-only change.
    assert not prepared[2]["_inherited_parent_suffix"]
    assert prepared[3]["_inherited_parent_suffix"]
    primary = transform({"dataset": records}, "05a-01")[0].set_index("ID")
    item = item_layer(records[0])[0]
    assert not primary.loc[f"{item}_0", "sublayer"]
    assert not primary.loc[f"{item}_1", "sublayer"]
    assert primary.loc[f"{item}_2", "sublayer"]
    assert primary["sublayer"].dtype == bool


def test_written_primary_schema_only_adds_boolean_sublayer(tmp_path, monkeypatch):
    from utils.distribution_writer import load_distribution_types
    from utils.field_order import PRIMARY_FIELD_ORDER
    config = {"input_csv": "unused.csv", "hub_metadata_csv": "unused.csv",
              "output_report_csv": "reports/arcgis_report.csv",
              "output_primary_csv": "arcgis_primary.csv",
              "output_distributions_csv": "arcgis_distributions.csv", "build_uploads": False,
              "primary_registry_csv": str(Path(__file__).resolve().parents[1] / "registry/arcgis_primary_registry.csv")}
    harvester = ArcGISHarvester(config)
    harvester.distribution_types = load_distribution_types()
    records = parcels()
    records[0]["distribution"].append({"title": "Shapefile", "accessURL": "https://example.org/parent.zip"})
    flat = harvester.flatten([{"workflow": {"Code": "test"}, "hub_defaults": {},
                               "fetched_catalog": {"dataset": records}}])
    frame = harvester.build_dataframe(flat).pipe(harvester.derive_fields).pipe(harvester.add_defaults).pipe(harvester.clean)
    old_columns = [c for c in PRIMARY_FIELD_ORDER if c in frame.columns]
    monkeypatch.chdir(tmp_path)
    paths = harvester.write_outputs(frame)
    written = pd.read_csv(paths["primary_csv"], keep_default_na=False)
    assert list(written.columns) == old_columns + ["sublayer"]
    assert written.sublayer.dtype == bool
    parent_id = item_layer(records[0])[0]
    assert not written.set_index("ID").loc[parent_id, "sublayer"]
    assert written.loc[written.ID != parent_id, "sublayer"].all()
    pd.testing.assert_frame_equal(written[old_columns].astype(str), frame[old_columns].astype(str), check_dtype=False)
    assert list(pd.read_csv(paths["distributions_csv"], nrows=0).columns) == ["friendlier_id", "reference_type", "distribution_url", "label"]
    from harvesters.base import BaseHarvester
    baseline = BaseHarvester({**config, "output_primary_csv": "baseline_primary.csv"})
    base_paths = baseline.write_outputs(frame)
    assert list(pd.read_csv(base_paths["primary_csv"], nrows=0).columns) == old_columns
