import copy
import json
from pathlib import Path

import pandas as pd
import pytest

from harvesters.arcgis import arcgis_filter_rows
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
    assert len(after) == 17
    assert after.ID.is_unique and after.Identifier.is_unique
    assert set(before.ID) <= set(after.ID)
    for item in {item_layer(r)[0] for r in source["dataset"]}:
        assert (after.ID == item).sum() == 1
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
    prepared = prepare_parent_context(source["dataset"], arcgis_filter_rows)
    assert prepare_parent_context(prepared, arcgis_filter_rows) == prepared


def test_minneapolis_unchanged():
    source = catalog("minneapolis")
    for before, after in zip(transform(source, "05c-01", False), transform(source, "05c-01", True)):
        pd.testing.assert_frame_equal(before, after)


def test_missing_or_mismatched_parent_does_not_admit_or_rename():
    records = parcels()
    orphan = prepare_parent_context(records[1:], arcgis_filter_rows)
    assert all("_context_title" not in r for r in orphan)
    records = copy.deepcopy(records)
    records[0]["distribution"][1]["accessURL"] = "https://example.org/unrelated/FeatureServer"
    prepared = prepare_parent_context(records, arcgis_filter_rows)
    assert not prepared[0]["_include_parent"]
    assert all("_context_title" not in r for r in prepared)


def test_duplicate_conflicts_and_duplicate_titles():
    records = parcels()
    assert len(prepare_parent_context(records + [copy.deepcopy(records[0])], arcgis_filter_rows)) == 8
    conflict = copy.deepcopy(records[0])
    conflict["title"] = "Conflicting parent"
    with pytest.raises(ValueError, match="Conflicting ArcGIS identifier"):
        prepare_parent_context(records + [conflict], arcgis_filter_rows)
    records[1]["title"] = records[2]["title"] = records[0]["title"]
    prepared = prepare_parent_context(records, arcgis_filter_rows)
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
