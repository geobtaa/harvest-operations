from copy import deepcopy

import pandas as pd
import pytest

from harvesters.arcgis import ArcGISHarvester
from utils.arcgis_parent_context import prepare_parent_context


ITEM = "008788cd5a0a48ddbc2a7340608da221"
ROOT_URL = (
    "https://services7.arcgis.com/iwxhJVOFEKDxO7gk/ArcGIS/rest/services/"
    "Lassen_2026_Roll_Year/FeatureServer"
)


def catalog():
    # The item ID, titles and service relationship match the live California feed.
    return [
        {
            "title": "Lassen 2026 Roll Year",
            "identifier": f"https://www.arcgis.com/home/item.html?id={ITEM}",
            "distribution": [{"title": "ArcGIS GeoService", "accessURL": ROOT_URL}],
        },
        {
            "title": "C18 2026",
            "identifier": f"https://www.arcgis.com/home/item.html?id={ITEM}&sublayer=2",
            "publisher": {"name": "California GIS"},
            "spatial": "-121,39,-120,40",
            "distribution": [
                {"title": "ArcGIS GeoService", "accessURL": ROOT_URL.replace("/ArcGIS/", "/arcgis/") + "/2"},
                {"title": "Shapefile", "accessURL": "https://example.org/parcels.zip"},
            ],
        },
    ]


def harvester(enabled=True):
    return ArcGISHarvester({
        "input_csv": "unused.csv",
        "hub_metadata_csv": "unused.csv",
        "output_report_csv": "unused.csv",
        "parent_sublayer_context": enabled,
    })


def frame(records, enabled=True):
    instance = harvester(enabled)
    flat = instance.flatten([{
        "workflow": {"Code": "18a-01"},
        "hub_defaults": {"Code": "18a-01", "Spatial Coverage": "California"},
        "fetched_catalog": {"dataset": records},
    }])
    return instance.build_dataframe(flat).pipe(instance.derive_fields)


def test_lassen_title_and_eligibility_and_links():
    resources = catalog()
    original = deepcopy(resources)
    before = frame(resources, enabled=False)
    after = frame(resources)
    assert after["Title"].tolist() == [
        "C18 2026 (sublayer of Lassen 2026 Roll Year) [California]"
    ]
    # The root is ineligible under the normal filter and supplies context only.
    assert before["ID"].tolist() == after["ID"].tolist() == [f"{ITEM}_2"]
    for column in ("Identifier", "Description", "distributions", "featureService", "Bounding Box"):
        pd.testing.assert_series_equal(before[column], after[column])
    assert resources == original
    assert prepare_parent_context(prepare_parent_context(resources)) == prepare_parent_context(resources)
    assert frame(list(reversed(resources)))["Title"].tolist() == after["Title"].tolist()


@pytest.mark.parametrize("case", [
    "no_parent", "different_item", "different_service", "different_layer",
    "no_sublayer", "blank_title", "placeholder_title", "ambiguous_parent",
    "missing_distributions",
])
def test_unconfirmed_catalog_relationship_leaves_title_unchanged(case):
    resources = catalog()
    if case == "no_parent":
        resources = resources[1:]
    elif case == "different_item":
        resources[0]["identifier"] = "https://www.arcgis.com/home/item.html?id=other"
    elif case == "different_service":
        resources[0]["distribution"][0]["accessURL"] = "https://example.org/other/FeatureServer"
    elif case == "different_layer":
        resources[1]["distribution"][0]["accessURL"] = ROOT_URL + "/3"
    elif case == "no_sublayer":
        resources[1]["identifier"] = f"https://www.arcgis.com/home/item.html?id={ITEM}"
    elif case == "blank_title":
        resources[0]["title"] = ""
    elif case == "placeholder_title":
        resources[0]["title"] = "{{title}}"
    elif case == "ambiguous_parent":
        resources.insert(0, {**deepcopy(resources[0]), "title": "Conflicting parent"})
    elif case == "missing_distributions":
        resources[0]["distribution"] = None
    prepared = prepare_parent_context(resources)
    assert all("_context_title" not in resource for resource in prepared)
    assert len(prepared) == len(resources)


def test_context_does_not_repeat_parent_wording_or_admit_blank_child_titles():
    for title in ("Lassen 2026 Roll Year - C18", "", "{{title}}"):
        resources = catalog()
        resources[1]["title"] = title
        assert "_context_title" not in prepare_parent_context(resources)[1]


def test_mapserver_pairs_and_eligible_parent_remain_supported():
    resources = catalog()
    for resource in resources:
        resource["distribution"][0]["accessURL"] = resource["distribution"][0]["accessURL"].replace(
            "FeatureServer", "MapServer"
        )
    resources[0]["distribution"].append({"title": "Shapefile", "accessURL": "https://example.org/root.zip"})
    before, after = frame(resources, False), frame(resources)
    assert before["ID"].tolist() == after["ID"].tolist() == [ITEM, f"{ITEM}_2"]
    assert after.iloc[0]["Alternative Title"] == "Lassen 2026 Roll Year"
    assert after.iloc[1]["Title"] == "C18 2026 (sublayer of Lassen 2026 Roll Year) [California]"
    pd.testing.assert_series_equal(before["mapService"], after["mapService"])


def test_context_stays_with_its_own_portal_catalog():
    parent, child = catalog()
    flat = harvester().flatten([
        {"workflow": {}, "fetched_catalog": {"dataset": [parent]}},
        {"workflow": {}, "fetched_catalog": {"dataset": [child]}},
    ])
    assert all("_context_title" not in row["resource"] for row in flat)
