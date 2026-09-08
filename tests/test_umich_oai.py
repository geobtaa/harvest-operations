import pandas as pd
import pytest

from harvesters.oai_qdc import OaiQdcHarvester
from utils.metadata_reconciliation import reconcile
from utils.umich_oai import collapse_umich_catalog_items


def xml_record(catalog, image, title="A map", deleted=False):
    return f"""<record><header {'status="deleted"' if deleted else ""}>
    <identifier>oai:quod.lib.umich.edu:IC-CLARK1IC-X-{catalog}%5D{image}</identifier>
    <datestamp>2026-09-06</datestamp></header><metadata>
    <dc xmlns="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dc="http://purl.org/dc/elements/1.1/">
    <dc:title>{title}</dc:title><dc:creator>Cartographer</dc:creator>
    <dc:identifier>http://name.umdl.umich.edu/IC-CLARK1IC-X-{catalog}%5D{image}</dc:identifier>
    <dc:identifier>https://quod.lib.umich.edu/cgi/i/image/api/manifest/clark1ic:{catalog}:{image}</dc:identifier>
    </dc></metadata></record>"""


def make_harvester(tmp_path, grouped=True):
    config = {
        "name": "umich",
        "oai_base_url": "https://quod.lib.umich.edu/cgi/o/oai/oai",
        "source_id_prefix": "um",
        "metadata_prefix": "oai_dc",
        "sets": [{"set": "dlps:clark1ic", "title": "Maps"}],
        "output_primary_csv": "outputs/umich_test_primary.csv",
    }
    if grouped:
        config["record_granularity"] = "umich_catalog_item"
    harvester = OaiQdcHarvester(config)
    harvester.load_reference_data()
    return harvester


def parse_pages(harvester, *pages):
    bundles = [
        {
            "set_spec": "dlps:clark1ic",
            "set_title": "Maps",
            "xml_path": f"{i}.xml",
            "xml_text": '<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"><ListRecords>'
            + page
            + "</ListRecords></OAI-PMH>",
        }
        for i, page in enumerate(pages)
    ]
    return harvester.build_dataframe(harvester.flatten(harvester.parse(bundles)))


def test_group_across_xml_pages_keep_aliases_and_manifest(tmp_path):
    harvester = make_harvester(tmp_path)
    frame = parse_pages(
        harvester,
        xml_record("003281819", "123_01"),
        xml_record("003281819", "123_02")
        + xml_record("003281819", "456")
        + xml_record("003281819", "deleted", deleted=True),
    )
    assert len(frame) == 1
    row = frame.iloc[0]
    assert row["ID"] == "um_clark1ic_3281819"
    assert all(image in row["Identifier"] for image in ["123_01", "123_02", "456"])
    assert "deleted" not in row["Identifier"]
    assert row["Creator"] == "Cartographer"
    manifest = "https://quod.lib.umich.edu/cgi/i/image/api/manifest/clark1ic:003281819"
    assert row["manifest"] == manifest
    dist = harvester.oai_build_distributions(frame)
    assert dist.reference_type.tolist().count("iiif_manifest") == 1
    assert (
        dist.loc[dist.reference_type == "iiif_manifest", "distribution_url"].iloc[0]
        == manifest
    )
    assert len(harvester.umich_item_membership) == 3
    # Existing canonical ID can be anchored to a non-first page.
    result = reconcile(
        frame.drop(columns=["information", "manifest"]),
        pd.DataFrame([{"ID": "old-uuid", "Creator": "Enriched creator"}]),
        pd.DataFrame(
            [
                {
                    "friendlier_id": "old-uuid",
                    "distribution_url": "https://quod.lib.umich.edu/c/clark1ic/x-003281819/123_02",
                }
            ]
        ),
        harvested_distributions=dist,
    )
    assert len(result["reconciled_primary"]) == 1
    assert result["reconciled_primary"].iloc[0]["ID"] == "old-uuid"
    assert result["reconciled_primary"].iloc[0]["Creator"] == "Enriched creator"
    assert set(result["reconciled_distributions"].friendlier_id) == {"old-uuid"}


def test_identical_titles_different_catalogs_stay_separate_and_default_unchanged(
    tmp_path,
):
    pages = (
        xml_record("003281819", "123_01")
        + xml_record("003281819", "123_02")
        + xml_record("009999999", "777")
    )
    grouped = parse_pages(make_harvester(tmp_path), pages)
    assert len(grouped) == 2
    assert grouped["ID"].nunique() == 2
    ungrouped = parse_pages(make_harvester(tmp_path, grouped=False), pages)
    assert len(ungrouped) == 3


def test_grouping_is_deterministic_and_does_not_discard_conflicting_metadata():
    rows = pd.DataFrame(
        [
            {
                "ID": "page2",
                "Title": "Map",
                "Description": "Note 2",
                "Identifier": "oai:quod.lib.umich.edu:IC-CLARK1IC-X-003281819%5D123_02",
            },
            {
                "ID": "page1",
                "Title": "Map",
                "Description": "Note 1",
                "Identifier": "oai:quod.lib.umich.edu:IC-CLARK1IC-X-003281819%5D123_01",
            },
        ]
    )
    separators = {"Description": "|", "Identifier": "|"}
    a = collapse_umich_catalog_items(rows, "um", separators)
    b = collapse_umich_catalog_items(rows.iloc[::-1], "um", separators)
    pd.testing.assert_frame_equal(a, b)
    assert a.iloc[0]["Description"] == "Note 1|Note 2"
    rows.loc[0, "Title"] = "Different title"
    with pytest.raises(ValueError, match="Conflicting 'Title'"):
        collapse_umich_catalog_items(rows, "um", separators)
    rows.loc[0, "Identifier"] = "https://example.edu/item"
    with pytest.raises(ValueError, match="needs one catalog identity"):
        collapse_umich_catalog_items(rows, "um", separators)


def test_harvester_assigns_persistent_ids_and_updates_membership(tmp_path, monkeypatch):
    harvester = make_harvester(tmp_path)
    harvester.config["local_id_registry"] = str(tmp_path / "local_ids.csv")
    harvester.config["output_distributions_csv"] = (
        "outputs/umich_test_distributions.csv"
    )
    pages = xml_record("003281819", "123_01") + xml_record("003281819", "123_02")
    first = parse_pages(harvester, pages)
    record_id = first.iloc[0].ID
    assert record_id.startswith("um_") and len(record_id) == 15
    assert set(harvester.umich_item_membership.item_harvested_id) == {record_id}
    assert harvester.local_id_assignments.iloc[0].assignment_status == "created"
    monkeypatch.chdir(tmp_path)
    paths = harvester.write_outputs(first)
    assert pd.read_csv(paths["id_assignments_csv"]).iloc[0].ID == record_id
    assert set(pd.read_csv(paths["distributions_csv"]).friendlier_id) == {record_id}
    second = parse_pages(harvester, pages)
    assert second.iloc[0].ID == record_id
    assert harvester.local_id_assignments.iloc[0].assignment_status == "reused"
