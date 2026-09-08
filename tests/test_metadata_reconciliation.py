from pathlib import Path

import pandas as pd
import pytest

from utils.metadata_reconciliation import deleted_keys, reconcile, umich_keys


def primary(*items):
    return pd.DataFrame(
        [
            {
                "ID": record_id,
                "Identifier": identifier,
                "Title": "Map",
                "Creator": "Source creator",
                "Subject": "Source subject",
                "Date Accessioned": "2026-09-06",
            }
            for record_id, identifier in items
        ]
    )


def identifier(image="39015091193402", catalog="003281819"):
    return f"oai:quod.lib.umich.edu:IC-CLARK1IC-X-{catalog}%5D{image}"


def existing(*items):
    return pd.DataFrame(
        [
            {"friendlier_id": record_id, "distribution_url": url}
            for record_id, url in items
        ],
        columns=["friendlier_id", "distribution_url"],
    )


def enriched(*ids):
    return pd.DataFrame(
        [
            {
                "ID": record_id,
                "Creator": "Curated|Creator",
                "Subject": "",
                "Date Accessioned": "2019-01-16",
            }
            for record_id in ids
        ],
        columns=["ID", "Creator", "Subject", "Date Accessioned"],
    )


def landing(image="39015091193402", catalog="003281819"):
    return f"https://quod.lib.umich.edu/c/clark1ic/x-{catalog}/{image}"


def test_sparse_overlay_stable_ids_new_rows_and_distributions():
    source = primary(("new-id", identifier()), ("new-map", identifier("999")))
    result = reconcile(
        source,
        enriched("old-id"),
        existing(("old-id", landing())),
        harvested_distributions=existing(
            ("new-id", landing()), ("new-map", landing("999"))
        ),
    )
    rows = result["reconciled_primary"].to_dict("records")
    assert [r["ID"] for r in rows] == ["old-id", "new-map"]
    assert rows[0]["Creator"] == "Curated|Creator"
    assert rows[0]["Subject"] == "Source subject"
    assert rows[0]["Date Accessioned"] == "2019-01-16"
    # Same catalog, different sheet/image: a candidate, never a confident new item.
    assert result["crosswalk"]["match_status"].tolist() == ["matched", "needs_review"]
    assert result["reconciled_distributions"]["friendlier_id"].tolist() == [
        "old-id",
        "new-map",
    ]
    assert source.iloc[0]["Creator"] == "Source creator"
    assert len(result["enrichment_audit"]) == 2


def test_changed_catalog_still_matches_exact_image():
    result = reconcile(
        primary(("new", identifier(catalog="123"))),
        enriched("old"),
        existing(("old", landing())),
    )
    assert result["crosswalk"].iloc[0]["match_method"] == "image"
    assert result["reconciled_primary"].iloc[0]["ID"] == "old"


def test_duplicate_old_or_new_image_requires_review():
    result = reconcile(
        primary(("new", identifier())),
        enriched("old1", "old2"),
        existing(("old1", landing()), ("old2", landing())),
    )
    assert result["crosswalk"].iloc[0]["match_status"] == "needs_review"
    assert result["enrichment_audit"].empty
    result = reconcile(
        primary(("new1", identifier()), ("new2", identifier())),
        enriched("old"),
        existing(("old", landing())),
    )
    assert result["crosswalk"]["match_status"].tolist() == [
        "needs_review",
        "needs_review",
    ]


def test_conflicting_id_and_image_requires_review():
    result = reconcile(
        primary(("old1", identifier())),
        enriched("old1", "old2"),
        existing(("old2", landing())),
    )
    assert result["crosswalk"].iloc[0]["match_status"] == "needs_review"


def test_saved_aliases_recognize_previously_new_items():
    first = reconcile(primary(("new", identifier())), enriched(), existing())
    assert len(first["new_items"]) == 1
    second = reconcile(
        primary(("changed", identifier())),
        enriched(),
        existing(),
        aliases=first["identity_aliases"],
    )
    assert second["crosswalk"].iloc[0]["match_status"] == "matched"
    assert second["reconciled_primary"].iloc[0]["ID"] == "new"


def test_keys_preserve_sheet_suffix_and_scope():
    assert "image:clark1ic:123_01" in umich_keys(landing("123_01"))
    assert "image:clark1ic:123" not in umich_keys(landing("123_01"))
    assert not umich_keys("https://other.example/c/clark1ic/x-003281819/123")
    assert umich_keys(landing()) == umich_keys(identifier())
    assert umich_keys(
        "http://name.umdl.umich.edu/IC-CLARK1IC-X-3281819%5D39015091193402"
    ) == umich_keys(identifier())
    assert umich_keys(
        "https://quod.lib.umich.edu/cgi/i/image/api/search/clark1ic:003281819"
    ) == {"catalog:clark1ic:3281819"}


def test_deleted_and_missing_records_are_retained(tmp_path: Path):
    (
        tmp_path / "page.xml"
    ).write_text(f"""<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"><ListRecords>
      <record><header status="deleted"><identifier>{identifier()}</identifier><datestamp>2026-08-20</datestamp></header></record>
      </ListRecords></OAI-PMH>""")
    result = reconcile(
        primary(("new", identifier("999", "999"))),
        enriched("old", "collection"),
        existing(("old", landing())),
        deletions=deleted_keys(tmp_path),
    )
    missing = result["missing_existing"].set_index("ID")
    assert missing.loc["old", "reconciliation_status"] == "source_deleted"
    assert missing.loc["old", "Creator"] == "Curated|Creator"
    assert missing.loc["collection", "reconciliation_status"] == "not_in_harvest"
    assert len(result["new_items"]) == 1
    # An active match takes precedence over an older tombstone.
    active = reconcile(
        primary(("new", identifier())),
        enriched("old"),
        existing(("old", landing())),
        deletions=deleted_keys(tmp_path),
    )
    assert active["missing_existing"].empty


def test_bad_inputs_fail_before_output():
    with pytest.raises(ValueError, match="unique"):
        reconcile(
            primary(("same", identifier()), ("same", identifier())),
            enriched(),
            existing(),
        )
    with pytest.raises(ValueError, match="missing from harvested schema"):
        reconcile(
            primary(("new", identifier())),
            pd.DataFrame([{"ID": "old", "Unexpected": "edit"}]),
            existing(),
        )
    with pytest.raises(ValueError, match="absent from primary"):
        reconcile(
            primary(("new", identifier())),
            enriched(),
            existing(),
            harvested_distributions=existing(("orphan", landing())),
        )


def test_review_can_accept_related_image_as_new_and_retain_identity():
    source = primary(("new", identifier("999")))
    old = existing(("old", landing()))
    decision = pd.DataFrame(
        [{"harvested_id": "new", "action": "accept_new", "canonical_id": ""}]
    )
    result = reconcile(source, enriched("old"), old, decisions=decision)
    assert len(result["new_items"]) == 1
    assert result["enrichment_audit"].empty
    assert result["crosswalk"].iloc[0]["match_method"] == "manual_accept_new"
    again = reconcile(
        primary(("renamed", identifier("999"))),
        enriched("old"),
        old,
        aliases=result["identity_aliases"],
    )
    assert again["reconciled_primary"].iloc[0]["ID"] == "new"


def test_manual_match_requires_one_to_one_known_identity():
    decision = pd.DataFrame(
        [{"harvested_id": "new", "action": "match", "canonical_id": "old"}]
    )
    old = existing(("old", landing()))
    result = reconcile(
        primary(("new", identifier("999", "999"))),
        enriched("old"),
        old,
        decisions=decision,
    )
    assert result["reconciled_primary"].iloc[0]["ID"] == "old"
    assert result["crosswalk"].iloc[0]["match_method"] == "manual_match"
    with pytest.raises(ValueError, match="one-to-one"):
        reconcile(
            primary(("auto", identifier()), ("new", identifier("999"))),
            enriched("old"),
            old,
            decisions=decision,
        )
    decision.loc[0, "canonical_id"] = "unknown"
    with pytest.raises(ValueError, match="Decisions require"):
        reconcile(
            primary(("new", identifier())), enriched("old"), old, decisions=decision
        )
