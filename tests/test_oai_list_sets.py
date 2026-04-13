from pathlib import Path

from scripts.oai_list_sets import (
    build_output_rows,
    default_output_csv,
    filter_sets_by_terms,
    parse_list_sets_response,
)


def test_parse_list_sets_response_extracts_sets_and_resumption_token() -> None:
    xml_text = """<?xml version="1.0" encoding="UTF-8"?>
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
      <ListSets>
        <set>
          <setSpec>node:8369</setSpec>
          <setName>Hixson Plat Map Atlases of Iowa</setName>
        </set>
        <set>
          <setSpec>node:1234</setSpec>
          <setName>Iowa Plat Book Collection</setName>
        </set>
        <resumptionToken>token-2</resumptionToken>
      </ListSets>
    </OAI-PMH>
    """

    sets, token, errors = parse_list_sets_response(xml_text)

    assert errors == []
    assert token == "token-2"
    assert sets == [
        {
            "set_spec": "node:8369",
            "set_title": "Hixson Plat Map Atlases of Iowa",
            "search_text": "node:8369 hixson plat map atlases of iowa",
        },
        {
            "set_spec": "node:1234",
            "set_title": "Iowa Plat Book Collection",
            "search_text": "node:1234 iowa plat book collection",
        },
    ]


def test_filter_sets_by_terms_is_case_insensitive() -> None:
    sets = [
        {
            "set_spec": "node:8369",
            "set_title": "Hixson Plat Map Atlases of Iowa",
            "search_text": "node:8369 hixson plat map atlases of iowa",
        },
        {
            "set_spec": "node:5000",
            "set_title": "County PLAT BOOK collection",
            "search_text": "node:5000 county plat book collection",
        },
        {
            "set_spec": "node:9000",
            "set_title": "Aerial Photography",
            "search_text": "node:9000 aerial photography",
        },
    ]

    matches = filter_sets_by_terms(sets, ["Atlas", "plat book"])

    assert matches == [
        {
            "set_spec": "node:8369",
            "set_title": "Hixson Plat Map Atlases of Iowa",
            "matched_terms": ["atlas"],
        },
        {
            "set_spec": "node:5000",
            "set_title": "County PLAT BOOK collection",
            "matched_terms": ["plat book"],
        },
    ]


def test_build_output_rows_marks_known_sets_and_sorts() -> None:
    matched_sets = [
        {
            "set_spec": "node:5000",
            "set_title": "County Plat Books",
            "matched_terms": ["plat book"],
        },
        {
            "set_spec": "node:8369",
            "set_title": "Hixson Plat Map Atlases of Iowa",
            "matched_terms": ["atlas"],
        },
    ]

    rows = build_output_rows(
        matched_sets=matched_sets,
        existing_set_specs={"node:8369"},
        set_column="set",
        title_column="title",
        include_known=False,
    )

    assert rows == [
        {
            "set": "node:5000",
            "title": "County Plat Books",
            "match_terms": "plat book",
            "already_listed": "no",
        }
    ]

    all_rows = build_output_rows(
        matched_sets=matched_sets,
        existing_set_specs={"node:8369"},
        set_column="set",
        title_column="title",
        include_known=True,
    )

    assert all_rows == [
        {
            "set": "node:5000",
            "title": "County Plat Books",
            "match_terms": "plat book",
            "already_listed": "no",
        },
        {
            "set": "node:8369",
            "title": "Hixson Plat Map Atlases of Iowa",
            "match_terms": "atlas",
            "already_listed": "yes",
        },
    ]


def test_default_output_csv_uses_existing_csv_stem() -> None:
    existing_csv = Path("/tmp/iowa-sets.csv")

    output_csv = default_output_csv(existing_csv, "iowa-library")

    assert output_csv == Path("/tmp/iowa-sets-discovered.csv")
