import json

import pytest

from utils.dspace_bitstreams import (
    DspaceBitstreamError,
    enrich_records_with_dspace_bitstreams,
    load_dspace_bitstream_inventory,
)


def test_dspace_inventory_joins_downloads_by_oai_handle(tmp_path) -> None:
    inventory_path = tmp_path / "bitstreams.json"
    inventory_path.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "handle": "11299/173781",
                        "thumbnail_url": "https://example.edu/bitstreams/thumb/content",
                        "files": [
                            {
                                "name": "map.pdf",
                                "description": "Surficial geology map",
                                "download_url": "https://example.edu/bitstreams/1/download",
                            },
                            {
                                "name": "readme.txt",
                                "description": "",
                                "download_url": "https://example.edu/bitstreams/2/download",
                            },
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    inventory = load_dspace_bitstream_inventory(inventory_path)

    enriched = enrich_records_with_dspace_bitstreams(
        [{"oai_identifier": "oai:conservancy.umn.edu:11299/173781"}],
        inventory,
    )

    assert enriched[0]["information"] == "https://hdl.handle.net/11299/173781"
    assert enriched[0]["thumbnail"] == (
        "https://example.edu/bitstreams/thumb/content"
    )
    assert enriched[0]["download"] == [
        {
            "url": "https://example.edu/bitstreams/1/download",
            "label": "Surficial geology map",
        },
        {
            "url": "https://example.edu/bitstreams/2/download",
            "label": "readme.txt",
        },
    ]


def test_dspace_inventory_allows_an_item_without_a_thumbnail(tmp_path) -> None:
    inventory_path = tmp_path / "bitstreams.json"
    inventory_path.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "handle": "11299/173781",
                        "thumbnail_url": "",
                        "files": [],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    enriched = enrich_records_with_dspace_bitstreams(
        [{"oai_identifier": "oai:conservancy.umn.edu:11299/173781"}],
        load_dspace_bitstream_inventory(inventory_path),
    )

    assert enriched[0]["thumbnail"] == ""


def test_dspace_inventory_fails_when_an_oai_record_is_missing() -> None:
    with pytest.raises(DspaceBitstreamError, match="missing 1 OAI record"):
        enrich_records_with_dspace_bitstreams(
            [{"oai_identifier": "oai:conservancy.umn.edu:11299/173781"}],
            {},
        )
