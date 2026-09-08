import pytest

from harvesters.oai_qdc import OaiQdcHarvester
from utils.creator_fields import split_creator_names_and_ids


@pytest.mark.parametrize(
    ("values", "names", "identifiers"),
    [
        (["Doe, Jane"], ["Doe, Jane"], []),
        (
            ["http://id.example/1", "https://id.example/2"],
            [],
            ["http://id.example/1", "https://id.example/2"],
        ),
        (
            [
                "Wilkes, Charles, 1798-1877. http://id.loc.gov/authorities/names/n50017314 http://viaf.org/viaf/2573964; National Ocean Survey. https://id.example/2"
            ],
            ["Wilkes, Charles, 1798-1877.", "National Ocean Survey."],
            [
                "http://id.loc.gov/authorities/names/n50017314",
                "http://viaf.org/viaf/2573964",
                "https://id.example/2",
            ],
        ),
        (
            ["Doe http://id.example/1;Roe https://id.example/2"],
            ["Doe", "Roe"],
            ["http://id.example/1", "https://id.example/2"],
        ),
        (
            ["Doe|https://id.example/1", "Doe", "https://id.example/1"],
            ["Doe"],
            ["https://id.example/1"],
        ),
        (["HTTP://id.example/1"], [], ["HTTP://id.example/1"]),
        ([], [], []),
    ],
)
def test_split_creator_names_and_ids(values, names, identifiers):
    assert split_creator_names_and_ids(values) == (names, identifiers)


def test_dc_crosswalk_routes_creator_urls():
    harvester = OaiQdcHarvester(
        {"name": "test", "oai_base_url": "https://example.edu/oai"}
    )
    frame = harvester.build_dataframe(
        [
            {
                "oai_identifier": "oai:example.edu:maps-1",
                "set_spec": "maps",
                "fields": {
                    "dc:title": ["Map"],
                    "dc:creator": ["Doe, Jane http://id.example/1; Roe, John"],
                    "dcterms:creator": ["https://id.example/2"],
                },
            }
        ]
    )
    assert frame.iloc[0]["Creator"] == "Doe, Jane|Roe, John"
    assert frame.iloc[0]["Creator ID"] == "http://id.example/1|https://id.example/2"
