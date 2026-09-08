import pytest

from harvesters.oai_qdc import OaiQdcHarvester
from utils.marc_coordinates import extract_scale_statements


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        (
            [
                "a 48000 W0741233 W0735341 N0405401 N0403225",
                "Scale approximately 1:48,000 (W 74°12'33\"--W 73°53'41\"/N 40°54'01\"--N 40°32'25\").",
            ],
            ["Scale approximately 1:48,000"],
        ),
        (["Scale [ca. 1:1,000] (W 90°--W 80°/N 45°--N 40°)."], ["Scale [ca. 1:1,000]"]),
        (["Scale not given (W 90°--W 80°/N 45°--N 40°)."], ["Scale not given"]),
        (["Not drawn to scale (W86°27'/N43°57')."], ["Not drawn to scale"]),
        (["Scales differ."], ["Scales differ"]),
        (
            ["Scale 1:15,840. Vertical scale 1:480 (W 90°--W 80°/N 45°--N 40°)."],
            ["Scale 1:15,840. Vertical scale 1:480"],
        ),
        (
            [
                "(E 27°--E 61°/N 32°--N 10°). Scale: approximately 1:5,702,400 90 mi. = 1 in."
            ],
            ["Scale: approximately 1:5,702,400 90 mi. = 1 in"],
        ),
        (["a 48000 W0741233 W0735341 N0405401 N0403225"], ["1:48,000"]),
        (["a15000"], ["1:15,000"]),
        (["57000 W0874951 W0872414 N0415811 N0413844"], ["1:57,000"]),
        (["a W0741233 W0735341 N0405401 N0403225"], []),
        (["(W 90°--W 80°/N 45°--N 40°).", "From an atlas."], []),
        (["Scale 1:50,000", "Scale 1:50,000"], ["Scale 1:50,000"]),
        ([], []),
    ],
)
def test_scale_statements(values, expected):
    assert extract_scale_statements(values) == expected


def test_dc_crosswalk_extracts_scale_from_source_without_losing_coordinates():
    harvester = OaiQdcHarvester(
        {"name": "test", "oai_base_url": "https://example.edu/oai"}
    )
    sources = [
        "a 48000 W0741233 W0735341 N0405401 N0403225",
        "Scale approximately 1:48,000 (W 74°--W 73°/N 40°--N 39°).",
    ]
    record = {
        "oai_identifier": "oai:example.edu:maps-1",
        "set_spec": "maps",
        "fields": {"dc:title": ["Map"], "dc:source": sources},
    }
    row = harvester.build_dataframe([record]).iloc[0]
    assert row["Spatial Resolution as Text"] == "Scale approximately 1:48,000"
    assert row["Source"] == "|".join(sources)
    record["fields"] = {"dc:title": ["Map"], "dcterms:source": sources}
    assert (
        harvester.build_dataframe([record]).iloc[0]["Spatial Resolution as Text"]
        == "Scale approximately 1:48,000"
    )


@pytest.mark.parametrize(
    "value",
    [
        "Scale approximately 1:48,000 W 74°12'33\"--W 73°53'41\"/N 40°54'01\"--N 40°32'25\"",
        "Scale approximately 1:48,000 (W 74°--W 73°/N 40°--N 39°]",
    ],
)
def test_scale_statements_strip_unparenthesized_or_malformed_coordinates(value):
    assert extract_scale_statements([value]) == ["Scale approximately 1:48,000"]
