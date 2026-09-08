import pytest

from utils.temporal_fields import extract_issued_year
from harvesters.oai_qdc import OaiQdcHarvester


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        (["Paris : Publisher, 1887."], "1887"),
        (["[1636?]"], "1636"),
        (["c1850"], "1850"),
        (["1542"], "1542"),
        (["2026-09-07T10:20:00Z"], "2026"),
        (["1887", "1887-01-01"], "1887"),
        (["[between 1710 and 1730]"], ""),
        (["1845 [1849?]"], ""),
        (["1869, c1868"], ""),
        (["185-?"], ""),
        (["n.d."], ""),
        (["unknown"], ""),
        (["39015091889678"], ""),
        ([], ""),
    ],
)
def test_extract_issued_year(values, expected):
    assert extract_issued_year(values) == expected


def test_dc_crosswalk_uses_year_without_changing_temporal_range():
    harvester = OaiQdcHarvester(
        {"name": "test", "oai_base_url": "https://example.edu/oai"}
    )
    records = [
        {
            "oai_identifier": "oai:example.edu:maps-1",
            "set_spec": "maps",
            "fields": {
                "dc:title": ["A map"],
                "dc:date": ["London : Publisher, [1846-1851]"],
            },
        }
    ]
    df = harvester.build_dataframe(records)
    assert df.iloc[0]["Date Issued"] == ""
    assert df.iloc[0]["Temporal Coverage"] == "1846-1851"
    assert df.iloc[0]["Date Range"] == "1846-1851"
    records[0]["fields"]["dc:date"] = ["London : Publisher, [1846?]"]
    assert harvester.build_dataframe(records).iloc[0]["Date Issued"] == "1846"
    records[0]["fields"]["dcterms:issued"] = ["Published 1852-03-10"]
    assert harvester.build_dataframe(records).iloc[0]["Date Issued"] == "1852"
