import pandas as pd
import pytest

from utils.dataframe_cleaner import deduplicate_rows_and_columns, prioritize_rows_by_column


PRIORITY = ["18c-01", "18b-01", "18d-01", "18a-01"]


@pytest.mark.parametrize("codes,expected", [
    (["18a-01", "18d-01", "18b-01", "18c-01"], "18c-01"),
    (["18a-01", "18d-01", "18b-01"], "18b-01"),
    (["18a-01", "18d-01"], "18d-01"),
    (["18f-01", "18a-01"], "18a-01"),
    (["18f-01", "other"], "18f-01"),
])
def test_duplicate_id_retains_preferred_series_and_its_metadata(codes, expected):
    df = pd.DataFrame([
        {"ID": "shared", "Code": code, "Title": code, "download": f"https://example.org/{code}"}
        for code in codes
    ])
    result = df.pipe(prioritize_rows_by_column, "Code", PRIORITY).pipe(deduplicate_rows_and_columns)
    assert result.to_dict("records") == [{
        "ID": "shared", "Code": expected, "Title": expected,
        "download": f"https://example.org/{expected}",
    }]


def test_priority_preserves_ties_unique_records_and_input_dataframe():
    df = pd.DataFrame([
        {"ID": "shared", "Code": "18c-01", "Title": "first"},
        {"ID": "shared", "Code": "18c-01", "Title": "second"},
        {"ID": "unique", "Code": "other", "Title": "unique"},
    ], index=[7, 7, 4])
    original = df.copy(deep=True)
    result = df.pipe(prioritize_rows_by_column, "Code", PRIORITY).pipe(deduplicate_rows_and_columns)
    assert result["Title"].tolist() == ["first", "unique"]
    pd.testing.assert_frame_equal(df, original)
    pd.testing.assert_frame_equal(prioritize_rows_by_column(df, "Code", []), df)
    pd.testing.assert_frame_equal(prioritize_rows_by_column(df, "absent", PRIORITY), df)
