import pandas as pd

from utils.derive_themes import derive_themes_from_keywords


def test_derive_themes_handles_unicode_casefold_equivalents():
    df = pd.DataFrame(
        [
            {
                "Title": "Map of the coaſt",
                "Keyword": "",
                "Subject": "",
            }
        ]
    )

    result = derive_themes_from_keywords(df, {"coast": "Oceans"})

    assert result.loc[0, "Theme"] == "Oceans"
