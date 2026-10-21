import pandas as pd

from utils.spatial_match import derive_county_spatial_from_titles


def test_derive_county_spatial_from_titles_handles_multiword_names() -> None:
    county_lookup = {
        "Minnesota--Lac qui Parle County": {},
        "Minnesota--St. Louis County": {},
        "Minnesota--Lake County": {},
    }
    source = pd.DataFrame(
        [
            {
                "Title": "C-65, Geologic Atlas of Lac qui Parle County, Minnesota",
                "Spatial Coverage": "",
            },
            {
                "Title": "C-51, Geologic Atlas of St. Louis County, Minnesota",
                "Spatial Coverage": "",
            },
            {
                "Title": "Minnesota County Atlas Index Map for 2010",
                "Spatial Coverage": "",
            },
            {
                "Title": "Lake County map",
                "Spatial Coverage": "Curated coverage",
            },
        ]
    )

    result = derive_county_spatial_from_titles(source, county_lookup)

    assert result["Spatial Coverage"].tolist() == [
        "Minnesota--Lac qui Parle County",
        "Minnesota--St. Louis County",
        "",
        "Curated coverage",
    ]
