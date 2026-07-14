from pathlib import Path

import pandas as pd

from harvesters.standalone_websites import StandaloneWebsiteLinkChecker
from routers.jobs import HARVESTER_REGISTRY


class FakeResponse:
    def __init__(self, status_code: int, url: str):
        self.status_code = status_code
        self.url = url
        self.closed = False

    def close(self) -> None:
        self.closed = True


class FakeSession:
    def __init__(self):
        self.headers = {}
        self.requested_urls: list[str] = []

    def get(self, url: str, **kwargs) -> FakeResponse:
        del kwargs
        self.requested_urls.append(url)
        responses = {
            "https://active.example.org": FakeResponse(200, "https://active.example.org/final"),
            "https://cloudflare.example.org": FakeResponse(403, "https://cloudflare.example.org"),
            "https://inactive.example.org": FakeResponse(404, "https://inactive.example.org"),
        }
        return responses[url]


def test_standalone_website_link_checker_checks_public_urls_and_skips_restricted(
    tmp_path: Path,
    monkeypatch,
) -> None:
    websites_path = tmp_path / "websites.csv"
    output_path = tmp_path / "outputs" / "standalone-websites_primary.csv"
    harvest_records_path = tmp_path / "harvest-records.csv"
    websites_df = pd.DataFrame(
        [
            {
                "ID": "public-site",
                "Code": "w00_01",
                "Title": "Public site",
                "Access Rights": "Public",
                "Identifier": "https://active.example.org",
                "Date Accessioned": "2020-01-01",
                "Publication State": "published",
                "Provenance": "",
                "Admin Note": "",
            },
            {
                "ID": "cloudflare-site",
                "Code": "w00_01",
                "Title": "Cloudflare site",
                "Access Rights": "Public",
                "Identifier": "https://cloudflare.example.org",
                "Date Accessioned": "2020-01-01",
                "Publication State": "published",
                "Provenance": "",
                "Admin Note": "",
            },
            {
                "ID": "inactive-site",
                "Code": "w00_01",
                "Title": "Inactive site",
                "Access Rights": "Public",
                "Identifier": "https://inactive.example.org",
                "Date Accessioned": "2020-01-01",
                "Publication State": "published",
                "Provenance": "",
                "Admin Note": "Existing note",
            },
            {
                "ID": "restricted-site",
                "Code": "w00_01",
                "Title": "Restricted site",
                "Access Rights": "Restricted",
                "Identifier": "",
                "Date Accessioned": "2020-01-01",
                "Publication State": "published",
                "Provenance": "",
                "Admin Note": "",
            },
            {
                "ID": "harvested-site",
                "Code": "01a-01",
                "Title": "Harvested site",
                "Access Rights": "Public",
                "Identifier": "https://not-standalone.example.org",
                "Date Accessioned": "2020-01-01",
                "Publication State": "published",
                "Provenance": "",
                "Admin Note": "",
            },
        ]
    )
    websites_df["Last Harvested"] = ""
    websites_df.to_csv(websites_path, index=False)
    pd.DataFrame(
        [
            {
                "ID": "harvest_standalone_websites",
                "Code": "w00_01",
                "Title": "Harvest record for standalone websites",
                "Access Rights": "Public",
                "Identifier": "",
                "Date Accessioned": "",
                "Publication State": "draft",
                "Provenance": "2026-01-01 / review / earlier review",
                "Admin Note": "",
                "Last Harvested": "2026-01-01",
            }
        ]
    ).to_csv(harvest_records_path, index=False)
    session = FakeSession()
    monkeypatch.setattr("harvesters.standalone_websites.requests.Session", lambda: session)

    results = StandaloneWebsiteLinkChecker(
        {
            "websites_csv": str(websites_path),
            "harvest_records_csv": str(harvest_records_path),
            "output_primary_csv": str(output_path),
            "today": "2026-07-13",
        }
    ).harvest_pipeline()

    output_df = pd.read_csv(results["primary_csv"], dtype=str).fillna("")
    assert session.requested_urls == [
        "https://active.example.org",
        "https://cloudflare.example.org",
        "https://inactive.example.org",
    ]
    assert output_df.columns.tolist() == pd.read_csv(websites_path, nrows=0).columns.tolist()
    assert output_df["ID"].tolist() == [
        "public-site",
        "cloudflare-site",
        "inactive-site",
        "restricted-site",
        "harvest_standalone_websites",
    ]
    assert "Link Status" not in output_df.columns
    assert output_df.loc[output_df["ID"].eq("public-site"), "Date Accessioned"].item() == "2026-07-13"
    assert output_df.loc[output_df["ID"].eq("cloudflare-site"), "Date Accessioned"].item() == "2026-07-13"
    inactive_row = output_df.loc[output_df["ID"].eq("inactive-site")].iloc[0]
    assert inactive_row["Publication State"] == "draft"
    assert inactive_row["Admin Note"] == "Existing note|2026-07-13 / review / link inactive (HTTP 404)"
    assert output_df.loc[output_df["ID"].eq("restricted-site"), "Date Accessioned"].item() == "2020-01-01"
    harvest_record = output_df.iloc[-1]
    assert harvest_record["Last Harvested"] == "2026-07-13"
    assert harvest_record["Provenance"] == (
        "2026-01-01 / review / earlier review|2026-07-13 / review / 1 sites inactive"
    )
    assert results["checked_count"] == 3
    assert results["active_count"] == 2
    assert results["inactive_count"] == 1
    assert results["skipped_count"] == 1
    assert not output_path.exists()


def test_standalone_website_link_checker_is_registered_as_a_job_type() -> None:
    assert HARVESTER_REGISTRY["standalone_websites"] is StandaloneWebsiteLinkChecker


def test_standalone_website_static_page_runs_the_job() -> None:
    html = Path("static/standalone-websites.html").read_text(encoding="utf-8")

    assert "Restricted records are recorded as skipped" in html
    assert 'new EventSource("/run-standalone-websites-stream")' in html
