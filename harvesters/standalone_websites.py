from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from harvesters.base import BaseHarvester


class StandaloneWebsiteLinkChecker(BaseHarvester):
    def __init__(self, config: dict[str, Any]):
        config = dict(config)
        config.setdefault("websites_csv", "reference_data/websites.csv")
        config.setdefault(
            "harvest_records_csv",
            "inputs/harvest-records.csv",
        )
        config.setdefault("output_primary_csv", "outputs/standalone-websites_primary.csv")
        config.setdefault("standalone_website_code", "w00_01")
        config.setdefault("timeout", 20)
        config.setdefault("user_agent", "harvest-operations standalone website link checker")
        super().__init__(config)
        self.websites_path = Path(self.config["websites_csv"])
        self.harvest_records_path = Path(self.config["harvest_records_csv"])
        self.primary_output_path = Path(self.config["output_primary_csv"])
        self.standalone_website_code = str(self.config["standalone_website_code"])
        self.timeout = int(self.config["timeout"])
        self.today = str(self.config.get("today") or datetime.now().strftime("%Y-%m-%d"))
        self.websites_df = pd.DataFrame()
        self.harvest_records_df = pd.DataFrame()

    def load_reference_data(self):
        self.websites_df = pd.read_csv(self.websites_path, dtype=str).fillna("")
        self.websites_df.columns = [str(column).strip() for column in self.websites_df.columns]
        self.harvest_records_df = pd.read_csv(
            self.harvest_records_path,
            dtype=str,
        ).fillna("")
        self.harvest_records_df.columns = [
            str(column).strip() for column in self.harvest_records_df.columns
        ]

    def fetch(self):
        candidates = standalone_website_link_candidates(
            self.websites_df,
            standalone_website_code=self.standalone_website_code,
        )
        session = requests.Session()
        session.headers.update({"User-Agent": self.config["user_agent"]})

        results = []
        for position, candidate in enumerate(candidates, start=1):
            print(
                "[Standalone websites] "
                f"Checking {position}/{len(candidates)}: {candidate['Title'] or candidate['ID']}."
            )
            result = check_standalone_website_link(
                candidate,
                session=session,
                timeout=self.timeout,
            )
            print(standalone_website_link_check_log(result))
            results.append(result)
        return results

    def flatten(self, harvested_metadata):
        return harvested_metadata

    def build_dataframe(self, flattened_data):
        return pd.DataFrame(flattened_data, columns=LINK_CHECK_RESULT_COLUMNS)

    def write_outputs(self, link_check_df: pd.DataFrame, distributions_df=None) -> dict:
        del distributions_df
        standalone_websites_df = self.websites_df.loc[
            link_check_df["Source Index"].astype(int)
        ].copy()
        output_df = apply_standalone_website_link_check_results(
            standalone_websites_df,
            link_check_df,
            today=self.today,
        )
        harvest_record = updated_standalone_website_harvest_record(
            self.harvest_records_df,
            column_order=list(output_df.columns),
            today=self.today,
            inactive_count=int(link_check_df["Link Status"].eq("Inactive").sum()),
        )
        output_df = output_df.loc[
            output_df["ID"].astype(str).str.strip().ne("harvest_standalone_websites")
        ]
        output_df = pd.concat([output_df, harvest_record], ignore_index=True)

        self.primary_output_path.parent.mkdir(parents=True, exist_ok=True)
        dated_primary_path = self.primary_output_path.with_name(
            f"{self.today}_{self.primary_output_path.name}"
        )
        output_df.to_csv(dated_primary_path, index=False, encoding="utf-8")

        return {
            "primary_csv": str(dated_primary_path),
            "checked_count": int(link_check_df["Link Status"].isin(["Active", "Inactive"]).sum()),
            "active_count": int(link_check_df["Link Status"].eq("Active").sum()),
            "inactive_count": int(link_check_df["Link Status"].eq("Inactive").sum()),
            "skipped_count": int(link_check_df["Link Status"].eq("Skipped").sum()),
        }

    def harvest_pipeline(self):
        self.load_reference_data()
        raw = self.fetch()
        parsed = self.parse(raw)
        flattened = self.flatten(parsed)
        link_check_df = self.build_dataframe(flattened)
        results = self.write_outputs(link_check_df)
        print(f"[Standalone websites] Link check complete: {results}")
        return results


# Custom functions for this harvester


LINK_CHECK_RESULT_COLUMNS = [
    "Source Index",
    "ID",
    "Code",
    "Title",
    "Access Rights",
    "Identifier",
    "Link Status",
    "HTTP Status",
    "Final URL",
    "Checked At",
    "Error",
]


def standalone_website_link_candidates(
    websites_df: pd.DataFrame,
    standalone_website_code: str,
) -> list[dict[str, str]]:
    working_df = websites_df.copy()
    for column in ("ID", "Code", "Title", "Access Rights", "Identifier"):
        if column not in working_df.columns:
            working_df[column] = ""

    standalone_rows = working_df.loc[
        working_df["Code"].astype(str).str.strip().eq(standalone_website_code)
    ].copy()
    standalone_rows["Source Index"] = standalone_rows.index
    return standalone_rows.to_dict(orient="records")


def check_standalone_website_link(
    candidate: dict[str, str],
    session: requests.Session,
    timeout: int,
) -> dict[str, str]:
    result = {
        "Source Index": str(candidate["Source Index"]),
        "ID": clean_link_check_value(candidate.get("ID", "")),
        "Code": clean_link_check_value(candidate.get("Code", "")),
        "Title": clean_link_check_value(candidate.get("Title", "")),
        "Access Rights": clean_link_check_value(candidate.get("Access Rights", "")),
        "Identifier": clean_link_check_value(candidate.get("Identifier", "")),
        "Link Status": "",
        "HTTP Status": "",
        "Final URL": "",
        "Checked At": datetime.now(timezone.utc).isoformat(),
        "Error": "",
    }
    if result["Access Rights"].casefold() == "restricted":
        result["Link Status"] = "Skipped"
        result["Error"] = "Restricted access; not checked."
        return result
    if not result["Identifier"]:
        result["Link Status"] = "Skipped"
        result["Error"] = "No Identifier URL to check."
        return result

    try:
        response = session.get(
            result["Identifier"],
            allow_redirects=True,
            stream=True,
            timeout=timeout,
        )
        try:
            result["HTTP Status"] = str(response.status_code)
            result["Final URL"] = str(response.url)
            result["Link Status"] = "Active" if is_active_link_status(response.status_code) else "Inactive"
        finally:
            response.close()
    except requests.RequestException as exc:
        result["Link Status"] = "Inactive"
        result["Error"] = str(exc)
    return result


def clean_link_check_value(value: Any) -> str:
    return "" if value is None else str(value).strip()


def is_active_link_status(status_code: int) -> bool:
    return 200 <= status_code < 400 or status_code == 403


def standalone_website_link_check_log(result: dict[str, str]) -> str:
    label = result["Title"] or result["ID"]
    if result["Link Status"] == "Active":
        return f"[Standalone websites] Active: {label} ({result['HTTP Status']})."
    if result["Link Status"] == "Skipped":
        return f"[Standalone websites] Skipped: {label}. {result['Error']}"
    detail = result["HTTP Status"] or result["Error"]
    return f"[Standalone websites] Inactive: {label}. {detail}"


def apply_standalone_website_link_check_results(
    websites_df: pd.DataFrame,
    link_check_df: pd.DataFrame,
    today: str,
) -> pd.DataFrame:
    output_df = websites_df.copy()
    for column in ("Date Accessioned", "Publication State", "Admin Note"):
        if column not in output_df.columns:
            output_df[column] = ""

    for _, result in link_check_df.iterrows():
        source_index = int(result["Source Index"])
        if result["Link Status"] == "Active":
            output_df.at[source_index, "Date Accessioned"] = today
        elif result["Link Status"] == "Inactive":
            output_df.at[source_index, "Publication State"] = "draft"
            output_df.at[source_index, "Admin Note"] = append_pipe_value(
                output_df.at[source_index, "Admin Note"],
                standalone_website_inactive_admin_note(result, today),
            )
    return output_df


def updated_standalone_website_harvest_record(
    harvest_records_df: pd.DataFrame,
    column_order: list[str],
    today: str,
    inactive_count: int,
) -> pd.DataFrame:
    harvest_record = harvest_records_df.loc[
        harvest_records_df["ID"].astype(str).str.strip().eq("harvest_standalone_websites")
    ].copy()
    if harvest_record.empty:
        raise ValueError("Could not find harvest_standalone_websites in harvest-records.csv.")

    harvest_record = harvest_record.iloc[[0]].reindex(columns=column_order, fill_value="")
    harvest_record["Last Harvested"] = today
    harvest_record["Provenance"] = append_pipe_value(
        harvest_record.iloc[0]["Provenance"],
        f"{today} / review / {inactive_count} sites inactive",
    )
    return harvest_record


def standalone_website_inactive_admin_note(result: pd.Series, today: str) -> str:
    if result["HTTP Status"]:
        return f"{today} / review / link inactive (HTTP {result['HTTP Status']})"
    return f"{today} / review / link inactive ({result['Error']})"


def append_pipe_value(existing_value: Any, value_to_append: str) -> str:
    existing = clean_link_check_value(existing_value)
    return f"{existing}|{value_to_append}" if existing else value_to_append
