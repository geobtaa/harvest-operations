#!/usr/bin/env python3
"""
Crosswalk legacy Social Science records to the 2026 Chicago LUNA export.

Outputs:
- a crosswalk CSV describing each legacy-to-new match
- a copy of the legacy Social Science CSV with the matched new IDs appended
- a filtered Chicago LUNA CSV with the Social Science rows removed
"""

from __future__ import annotations

import argparse
import csv
import re
import unicodedata
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

DEFAULT_LEGACY_CSV = PROJECT_ROOT / "outputs" / "socialScience.csv"
DEFAULT_NEW_CSV = PROJECT_ROOT / "outputs" / "2026-03-27_chicago_luna_primary_final.csv"
DEFAULT_CROSSWALK_CSV = PROJECT_ROOT / "outputs" / "2026-03-27_chicago_luna_social_science_crosswalk.csv"
DEFAULT_ENRICHED_LEGACY_CSV = (
    PROJECT_ROOT / "outputs" / "2026-03-27_socialScience_with_new_ids.csv"
)
DEFAULT_FILTERED_NEW_CSV = (
    PROJECT_ROOT / "outputs" / "2026-03-27_chicago_luna_primary_final_without_social_science.csv"
)

THEMATIC_FILE_RE = re.compile(r"(G4104-C6-1933-U5-[a-z])\.(?:jpg|tif)", re.IGNORECASE)
THEMATIC_CODE_RE = re.compile(r"(G4104-C6-1933-U5-[a-z])(?:\b|[./])", re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--legacy-csv", type=Path, default=DEFAULT_LEGACY_CSV)
    parser.add_argument("--new-csv", type=Path, default=DEFAULT_NEW_CSV)
    parser.add_argument("--crosswalk-csv", type=Path, default=DEFAULT_CROSSWALK_CSV)
    parser.add_argument("--enriched-legacy-csv", type=Path, default=DEFAULT_ENRICHED_LEGACY_CSV)
    parser.add_argument("--filtered-new-csv", type=Path, default=DEFAULT_FILTERED_NEW_CSV)
    return parser.parse_args()


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def write_rows(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def skeletonize_title(value: str) -> str:
    text = unicodedata.normalize("NFKC", str(value or "").lower()).strip()
    text = text.replace("&", " and ")
    text = re.sub(r"^\[(.*)\]\.?$", r"\1", text)
    text = text.replace("|", " ")
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def extract_identifier_titles(identifier_text: str) -> list[str]:
    titles = []
    for part in str(identifier_text or "").split("|"):
        clean_part = part.strip()
        if not clean_part:
            continue
        title_only = re.sub(r"\s+https?://\S+$", "", clean_part).strip(" .")
        if title_only:
            titles.append(title_only)
    return titles


def extract_thematic_code_from_legacy(row: dict[str, str]) -> str:
    match = THEMATIC_FILE_RE.search(row.get("B1G Image", ""))
    return match.group(1).lower() if match else ""


def extract_thematic_codes_from_identifier(identifier_text: str) -> list[str]:
    codes = []
    for part in str(identifier_text or "").split("|"):
        match = THEMATIC_CODE_RE.search(part)
        if match:
            codes.append(match.group(1).lower())
    return codes


def is_thematic_legacy_row(row: dict[str, str]) -> bool:
    return bool(extract_thematic_code_from_legacy(row))


def is_collection_row(row: dict[str, str]) -> bool:
    return row.get("Title", "").strip() == "Social Scientists Map Chicago Collection"


def build_unique_title_index(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    buckets: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        candidate_titles = [part.strip() for part in str(row.get("Title", "")).split("|") if part.strip()]
        if not candidate_titles:
            candidate_titles = [row.get("Title", "")]
        for candidate_title in candidate_titles:
            key = skeletonize_title(candidate_title)
            if not key:
                continue
            buckets.setdefault(key, []).append(row)
    return {key: bucket[0] for key, bucket in buckets.items() if len(bucket) == 1}


def build_thematic_row_map(rows: list[dict[str, str]]) -> tuple[dict[str, dict[str, str]], list[str]]:
    thematic_rows = [
        row
        for row in rows
        if row.get("Title", "").strip() == "[Thematic maps of Chicago]."
        and "G4104-C6-1933-U5-" in row.get("Identifier", "")
    ]
    if not thematic_rows:
        return {}, []

    codes_in_order = extract_thematic_codes_from_identifier(thematic_rows[0].get("Identifier", ""))
    if len(codes_in_order) != len(thematic_rows):
        raise ValueError(
            "Thematic bundle order is ambiguous: "
            f"{len(codes_in_order)} identifier codes for {len(thematic_rows)} rows."
        )

    thematic_by_code: dict[str, dict[str, str]] = {}
    for code, row in zip(codes_in_order, thematic_rows):
        thematic_by_code[code] = row

    return thematic_by_code, codes_in_order


def main() -> None:
    args = parse_args()

    legacy_rows = read_rows(args.legacy_csv)
    new_rows = read_rows(args.new_csv)

    unique_new_titles = build_unique_title_index(new_rows)
    thematic_new_by_code, thematic_order = build_thematic_row_map(new_rows)

    matched_new_ids: set[str] = set()
    crosswalk_rows: list[dict[str, str]] = []
    enriched_legacy_rows: list[dict[str, str]] = []

    for legacy_row in legacy_rows:
        legacy_title = legacy_row.get("Title", "")
        legacy_key = skeletonize_title(legacy_title)

        match_row: dict[str, str] | None = None
        match_method = ""
        match_note = ""

        if is_collection_row(legacy_row):
            match_method = "legacy_collection_only"
            match_note = "Collection-level legacy record has no item-level counterpart in the 2026 LUNA CSV."
        elif is_thematic_legacy_row(legacy_row):
            thematic_code = extract_thematic_code_from_legacy(legacy_row)
            match_row = thematic_new_by_code.get(thematic_code)
            match_method = "thematic_bundle_order_inferred" if match_row else "unmatched"
            if match_row:
                match_note = (
                    "Mapped via the ordered G4104-C6-1933-U5 filename/code sequence embedded "
                    "in the repeated LUNA Identifier field for the thematic bundle."
                )
            else:
                match_note = f"No thematic bundle row found for code {thematic_code}."
        else:
            match_row = unique_new_titles.get(legacy_key)
            if match_row:
                match_method = "title_skeleton_unique"
                match_note = "Matched on a punctuation/case-insensitive title key."
            else:
                match_method = "unmatched"
                match_note = "No unique title-based match found in the 2026 LUNA CSV."

        if match_row:
            matched_new_ids.add(match_row["ID"])

        crosswalk_row = {
            "legacy_title": legacy_title,
            "legacy_id": legacy_row.get("ID", ""),
            "legacy_identifier": legacy_row.get("Identifier", ""),
            "legacy_b1g_image": legacy_row.get("B1G Image", ""),
            "legacy_thematic_code": extract_thematic_code_from_legacy(legacy_row),
            "new_id": match_row.get("ID", "") if match_row else "",
            "new_title": match_row.get("Title", "") if match_row else "",
            "new_identifier": match_row.get("Identifier", "") if match_row else "",
            "match_method": match_method,
            "match_note": match_note,
        }
        crosswalk_rows.append(crosswalk_row)

        enriched_row = dict(legacy_row)
        enriched_row["Matched New ID"] = crosswalk_row["new_id"]
        enriched_row["Matched New Title"] = crosswalk_row["new_title"]
        enriched_row["Matched New Identifier"] = crosswalk_row["new_identifier"]
        enriched_row["Match Method"] = match_method
        enriched_row["Match Note"] = match_note
        enriched_legacy_rows.append(enriched_row)

    filtered_new_rows = [row for row in new_rows if row.get("ID", "") not in matched_new_ids]

    if len(thematic_new_by_code) != len(thematic_order):
        raise ValueError("Duplicate thematic code detected while building the new-row map.")

    matched_item_count = sum(
        1 for row in crosswalk_rows if row["match_method"] not in {"legacy_collection_only", "unmatched"}
    )
    unmatched_count = sum(1 for row in crosswalk_rows if row["match_method"] == "unmatched")

    if matched_item_count != 45:
        raise ValueError(f"Expected 45 item-level matches, found {matched_item_count}.")
    if unmatched_count:
        raise ValueError(f"Expected 0 unmatched item-level rows, found {unmatched_count}.")

    write_rows(
        args.crosswalk_csv,
        crosswalk_rows,
        [
            "legacy_title",
            "legacy_id",
            "legacy_identifier",
            "legacy_b1g_image",
            "legacy_thematic_code",
            "new_id",
            "new_title",
            "new_identifier",
            "match_method",
            "match_note",
        ],
    )
    write_rows(
        args.enriched_legacy_csv,
        enriched_legacy_rows,
        list(legacy_rows[0].keys())
        + [
            "Matched New ID",
            "Matched New Title",
            "Matched New Identifier",
            "Match Method",
            "Match Note",
        ],
    )
    write_rows(args.filtered_new_csv, filtered_new_rows, list(new_rows[0].keys()))

    print(f"Legacy rows: {len(legacy_rows)}")
    print(f"Matched item rows: {matched_item_count}")
    print(f"Collection-only legacy rows: {sum(1 for row in crosswalk_rows if row['match_method'] == 'legacy_collection_only')}")
    print(f"Filtered new rows: {len(filtered_new_rows)}")
    print(f"Removed from new CSV: {len(matched_new_ids)}")
    print(f"Crosswalk written to: {args.crosswalk_csv}")
    print(f"Enriched legacy CSV written to: {args.enriched_legacy_csv}")
    print(f"Filtered new CSV written to: {args.filtered_new_csv}")


if __name__ == "__main__":
    main()
