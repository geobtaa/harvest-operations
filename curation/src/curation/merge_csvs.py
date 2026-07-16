from __future__ import annotations

import csv
import re
from collections import defaultdict
from itertools import product
from pathlib import Path


def _sanitize_suffix(label: str) -> str:
    sanitized = re.sub(r"[^0-9A-Za-z]+", "_", label.strip()).strip("_").lower()
    return sanitized or "csv"


def _read_csv_rows(path: str | Path) -> tuple[list[str], list[dict[str, str]]]:
    csv_path = Path(path)
    with csv_path.open("r", newline="", encoding="utf-8-sig") as infile:
        reader = csv.DictReader(infile)
        if reader.fieldnames is None:
            raise ValueError(f"{csv_path} does not contain a header row")
        rows = [{key: value or "" for key, value in row.items()} for row in reader]
    return list(reader.fieldnames), rows


def _normalize_key(value: str, *, ignore_case: bool) -> str:
    return value.casefold() if ignore_case else value


def _build_column_maps(
    left_headers: list[str],
    right_headers: list[str],
    *,
    left_key: str,
    right_key: str,
    left_label: str,
    right_label: str,
    status_column: str,
    source_column: str,
) -> tuple[dict[str, str], dict[str, str], list[str]]:
    left_suffix = _sanitize_suffix(left_label)
    right_suffix = _sanitize_suffix(right_label)
    reserved = {status_column, source_column}

    include_right = [header for header in right_headers if not (left_key == right_key and header == right_key)]
    duplicates = set(left_headers) & set(include_right)

    left_preferred: list[tuple[str, str]] = []
    for header in left_headers:
        name = header
        if header in duplicates or header in reserved:
            name = f"{header}_{left_suffix}"
        left_preferred.append((header, name))

    right_preferred: list[tuple[str, str]] = []
    for header in include_right:
        name = header
        if header in duplicates or header in reserved:
            name = f"{header}_{right_suffix}"
        right_preferred.append((header, name))

    used = set(reserved)
    left_map: dict[str, str] = {}
    right_map: dict[str, str] = {}
    output_columns: list[str] = []

    def assign_names(
        preferred_columns: list[tuple[str, str]],
        target: dict[str, str],
    ) -> None:
        for header, preferred in preferred_columns:
            candidate = preferred
            counter = 2
            while candidate in used:
                candidate = f"{preferred}_{counter}"
                counter += 1
            used.add(candidate)
            output_columns.append(candidate)
            target[header] = candidate

    assign_names(left_preferred, left_map)
    assign_names(right_preferred, right_map)

    output_columns.extend([status_column, source_column])
    return left_map, right_map, output_columns


def _index_rows(
    rows: list[dict[str, str]],
    key_name: str,
    *,
    ignore_case: bool,
) -> dict[str, list[dict[str, str]]]:
    indexed: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        indexed[_normalize_key(row.get(key_name, ""), ignore_case=ignore_case)].append(row)
    return indexed


def _build_output_row(
    *,
    left_row: dict[str, str] | None,
    right_row: dict[str, str] | None,
    left_headers: list[str],
    right_headers: list[str],
    left_map: dict[str, str],
    right_map: dict[str, str],
    output_columns: list[str],
    left_key: str,
    right_key: str,
    status_column: str,
    source_column: str,
    left_label: str,
    right_label: str,
) -> dict[str, str]:
    row = {column: "" for column in output_columns}

    if left_row is not None:
        for header in left_headers:
            row[left_map[header]] = left_row.get(header, "")
    if right_row is not None:
        if left_row is None and left_key == right_key:
            row[left_map[left_key]] = right_row.get(right_key, "")
        for header in right_headers:
            if left_key == right_key and header == right_key:
                continue
            row[right_map[header]] = right_row.get(header, "")

    if left_row is not None and right_row is not None:
        row[status_column] = "matched"
    elif left_row is not None:
        row[status_column] = "unmatched"
        row[source_column] = left_label
    else:
        row[status_column] = "unmatched"
        row[source_column] = right_label

    return row


def merge_csvs(
    left_path: str | Path,
    right_path: str | Path,
    output_path: str | Path,
    *,
    left_key: str,
    right_key: str | None = None,
    left_label: str | None = None,
    right_label: str | None = None,
    status_column: str = "match_status",
    source_column: str = "unmatched_source",
    ignore_key_case: bool = False,
) -> int:
    left_file = Path(left_path)
    right_file = Path(right_path)
    output_file = Path(output_path)

    left_headers, left_rows = _read_csv_rows(left_file)
    right_headers, right_rows = _read_csv_rows(right_file)

    right_key = right_key or left_key
    left_label = left_label or left_file.stem
    right_label = right_label or right_file.stem

    if left_key not in left_headers:
        raise ValueError(f"{left_file} is missing key column {left_key!r}")
    if right_key not in right_headers:
        raise ValueError(f"{right_file} is missing key column {right_key!r}")

    left_map, right_map, output_columns = _build_column_maps(
        left_headers,
        right_headers,
        left_key=left_key,
        right_key=right_key,
        left_label=left_label,
        right_label=right_label,
        status_column=status_column,
        source_column=source_column,
    )

    left_index = _index_rows(left_rows, left_key, ignore_case=ignore_key_case)
    right_index = _index_rows(right_rows, right_key, ignore_case=ignore_key_case)

    merged_rows: list[dict[str, str]] = []
    nonblank_columns: set[str] = set()

    seen_left_keys: set[str] = set()
    for left_row in left_rows:
        key = _normalize_key(left_row.get(left_key, ""), ignore_case=ignore_key_case)
        if key in seen_left_keys:
            continue
        seen_left_keys.add(key)

        left_group = left_index[key]
        right_group = right_index.get(key, [])
        pairs = product(left_group, right_group) if right_group else ((row, None) for row in left_group)

        for current_left, current_right in pairs:
            output_row = _build_output_row(
                left_row=current_left,
                right_row=current_right,
                left_headers=left_headers,
                right_headers=right_headers,
                left_map=left_map,
                right_map=right_map,
                output_columns=output_columns,
                left_key=left_key,
                right_key=right_key,
                status_column=status_column,
                source_column=source_column,
                left_label=left_label,
                right_label=right_label,
            )
            merged_rows.append(output_row)
            nonblank_columns.update(column for column, value in output_row.items() if value != "")

    seen_right_keys: set[str] = set()
    for right_row in right_rows:
        key = _normalize_key(right_row.get(right_key, ""), ignore_case=ignore_key_case)
        if key in seen_right_keys or key in left_index:
            continue
        seen_right_keys.add(key)

        for current_right in right_index[key]:
            output_row = _build_output_row(
                left_row=None,
                right_row=current_right,
                left_headers=left_headers,
                right_headers=right_headers,
                left_map=left_map,
                right_map=right_map,
                output_columns=output_columns,
                left_key=left_key,
                right_key=right_key,
                status_column=status_column,
                source_column=source_column,
                left_label=left_label,
                right_label=right_label,
            )
            merged_rows.append(output_row)
            nonblank_columns.update(column for column, value in output_row.items() if value != "")

    final_columns = [column for column in output_columns if column in nonblank_columns]

    with output_file.open("w", newline="", encoding="utf-8") as outfile:
        writer = csv.DictWriter(outfile, fieldnames=final_columns)
        writer.writeheader()
        writer.writerows(
            {column: row[column] for column in final_columns}
            for row in merged_rows
        )

    return len(merged_rows)



