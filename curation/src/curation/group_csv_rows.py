from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable, Iterator, Sequence


def _extract_value(row: Sequence[str], column: int | None) -> str:
    if column is not None:
        if column < 0:
            raise ValueError("column must be zero or greater")
        return row[column].strip() if column < len(row) else ""

    for value in row:
        trimmed = value.strip()
        if trimmed:
            return trimmed

    return ""


def _chunk_values(
    values: Iterable[str],
    rows_per_record: int,
    *,
    pad_missing: bool = False,
    drop_incomplete: bool = False,
) -> Iterator[list[str]]:
    if rows_per_record <= 0:
        raise ValueError("rows_per_record must be greater than zero")
    if pad_missing and drop_incomplete:
        raise ValueError("pad_missing and drop_incomplete cannot both be True")

    chunk: list[str] = []
    for value in values:
        chunk.append(value)
        if len(chunk) == rows_per_record:
            yield chunk
            chunk = []

    if chunk:
        if drop_incomplete:
            return
        if not pad_missing:
            raise ValueError(
                "input row count is not evenly divisible by rows_per_record; "
                "use pad_missing=True to keep the final partial row or "
                "drop_incomplete=True to discard it"
            )
        chunk.extend([""] * (rows_per_record - len(chunk)))
        yield chunk


def reshape_csv(
    input_path: str | Path,
    output_path: str | Path,
    *,
    rows_per_record: int,
    column: int | None = None,
    pad_missing: bool = False,
    drop_incomplete: bool = False,
) -> int:
    input_file = Path(input_path)
    output_file = Path(output_path)

    values: list[str] = []
    with input_file.open("r", newline="", encoding="utf-8-sig") as infile:
        reader = csv.reader(infile)
        for row in reader:
            if not any(cell.strip() for cell in row):
                continue
            values.append(_extract_value(row, column))

    records = list(
        _chunk_values(
            values,
            rows_per_record,
            pad_missing=pad_missing,
            drop_incomplete=drop_incomplete,
        )
    )

    with output_file.open("w", newline="", encoding="utf-8") as outfile:
        writer = csv.writer(outfile)
        writer.writerows(records)

    return len(records)
