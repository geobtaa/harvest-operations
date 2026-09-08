"""Build one Michigan catalog item from its image-level OAI records."""

from __future__ import annotations

from collections import defaultdict
import re
from urllib.parse import unquote

import pandas as pd


OAI_IMAGE_ID = re.compile(
    r"oai:quod\.lib\.umich\.edu:IC-([\w]+)-X-([\w.-]+)\]([\w.-]+)$",
    re.IGNORECASE,
)


def collapse_umich_catalog_items(
    df: pd.DataFrame, source_id_prefix: str, field_separators: dict[str, str]
) -> pd.DataFrame:
    """Group by collection + catalog number, never by title or image suffix.

    Keep all page identifiers for reconciliation, union repeatable metadata, and
    reject conflicting scalar metadata rather than silently discarding it. The
    catalog manifest contains every canvas, including images with distinct barcodes.
    """
    if df.empty:
        return df.copy()
    groups = defaultdict(list)
    for row in df.to_dict("records"):
        identities = [
            match.groups()
            for token in row.get("Identifier", "").split("|")
            if (match := OAI_IMAGE_ID.fullmatch(unquote(token.strip())))
        ]
        keys = {
            (collection.lower(), catalog.lstrip("0") or "0")
            for collection, catalog, image in identities
        }
        if len(keys) != 1:
            raise ValueError(
                f"Michigan item grouping needs one catalog identity per row: {row['ID']}"
            )
        groups[next(iter(keys))].append((row, identities))

    records, membership = [], []
    for (collection, catalog), members in sorted(groups.items()):
        members.sort(key=lambda member: sorted(member[1]))
        # Retain source padding in URLs; normalized catalog numbers stabilize local IDs.
        source_catalog = max(
            (identity[1] for _, identities in members for identity in identities),
            key=lambda value: (len(value), value),
        )
        record = dict(members[0][0])
        record_id = f"{source_id_prefix}_{collection}_{catalog}"
        for column in df.columns:
            if column in {"ID", "information", "manifest"}:
                continue
            values = list(
                dict.fromkeys(row[column] for row, _ in members if row[column])
            )
            if len(values) <= 1:
                record[column] = values[0] if values else ""
                continue
            separator = field_separators.get(column, "")
            if not separator:
                raise ValueError(
                    f"Conflicting {column!r} values for Michigan catalog item {collection}:{catalog}"
                )
            record[column] = separator.join(
                dict.fromkeys(
                    part for value in values for part in value.split(separator) if part
                )
            )
        record["ID"] = record_id
        # A source-provided persistent URL for one image also opens the item viewer.
        # Keep a single landing link; the manifest explicitly addresses the whole item.
        record["manifest"] = (
            f"https://quod.lib.umich.edu/cgi/i/image/api/manifest/{collection}:{source_catalog}"
        )
        records.append(record)
        for row, identities in members:
            for _, _, image in identities:
                membership.append(
                    {
                        "image_harvested_id": row["ID"],
                        "item_harvested_id": record_id,
                        "catalog_key": f"{collection}:{catalog}",
                        "image_identifier": image,
                        "image_record_count": len(members),
                        "manifest": record["manifest"],
                    }
                )
    result = pd.DataFrame(records)
    result.attrs["umich_item_membership"] = pd.DataFrame(membership).drop_duplicates()
    return result
