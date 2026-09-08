"""Conservative identity matching and sparse enrichment overlays for reharvests."""

from __future__ import annotations

from collections import defaultdict
import re
from urllib.parse import unquote, urlsplit
import xml.etree.ElementTree as ET
from pathlib import Path

import pandas as pd


OAI_NS = "{http://www.openarchives.org/OAI/2.0/}"


def umich_keys(value: str) -> set[str]:
    """Extract collection-scoped image/item keys; retain sheet suffixes exactly.

    Catalog-only links are weak evidence: a catalog record can contain many images.
    URL hosts are checked so unrelated URLs cannot accidentally supply identities.
    """
    keys = set()
    for token in value.split("|"):
        token = unquote(token.strip())
        parsed = urlsplit(token)
        if parsed.scheme in {"http", "https"}:
            if parsed.hostname not in {"quod.lib.umich.edu", "name.umdl.umich.edu"}:
                continue
            text = parsed.path
        elif token.lower().startswith("oai:quod.lib.umich.edu:"):
            text = token
        else:
            continue
        patterns = [
            r"IC-([\w]+)-X-([\w.-]+)\]([\w.-]+)",
            r"/c/([\w]+)/x-([\w.-]+)/([\w.-]+)",
            r"/api/(?:manifest|search)/([\w]+):([\w.-]+)(?::([\w.-]+))?",
            r"/api/thumb/([\w]+)/([\w.-]+)/([\w.-]+)",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if not match:
                continue
            collection, catalog, item = match.groups()
            collection = collection.lower()
            # Catalog numbers vary in zero padding between older and newer exports.
            catalog = catalog.lstrip("0") or "0"
            keys.add(f"catalog:{collection}:{catalog}")
            if item:
                keys.add(f"image:{collection}:{item}")
                keys.add(f"item:{collection}:{catalog}:{item}")
    return keys


def read_csv(path: Path, required: tuple[str, ...]) -> pd.DataFrame:
    frame = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    missing = set(required) - set(frame.columns)
    if missing:
        raise ValueError(f"{path}: missing columns {sorted(missing)}")
    return frame


def require_unique_ids(frame: pd.DataFrame, label: str) -> None:
    if frame["ID"].str.strip().eq("").any() or frame["ID"].duplicated().any():
        raise ValueError(f"{label}: IDs must be nonblank and unique")


def deleted_keys(xml_dir: Path | None) -> dict[str, set[str]]:
    result = defaultdict(set)
    if xml_dir is None:
        return result
    paths = sorted(xml_dir.rglob("*.xml"))
    if not paths:
        raise ValueError(f"No XML files found in {xml_dir}")
    for path in paths:
        for _, element in ET.iterparse(path, events=("end",)):
            if element.tag != f"{OAI_NS}record":
                continue
            header = element.find(f"{OAI_NS}header")
            if header is not None and header.get("status") == "deleted":
                identifier = header.findtext(f"{OAI_NS}identifier", "")
                datestamp = header.findtext(f"{OAI_NS}datestamp", "")
                for key in umich_keys(identifier):
                    if not key.startswith("catalog:"):
                        result[key].add(f"{datestamp} {identifier}")
            element.clear()
    return result


def reconcile(
    harvested: pd.DataFrame,
    enrichments: pd.DataFrame,
    distributions: pd.DataFrame,
    aliases: pd.DataFrame | None = None,
    deletions: dict[str, set[str]] | None = None,
    harvested_distributions: pd.DataFrame | None = None,
    decisions: pd.DataFrame | None = None,
    excluded_enrichment_fields: tuple[str, ...] = (),
) -> dict[str, pd.DataFrame]:
    """Return reviewable outputs without modifying the source frames.

    Match only mutually unique strong-key candidates. Catalog-only candidates and
    conflicting keys are held for review; blank overrides never erase source data.
    Saved aliases extend the existing inventory, including previously new records.
    """
    require_unique_ids(harvested, "Harvest")
    require_unique_ids(enrichments, "Enrichments")
    unknown = set(enrichments.columns) - set(harvested.columns)
    if unknown:
        raise ValueError(
            f"Enrichment columns missing from harvested schema: {sorted(unknown)}"
        )
    old_keys = defaultdict(set)
    for record_id in enrichments["ID"]:
        old_keys[record_id].add(f"id:{record_id}")
    for row in distributions.to_dict("records"):
        record_id = row["friendlier_id"]
        if not record_id.strip():
            raise ValueError("Existing distributions contain a blank friendlier_id")
        old_keys[record_id].update(umich_keys(row["distribution_url"]))
        old_keys[record_id].add(f"id:{record_id}")
    if aliases is not None:
        for row in aliases.to_dict("records"):
            record_id, key = row["canonical_id"], row["match_key"]
            if not record_id.strip() or not key.strip():
                raise ValueError(
                    "Saved aliases must have nonblank canonical_id and match_key"
                )
            old_keys[record_id].update((key, f"id:{record_id}"))
    index = defaultdict(set)
    for record_id, keys in old_keys.items():
        for key in keys:
            index[key].add(record_id)
    new_keys = {}
    for row in harvested.to_dict("records"):
        new_keys[row["ID"]] = umich_keys(row.get("Identifier", "")) | {
            f"id:{row['ID']}"
        }
    if harvested_distributions is not None:
        for row in harvested_distributions.to_dict("records"):
            if row["friendlier_id"] not in new_keys:
                raise ValueError(
                    f"Harvest distribution ID absent from primary: {row['friendlier_id']}"
                )
            new_keys[row["friendlier_id"]].update(umich_keys(row["distribution_url"]))
    candidates, weak_candidates = {}, {}
    for record_id, keys in new_keys.items():
        candidates[record_id] = set().union(
            *(index[k] for k in keys if not k.startswith("catalog:"))
        )
        weak_candidates[record_id] = set().union(
            *(index[k] for k in keys if k.startswith("catalog:"))
        )
    manual = {}
    if decisions is not None:
        for row in decisions.to_dict("records"):
            action = row["action"].strip()
            if not action:
                continue
            source_id, target = row["harvested_id"], row["canonical_id"]
            if source_id not in new_keys or source_id in manual:
                raise ValueError(f"Unknown or duplicate review decision: {source_id}")
            if action == "accept_new" and not target.strip():
                if source_id in old_keys:
                    raise ValueError(
                        f"Cannot accept an existing canonical ID as new: {source_id}"
                    )
                candidates[source_id] = set()
                weak_candidates[source_id] = set()
            elif action == "match" and target in old_keys:
                candidates[source_id] = {target}
            else:
                raise ValueError(
                    "Decisions require accept_new with blank canonical_id, or match with an existing canonical_id"
                )
            manual[source_id] = action
    reverse = defaultdict(set)
    for record_id in new_keys:
        for old_id in candidates[record_id]:
            reverse[old_id].add(record_id)
    for source_id, action in manual.items():
        if action == "match" and len(reverse[next(iter(candidates[source_id]))]) != 1:
            raise ValueError(f"Review decision is not one-to-one: {source_id}")
    overlays = enrichments.set_index("ID").to_dict("index")
    rows, crosswalk, audit = [], [], []
    matched_old, review_old = set(), set()
    for row in harvested.to_dict("records"):
        source_id = row["ID"]
        strong = candidates[source_id]
        old_id = next(iter(strong)) if len(strong) == 1 else ""
        matched = bool(old_id and len(reverse[old_id]) == 1)
        possible = strong or weak_candidates[source_id]
        status = "matched" if matched else "needs_review" if possible else "new"
        canonical_id = old_id if matched else source_id
        evidence = sorted(new_keys[source_id] & old_keys[old_id]) if matched else []
        method = next(
            (
                kind
                for kind in ("id", "item", "image")
                if any(k.startswith(kind + ":") for k in evidence)
            ),
            "",
        )
        if source_id in manual:
            method = "manual_" + manual[source_id]
        fields = []
        if matched:
            matched_old.add(old_id)
            for field, value in overlays.get(old_id, {}).items():
                if field in excluded_enrichment_fields:
                    continue
                if value.strip():
                    audit.append(
                        {
                            "canonical_id": old_id,
                            "harvested_id": source_id,
                            "field": field,
                            "harvested_value": row[field],
                            "enriched_value": value,
                            "value_changed": row[field] != value,
                        }
                    )
                    row[field] = value
                    fields.append(field)
        else:
            review_old.update(possible)
        row["ID"] = canonical_id
        rows.append(row)
        crosswalk.append(
            {
                "harvested_id": source_id,
                "canonical_id": canonical_id,
                "match_status": status,
                "match_method": method,
                "match_evidence": "|".join(evidence),
                "candidate_existing_ids": "|".join(sorted(possible)),
                "enriched_fields": "|".join(fields),
                "Title": row.get("Title", ""),
            }
        )
    primary = pd.DataFrame(rows, columns=harvested.columns)
    require_unique_ids(
        primary, "Reconciled output (resolve identity collisions before continuing)"
    )
    crosswalk_df = pd.DataFrame(crosswalk)
    review = pd.concat(
        [
            primary.reset_index(drop=True),
            crosswalk_df.drop(columns=["Title"]).reset_index(drop=True),
        ],
        axis=1,
    )
    missing = []
    for record_id in sorted(set(old_keys) - matched_old):
        notices = sorted(
            set().union(
                *(
                    (deletions or {}).get(k, set())
                    for k in old_keys[record_id]
                    if not k.startswith("catalog:")
                )
            )
        )
        status = (
            "needs_review"
            if record_id in review_old
            else "source_deleted"
            if notices
            else "not_in_harvest"
        )
        missing.append(
            {
                "ID": record_id,
                **overlays.get(record_id, {}),
                "reconciliation_status": status,
                "source_deletion_notices": "|".join(notices),
            }
        )
    # Keep historical aliases, adding only resolved identities and truly unmatched items.
    for result in crosswalk:
        if result["match_status"] != "needs_review":
            old_keys[result["canonical_id"]].update(new_keys[result["harvested_id"]])
    alias_rows = [
        {"canonical_id": old_id, "match_key": key}
        for old_id in sorted(old_keys)
        for key in sorted(old_keys[old_id])
    ]
    outputs = {
        "reconciled_primary": primary,
        "review_primary": review,
        "new_items": review[review["match_status"].eq("new")],
        "unmatched_items": review[review["match_status"].ne("matched")],
        "needs_review": review[review["match_status"].eq("needs_review")],
        "crosswalk": crosswalk_df,
        "identity_aliases": pd.DataFrame(
            alias_rows, columns=["canonical_id", "match_key"]
        ),
        "enrichment_audit": pd.DataFrame(
            audit,
            columns=[
                "canonical_id",
                "harvested_id",
                "field",
                "harvested_value",
                "enriched_value",
                "value_changed",
            ],
        ),
        "missing_existing": pd.DataFrame(
            missing,
            columns=[
                *enrichments.columns,
                "reconciliation_status",
                "source_deletion_notices",
            ],
        ),
    }
    template = crosswalk_df.loc[
        crosswalk_df["match_status"].eq("needs_review"),
        ["harvested_id", "Title", "candidate_existing_ids"],
    ].copy()
    template["action"] = ""
    template["canonical_id"] = ""
    outputs["review_decisions"] = template
    if harvested_distributions is not None:
        remapped = harvested_distributions.copy()
        mapping = crosswalk_df.set_index("harvested_id")["canonical_id"]
        remapped["friendlier_id"] = remapped["friendlier_id"].map(mapping)
        outputs["reconciled_distributions"] = remapped
    return outputs
