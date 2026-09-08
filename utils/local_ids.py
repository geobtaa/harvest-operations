"""Persist random local IDs independently of source identifiers."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import fcntl
import os
from pathlib import Path
import string
import tempfile

from nanoid import generate
import pandas as pd


REGISTRY_COLUMNS = ["ID", "Identifier", "created_at", "assignment_origin"]
ALPHABET = string.ascii_letters + string.digits


def new_local_id(prefix: str, used: set[str]) -> str:
    """Generate 12 alphanumeric characters including all three character classes."""
    for _ in range(1000):
        suffix = generate(alphabet=ALPHABET, size=12)
        candidate = prefix + suffix
        if (
            any(c.islower() for c in suffix)
            and any(c.isupper() for c in suffix)
            and any(c.isdigit() for c in suffix)
            and candidate not in used
        ):
            return candidate
    raise ValueError("Could not generate a unique mixed-case alphanumeric Nano ID")


def assign_persistent_ids(
    df: pd.DataFrame, registry_path: Path, identity_keys, prefix: str = "um_"
) -> pd.DataFrame:
    """Reuse IDs by source identity aliases; atomically log new assignments.

    Existing IDs in the registry are retained regardless of their format. A caller
    can seed established catalog IDs before the first run. Conflicting aliases or
    multiple input rows claiming one local ID fail before the registry is written.
    """
    registry_path = Path(registry_path)
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    # A stable separate lock survives atomic replacement of the CSV itself.
    with registry_path.with_suffix(registry_path.suffix + ".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        registry = (
            pd.read_csv(registry_path, dtype=str, keep_default_na=False)
            if registry_path.exists()
            else pd.DataFrame(columns=REGISTRY_COLUMNS)
        )
        if set(REGISTRY_COLUMNS) - set(registry.columns):
            raise ValueError(f"Missing registry columns in {registry_path}")
        if registry.ID.str.strip().eq("").any() or registry.ID.duplicated().any():
            raise ValueError("Local ID registry requires unique, nonblank IDs")
        entries = {row["ID"]: row for row in registry.to_dict("records")}
        index = defaultdict(set)
        for record_id, row in entries.items():
            keys = identity_keys(row["Identifier"])
            if not keys:
                raise ValueError(
                    f"Registry ID has no usable source identifiers: {record_id}"
                )
            for key in keys:
                index[key].add(record_id)
        used, claimed, log, rows = set(entries), set(), [], []
        now = datetime.now(timezone.utc).isoformat()
        for row in df.to_dict("records"):
            identifiers = row.get("Identifier", "")
            keys = identity_keys(identifiers)
            if not keys:
                raise ValueError(
                    f"No source identifiers for local ID assignment: {row['ID']}"
                )
            candidates = set().union(*(index[key] for key in keys))
            if len(candidates) > 1:
                raise ValueError(
                    f"Conflicting local IDs for {row['ID']}: {sorted(candidates)}"
                )
            if candidates:
                record_id = next(iter(candidates))
                status = "reused"
            else:
                record_id = new_local_id(prefix, used)
                used.add(record_id)
                entries[record_id] = {
                    "ID": record_id,
                    "Identifier": "",
                    "created_at": now,
                    "assignment_origin": "nanoid",
                }
                status = "created"
            if record_id in claimed:
                raise ValueError(f"Multiple harvested items claim local ID {record_id}")
            claimed.add(record_id)
            historical = entries[record_id]["Identifier"].split("|")
            entries[record_id]["Identifier"] = "|".join(
                dict.fromkeys(
                    token for token in historical + identifiers.split("|") if token
                )
            )
            for key in keys:
                index[key].add(record_id)
            log.append(
                {
                    "previous_harvested_id": row["ID"],
                    "ID": record_id,
                    "assignment_status": status,
                    "Identifier": identifiers,
                }
            )
            row["ID"] = record_id
            rows.append(row)
        updated = pd.DataFrame(entries.values(), columns=REGISTRY_COLUMNS)
        # Failed validation never changes the persistent registry.
        temp_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                dir=registry_path.parent,
                prefix=registry_path.name + ".",
                suffix=".tmp",
                delete=False,
                encoding="utf-8",
                newline="",
            ) as handle:
                temp_path = Path(handle.name)
                updated.to_csv(handle, index=False)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, registry_path)
        finally:
            if temp_path is not None:
                temp_path.unlink(missing_ok=True)
        result = pd.DataFrame(rows, columns=df.columns)
        result.attrs["local_id_assignments"] = pd.DataFrame(
            log,
            columns=["previous_harvested_id", "ID", "assignment_status", "Identifier"],
        )
        return result
