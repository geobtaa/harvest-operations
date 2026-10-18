import re

import pandas as pd
import pytest

from utils.local_ids import (
    REGISTRY_COLUMNS,
    assign_persistent_ids,
    assign_reconciled_new_ids,
    new_local_id,
)
from utils.metadata_reconciliation import handle_keys, reconcile, umich_keys


def source(catalog="123", image="456", record_id="temporary"):
    return pd.DataFrame(
        [
            {
                "ID": record_id,
                "Identifier": f"oai:quod.lib.umich.edu:IC-CLARK1IC-X-{catalog}%5D{image}",
                "Title": "Map",
            }
        ]
    )


def test_nanoid_format_and_registry_reuse(tmp_path):
    path = tmp_path / "ids.csv"
    first = assign_persistent_ids(source(), path, umich_keys)
    record_id = first.iloc[0].ID
    assert re.fullmatch(r"um_[A-Za-z0-9]{12}", record_id)
    assert any(c.islower() for c in record_id[3:])
    assert any(c.isupper() for c in record_id[3:])
    assert any(c.isdigit() for c in record_id[3:])
    # Source changes the image ID, but the catalog alias remains.
    again = assign_persistent_ids(
        source(image="789", record_id="changed"), path, umich_keys
    )
    assert again.iloc[0].ID == record_id
    assert again.attrs["local_id_assignments"].iloc[0].assignment_status == "reused"
    registry = pd.read_csv(path)
    assert "456" in registry.iloc[0].Identifier and "789" in registry.iloc[0].Identifier
    assert again.iloc[0].Identifier == source(image="789").iloc[0].Identifier


def test_existing_id_is_retained_and_absent_entries_survive(tmp_path):
    path = tmp_path / "ids.csv"
    pd.DataFrame(
        [
            {
                "ID": "existing-uuid",
                "Identifier": source().iloc[0].Identifier,
                "created_at": "2020-01-01",
                "assignment_origin": "existing",
            }
        ],
        columns=REGISTRY_COLUMNS,
    ).to_csv(path, index=False)
    result = assign_persistent_ids(source(), path, umich_keys)
    assert result.iloc[0].ID == "existing-uuid"
    assign_persistent_ids(source(catalog="999", image="888"), path, umich_keys)
    assert "existing-uuid" in set(pd.read_csv(path).ID)


def test_generation_retries_missing_character_classes_and_collision(monkeypatch):
    suffixes = iter(["a" * 12, "A" * 12, "1" * 12, "aB1234567890", "cD1234567890"])

    def generate(alphabet, size):
        assert set(alphabet) == set(
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        )
        assert size == 12
        return next(suffixes)

    monkeypatch.setattr("utils.local_ids.generate", generate)
    assert new_local_id("um_", {"um_aB1234567890"}) == "um_cD1234567890"


def test_conflicting_aliases_and_multiple_claims_do_not_write(tmp_path):
    path = tmp_path / "ids.csv"
    rows = [
        dict(
            ID=record_id,
            Identifier=source().iloc[0].Identifier,
            created_at="today",
            assignment_origin="existing",
        )
        for record_id in ["old1", "old2"]
    ]
    pd.DataFrame(rows).to_csv(path, index=False)
    before = path.read_bytes()
    with pytest.raises(ValueError, match="Conflicting local IDs"):
        assign_persistent_ids(source(), path, umich_keys)
    assert path.read_bytes() == before
    pd.DataFrame(rows[:1]).to_csv(path, index=False)
    before = path.read_bytes()
    with pytest.raises(ValueError, match="Multiple harvested items"):
        assign_persistent_ids(
            pd.concat([source(), source(record_id="another")]), path, umich_keys
        )
    assert path.read_bytes() == before


def test_reconciled_new_ids_are_persistent_and_do_not_replace_existing_ids(
    tmp_path,
    monkeypatch,
):
    suffixes = iter(["aB1234567890"])
    monkeypatch.setattr(
        "utils.local_ids.generate",
        lambda alphabet, size: next(suffixes),
    )
    harvested = pd.DataFrame(
        [
            {
                "ID": "harvested-existing",
                "Identifier": "hdl:1903.1/12533",
                "Title": "Existing map",
            },
            {
                "ID": "harvested-new",
                "Identifier": "hdl:1903.1/99999",
                "Title": "New map",
            },
        ]
    )
    distributions = pd.DataFrame(
        [
            {
                "friendlier_id": "harvested-existing",
                "distribution_url": "https://hdl.handle.net/1903.1/12533",
            },
            {
                "friendlier_id": "harvested-new",
                "distribution_url": "https://hdl.handle.net/1903.1/99999",
            },
        ]
    )
    outputs = reconcile(
        harvested,
        pd.DataFrame([{"ID": "existing-uuid", "Title": "Curated map"}]),
        pd.DataFrame(
            [
                {
                    "friendlier_id": "existing-uuid",
                    "distribution_url": "http://hdl.handle.net/1903.1/12533",
                }
            ]
        ),
        harvested_distributions=distributions,
        identity_keys=handle_keys,
    )
    registry_path = tmp_path / "umd_ids.csv"

    assigned = assign_reconciled_new_ids(
        outputs,
        registry_path,
        handle_keys,
        "umd_",
    )

    assert assigned["reconciled_primary"]["ID"].tolist() == [
        "existing-uuid",
        "umd_aB1234567890",
    ]
    assert assigned["reconciled_distributions"]["friendlier_id"].tolist() == [
        "existing-uuid",
        "umd_aB1234567890",
    ]
    assert assigned["crosswalk"]["canonical_id"].tolist() == [
        "existing-uuid",
        "umd_aB1234567890",
    ]
    assert assigned["local_id_assignments"].iloc[0]["assignment_status"] == "created"

    repeated = assign_reconciled_new_ids(
        outputs,
        registry_path,
        handle_keys,
        "umd_",
    )
    assert repeated["reconciled_primary"]["ID"].tolist()[-1] == "umd_aB1234567890"
    assert repeated["local_id_assignments"].iloc[0]["assignment_status"] == "reused"
