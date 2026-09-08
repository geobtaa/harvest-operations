import pandas as pd

from harvesters.oai_qdc import OaiQdcHarvester
from utils.metadata_reconciliation import reconcile


def test_source_subject_terms_keep_labels_and_discard_http_values():
    harvester = OaiQdcHarvester(
        {"name": "test", "oai_base_url": "https://example.edu/oai"}
    )
    row = harvester.build_dataframe(
        [
            {
                "oai_identifier": "oai:example.edu:maps-1",
                "set_spec": "maps",
                "fields": {
                    "dc:title": ["Map"],
                    "dc:subject": [
                        "World maps. http://id.loc.gov/authorities/subjects/sh85148208; Railroads.",
                        "https://id.example/subject",
                    ],
                    "dcterms:subject": [
                        "Geology Michigan Maps. https://id.example/geology"
                    ],
                },
            }
        ]
    ).iloc[0]
    assert row["Subject"] == "World maps.|Railroads.|Geology Michigan Maps."
    assert not row["Creator ID"]
    assert "id.example" not in "|".join(str(value) for value in row)


def test_excluded_subject_does_not_override_source_even_when_blank():
    harvested = pd.DataFrame(
        [
            {
                "ID": "old1",
                "Identifier": "",
                "Subject": "Source subject",
                "Creator": "Source name",
            },
            {"ID": "old2", "Identifier": "", "Subject": "", "Creator": "Source name"},
        ]
    )
    enrichments = pd.DataFrame(
        [
            {"ID": record_id, "Subject": "Manual subject", "Creator": "Curated name"}
            for record_id in ["old1", "old2"]
        ]
    )
    distributions = pd.DataFrame(columns=["friendlier_id", "distribution_url"])
    result = reconcile(
        harvested, enrichments, distributions, excluded_enrichment_fields=("Subject",)
    )
    assert result["reconciled_primary"]["Subject"].tolist() == ["Source subject", ""]
    assert result["reconciled_primary"]["Creator"].tolist() == [
        "Curated name",
        "Curated name",
    ]
    assert "Subject" not in set(result["enrichment_audit"].field)
    assert enrichments.Subject.tolist() == ["Manual subject", "Manual subject"]
