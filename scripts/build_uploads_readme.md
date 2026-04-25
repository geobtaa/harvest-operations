# Build Uploads Script

## Purpose

`scripts/build_uploads.py` compares the two most recent dated harvest outputs for a source and prepares upload-ready CSVs for GBL Admin.

It handles three cases:

1. New primary records
2. Retired primary records
3. Distribution rows that were added or removed for records whose `ID` stayed the same

This is especially useful for `ogmWisc`, where the source metadata may keep the same record `ID` while replacing or removing distribution links.

## Outputs

For a source like `ogmWisc`, the script writes:

* `outputs/to_upload/YYYY-MM-DD_ogmWisc_primary_upload.csv`
* `outputs/to_upload/YYYY-MM-DD_ogmWisc_distributions_new.csv`
* `outputs/to_upload/YYYY-MM-DD_ogmWisc_distributions_delete.csv`

## What the script compares

### Primary CSVs

The script finds the two most recent files matching:

* `YYYY-MM-DD_<source>_primary.csv`
* `YYYY-MM_DD_<source>_primary.csv`

It compares them by `ID`.

* Rows in the newest file but not the previous file are treated as new primary records.
* Rows in the previous file but not the newest file are treated as retired primary records.
* Shared `ID` values are treated as unchanged at the primary level.

### Distribution CSVs

The script finds:

* the distributions CSV for the newest primary date
* the distributions CSV for the previous primary date, if it exists

It compares distribution rows by:

* `friendlier_id`
* `reference_type`
* `distribution_url`
* `label`

For shared primary `ID` values:

* rows present only in the new distributions file go to `distributions_new.csv`
* rows present only in the old distributions file go to `distributions_delete.csv`

For brand-new primary records:

* all current distribution rows for those new `ID` values go to `distributions_new.csv`

If the previous distributions snapshot is missing, the script still builds the primary upload and the new-distributions file for brand-new records, but it skips same-ID distribution change detection.

## Primary upload behavior

`primary_upload.csv` contains:

* all newest-file rows where `Resource Class == "Websites"` if that column exists
* all brand-new primary records
* all retired primary records, with:
  * `Publication State = unpublished`
  * `Date Retired = <today>`
  * `Display Note` updated with the retirement warning

The primary upload does not include same-ID records whose only change was in the distributions table. Those changes are handled through the distribution delta files instead.

## Distribution delta behavior

`distributions_new.csv` contains rows to add:

* all distributions for brand-new primary records
* newly added distribution rows for existing records

`distributions_delete.csv` contains rows to delete:

* distribution rows that existed in the previous run but are gone in the newest run for the same `friendlier_id`

## Command line usage

The script now accepts a source argument and optional outputs directory:

```bash
python3 scripts/build_uploads.py ogmWisc
```

```bash
python3 scripts/build_uploads.py ogmWisc --outputs-dir outputs
```

If no source is provided, it defaults to `ogmWisc`.

## Assumptions

* Primary CSVs include an `ID` column.
* Distribution CSVs include a `friendlier_id` column.
* Distribution CSVs normally include `reference_type`, `distribution_url`, and `label`. If any of those columns are missing, the script fills them as blank strings before comparing.
* The comparison is based on the two most recent dated primary snapshots for the chosen source.

## Troubleshooting

* **Need at least two matching primary files**
  Ensure the source has at least two dated `*_primary.csv` files in `outputs/`.

* **Newest distributions file not found**
  Ensure a same-date `*_distributions.csv` exists for the newest primary run.

* **No same-ID distribution changes detected**
  Confirm that an older same-date distributions snapshot exists for the previous primary run. Without that older file, the script can only prepare new distributions for brand-new records.

* **Unexpected distribution delete rows**
  The delete file is row-based, not ID-based. If a record keeps the same `friendlier_id` but loses one link, only that missing distribution row is written to `distributions_delete.csv`.
