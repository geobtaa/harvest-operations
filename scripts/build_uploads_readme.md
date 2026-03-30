# Build Uploads Script for Harvester Outputs

## Purpose

This script streamlines uploads to GBL Admin by producing two smaller CSV files that contain only records that changed since the previous harvest. Each harvest produces a “primary” CSV for a source (for example, ArcGIS or PASDA). Between harvests, some resources are newly published and others are retired. The script compares two dated primary CSVs for the same source, identifies the differences by `ID`, and writes an upload-ready file that includes only the new and retired records. It also filters the same-day “distributions” CSV to include only rows that belong to changed, published records.

## What the script does

1. **Selects input files**

   * Looks in `outputs/` for files named `YYYY-MM-DD_<source>_primary.csv` or `YYYY-MM_DD_<source>_primary.csv`.
   * Picks the most recent primary file as “new”.
   * Picks the second most recent primary file as “old”.

2. **Loads and normalizes**

   * Loads both CSVs as strings, trims whitespace, drops blank `ID` values, and de-duplicates by `ID`.
   * Accepts comma CSV and falls back to tab if needed.

3. **Builds the primary upload (`df`)**

   * Adds all rows from the newest file where `Resource Class == "Websites"`.
   * Adds rows that are present in “new” but not in “old” (newly added items).
   * Adds rows that are present in “old” but not in “new” (retired items) and sets:

     * `Publication State = "unpublished"`
     * `Date Retired = <today’s date>`
   * De-duplicates by `ID`.
   * Writes `outputs/YYYY-MM-DD_<source>_primary_upload.csv`.

4. **Builds the distributions upload**

   * Finds `outputs/YYYY-MM-DD_<source>_distributions.csv` for the same date as the newest primary file.
   * Loads it and keeps only rows whose `friendlier_id` matches `df["ID"]` where `df["Publication State"] == "published"`.
   * Preserves all matching rows, including multiple distributions per `ID`.
   * Writes `outputs/YYYY-MM-DD_<source>_distributions_upload.csv`.

## File naming and folder layout

* **Primary inputs** in `outputs/`:

  * `YYYY-MM-DD_<source>_primary.csv`
  * `YYYY-MM_DD_<source>_primary.csv`
* **Distributions inputs** in `outputs/`:

  * `YYYY-MM-DD_<source>_distributions.csv`
  * `YYYY-MM_DD_<source>_distributions.csv`
* **Outputs** in `outputs/` (dated with the day the script runs):

  * `YYYY-MM-DD_<source>_primary_upload.csv`
  * `YYYY-MM-DD_<source>_distributions_upload.csv`

## How change detection works

* The script uses a left join with an indicator to compare `ID` values between the “new” and “old” primary dataframes.

  * **New items** are `ID` values present in “new” and absent from “old”.
  * **Retired items** are `ID` values present in “old” and absent from “new”.
* A row that exists in both files with the same `ID` is considered unchanged and is omitted, unless it is a “Websites” row from the newest file, which is always included by design.

## Configuration

At the top of the script, set the source label:

```python
# e.g., "arcgis", "pasda", "ogm"
SOURCE = "arcgis"
```

Change this to reuse the script for another harvester. Only the `<source>` part of the filename changes.

## Assumptions

* Primary CSVs include an `ID` column.
* Primary CSVs include `Publication State` and `Date Retired` columns, or these will be created for retired rows.
* Primary CSVs include `Resource Class` if you want “Websites” rows always included.
* Distributions CSVs include a `friendlier_id` column that corresponds to the primary `ID`.
* The newest distributions CSV date matches the newest primary CSV date.

## Inputs and outputs

**Inputs**

* `outputs/YYYY-MM-DD_<source>_primary.csv` (newest)
* `outputs/YYYY-MM-DD_<source>_primary.csv` (second newest)
* `outputs/YYYY-MM-DD_<source>_distributions.csv` (same date as newest primary)

**Outputs**

* `outputs/YYYY-MM-DD_<source>_primary_upload.csv`
  Contains:

  * New items from the newest primary
  * Retired items from the previous primary with state updates
  * All “Websites” items from the newest primary
* `outputs/YYYY-MM-DD_<source>_distributions_upload.csv`
  Contains only distributions whose `friendlier_id` matches published `ID` values from the primary upload.

## Running the script

Place the script at `utils/arcgis_compare.py` and run:

```bash
python utils/arcgis_compare.py
```

The script discovers the appropriate input files in `outputs/` and writes the two upload CSVs back to `outputs/`.

## Troubleshooting

* **No differences detected**
  Confirm that the “old” file is the second most recent primary file and not the same as the newest. The script prints which files it selected.

* **No distributions upload found**
  Ensure that a distributions file exists for the same date as the newest primary file.

* **Missing columns**
  Ensure `ID` exists in both primary CSVs and `friendlier_id` exists in the distributions CSV.

* **Mismatched date separators**
  Both `YYYY-MM-DD` and `YYYY-MM_DD` are accepted. The script normalizes the date internally.
