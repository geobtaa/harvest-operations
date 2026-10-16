# ArcGIS Harvester

The ArcGIS harvester scans the configured ArcGIS Hub DCAT feeds, keeps supported
web-service datasets, normalizes them into the GeoBTAA schema, and prepares the
files needed for review and upload.

## Quick start

1. Start the local application and open the Harvest Task Dashboard.
2. Run the dashboard source-CSV task. This creates
   `inputs/harvest-workflow-inputs/py-arcgis-hub.csv` from the current harvest
   records.
3. Review that workflow CSV and confirm it contains every hub intended for the
   run. An incomplete list can make valid registry records appear retired.
4. Preserve or commit the current ArcGIS registry files before running:
   - `registry/arcgis_primary_registry.csv`
   - `registry/arcgis_distributions_registry.csv`
5. Open the ArcGIS Harvester page and start a full run.
6. Review the streamed messages, the report's error rows, and all new/retired
   counts before uploading anything.
7. Run the Harvest Task Dashboard reports task after the harvest report CSV is
   present.
8. Commit the dated ArcGIS report, updated registries, and regenerated dashboard
   reports together.

The browser page is `/static/arcgis.html`. The workflow name used by the
dashboard is `py_arcgis_hub`; its filename form is `py-arcgis-hub`.

## Required inputs

| Input | Default path | Purpose |
| --- | --- | --- |
| Workflow input | `inputs/harvest-workflow-inputs/py-arcgis-hub.csv` | Lists the ArcGIS harvest records and DCAT endpoint URLs to fetch. |
| Website defaults | `reference_data/websites.csv` | Supplies stable website metadata and fallback values matched by `Code`, `Identifier`, or `ID`. |
| Primary registry | `registry/arcgis_primary_registry.csv` | Stores the previous active primary-record snapshot used to identify new and retired records. |
| Distribution registry | `registry/arcgis_distributions_registry.csv` | Stores the previous active distribution snapshot used to identify added and deleted links. |

The paths are configured in `config/arcgis.yaml`. Each workflow row's
`Endpoint URL` must return DCAT JSON with a `dataset` list. The harvester stops
before fetching if the same normalized endpoint URL is assigned to multiple
harvest codes.

## Outputs to verify

A full run writes these dated files:

| Output | Path pattern | Purpose |
| --- | --- | --- |
| Primary | `outputs/YYYY-MM-DD_arcgis_primary.csv` | Harvested dataset rows plus harvest-record rows whose `Last Harvested` value was updated. |
| Distributions | `outputs/YYYY-MM-DD_arcgis_distributions.csv` | Landing-page, download, and ArcGIS service links derived from the primary output. |
| Harvest report | `reports/arcgis/YYYY-MM-DD_arcgis_report.csv` | One status/count row per hub and a final `TOTAL` row. This is the dashboard's source for ArcGIS run history. |
| Primary upload | `outputs/to_upload/YYYY-MM-DD_arcgis_primary_upload.csv` | Updated harvest records, new dataset records, and records to unpublish. |
| New distributions | `outputs/to_upload/YYYY-MM-DD_arcgis_distributions_new.csv` | Distribution rows to add. |
| Deleted distributions | `outputs/to_upload/YYYY-MM-DD_arcgis_distributions_delete.csv` | Distribution rows to delete. |

The run also rewrites both registry CSVs as current active snapshots after it
builds the upload deltas. Review those changes before committing them.

### Reading the harvest report

Each hub row includes:

- `Harvest Run` and `Harvest Message` for the request outcome;
- `Total Records Found` from the hub's DCAT response;
- `New Records` and `Unpublished Records`, calculated by comparing the current
  primary output with the shared pre-run primary registry.

The final `TOTAL` row summarizes successful/failed hubs and the record counts.
An endpoint error does not stop the remaining hubs from running, so inspect the
error rows even when the overall process reaches completion.

## Dashboard reporting and publishing

The ArcGIS report archive and Harvest Task Retrospective discover runs from
files in `reports/arcgis/` whose names exactly match:

```text
YYYY-MM-DD_arcgis_report.csv
```

They do **not** infer a completed harvest from
`registry/arcgis_primary_registry.csv` or
`registry/arcgis_distributions_registry.csv`.

After verifying a harvest:

1. Confirm the dated report CSV exists in `reports/arcgis/`.
2. Run the Harvest Task Dashboard reports task using
   `config/harvest-task-reports.yaml`, or run the complete dashboard job.
3. Confirm the retrospective contains an `ArcGIS Hubs Harvest Report` entry for
   the run date.
4. Confirm a dated file such as
   `reports/YYYY-MM-DD_harvest-task-dashboard-py-arcgis-hub.html` was created.
5. Commit and push the source report and generated dashboard HTML. GitHub Pages
   publishes committed dashboard reports from `main`.

## Before uploading

Use the report and Git diff as a review gate:

- Investigate every hub with `Harvest Run = error`.
- Compare new and unpublished totals with the expected scope of the run.
- Treat a large or widespread retirement count as a possible incomplete input,
  source outage, filtering change, or failed endpoint—not automatically as a
  valid deletion event.
- Inspect all three upload delta CSVs.
- Review both registry diffs. The registries are updated locally when upload
  deltas are built, before any external upload occurs.
- If the run is invalid, do not upload its deltas. Restore the prior registries
  from version control after preserving any files needed for diagnosis.

## Test runs

The browser's test option selects three random rows from the normal workflow
input. It is useful for checking requests and transformations, but it is not a
production result.

The current test path uses the normal dated output/report names and runs the
normal registry comparison. Before a test run, preserve any same-day full
outputs and the registry files. After the test, do not commit its report,
upload deltas, or registry changes as though they represented a full harvest.

## Troubleshooting

### A harvest is absent from the retrospective or ArcGIS report archive

Check for `reports/arcgis/YYYY-MM-DD_arcgis_report.csv` on the branch where the
dashboard is being generated. Updating the registries alone does not create a
dashboard history entry.

If the report was committed on another branch, restore or copy that single file
onto the working branch, then rerun the dashboard reports task. Prefer restoring
the individual report over cherry-picking a commit that also contains unrelated
work. Commit the source report and regenerated dashboard HTML together.

Also verify the filename. A report under `outputs/`, an undated file, or a name
other than `YYYY-MM-DD_arcgis_report.csv` is ignored by dashboard discovery.

### The dashboard still shows an older run

- Confirm the new report is present in the current working tree and branch.
- Confirm its date is valid and zero-padded, for example `2026-10-09`.
- Rerun the dashboard **reports** task; committing only the source CSV does not
  regenerate existing HTML.
- Confirm the dated `py-arcgis-hub` HTML and retrospective HTML were updated.

### The workflow input is missing or stale

Run the Harvest Task Dashboard source-CSV task again, then inspect
`inputs/harvest-workflow-inputs/py-arcgis-hub.csv`. The harvester raises a
`FileNotFoundError` when this file is absent.

### The run stops before fetching

Look for the duplicate-endpoint error. One normalized `Endpoint URL` cannot be
assigned to multiple harvest codes. Correct the source harvest records, rebuild
the workflow input, and rerun.

### A hub reports an HTTP or JSON error

Open the URL from the report's `Harvest Message`. Common causes include an
expired or redirected DCAT URL, `403`, `404`, `500`, a timeout, or a response
that is not valid JSON. Correct the harvest record or retry after a temporary
source outage. Review proposed retirements associated with failed hubs before
uploading.

### Record totals are unexpectedly low or zero

The harvester intentionally keeps datasets that have either a `Shapefile`
distribution or an `ArcGIS GeoService` whose access URL contains
`ImageServer`. Placeholder titles and malformed distribution lists are dropped.
Check the source DCAT response and the filtering rules before assuming the hub
contains no records.

### New or unpublished counts look wrong

The report and upload deltas both compare against the Git-tracked registry.
Confirm the registry represented the previous complete run before starting the
harvest. If it had already been changed by a partial or test run, restore the
last authoritative registry version before rerunning.

### The browser stream disconnects

Check the server log and the dated output files before rerunning. A disconnected
browser does not prove the server process stopped, and rerunning on the same day
can overwrite same-named output files.

## Technical pipeline

`ArcGISHarvester` follows the template-method sequence defined by
`BaseHarvester.harvest_pipeline()`:

1. `load_reference_data` loads shared themes and builds the website-defaults
   lookup.
2. `fetch` requests each DCAT endpoint with a 30-second timeout, records a
   success/error report row, and continues after individual request failures.
3. `parse` uses the base passthrough because the fetched catalogs are already
   structured.
4. `flatten` expands each catalog into one source dataset per item while
   retaining its workflow row and matched website defaults.
5. `build_dataframe` filters supported items, maps source/workflow fields into
   the target schema, and extracts distribution URLs.
6. `derive_fields` applies shared themes, parses identifiers, infers temporal
   coverage/date ranges, calculates bounding boxes, cleans creators, formats
   titles, and matches resource types.
7. `add_defaults` applies website defaults and ArcGIS defaults such as
   `Language = eng` and `Resource Class = Web services`.
8. `add_provenance` adds the retrieval statement and appends workflow harvest
   records with the current `Last Harvested` date.
9. `clean` and `validate` run the shared dataframe, spatial, and validation
   pipelines.
10. `write_outputs` removes `Created At` and `Updated At`, builds the secondary
    distribution table, writes the dated full outputs, and writes the dated
    harvest report.
11. `build_uploads` compares current records with the registries, writes upload
    deltas, and replaces the registries with the current active snapshots.

## ArcGIS-specific transformations

### Identifiers

The harvester parses ArcGIS Hub query-string identifiers, uses the `id` query
parameter as the stable item ID, appends `sublayer` when present, and builds the
canonical identifier `https://hub.arcgis.com/datasets/{id}`.

### Distributions

ArcGIS GeoService URLs map to these output columns and labels:

| URL pattern | Output column | Label |
| --- | --- | --- |
| `FeatureServer` | `featureService` | `ArcGIS FeatureLayer` |
| `MapServer` | `mapService` | `ArcGIS DynamicMapLayer` |
| `ImageServer` | `imageService` | `ArcGIS ImageMapLayer` |
| `TileServer` | `tileService` | `ArcGIS TiledMapLayer` |

### Harvest-record rows

The workflow input rows are appended to the primary output with an updated
`Last Harvested` date and included in each primary upload delta. The harvester
does not update separate parent website records.

### Registry-based retirement

Current dataset IDs are compared with active IDs in the primary registry.
Missing IDs become upload rows with `Publication State = unpublished`,
`Access Rights = Public`, a retirement date, and a warning display note. The
retired records are then omitted from the rewritten current registry so they
are not emitted again on the next run. Existing `Date Accessioned` values are
preserved for records that remain active.

Distribution additions and deletions are calculated separately by comparing
the current distribution output with the distribution registry.

## Code organization

- The harvester class is in `harvesters/arcgis.py` and implements only methods
  from `BaseHarvester`.
- Reusable helpers such as `first_non_empty` and `read_csv_rows` live in
  `utils/harvester_helpers.py`.
- ArcGIS-only helpers are module-level functions below the class under
  `# Custom functions for this harvester`.
- Dataframe transformations are composed with `DataFrame.pipe(...)` where
  practical.
