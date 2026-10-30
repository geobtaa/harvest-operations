The `scripts` folder contains standalone python scripts that are not integrated into the harvester modules.

`run_arcgis_review.py` runs the regular ArcGIS harvest pipeline for only the portals
in a supplied CSV, with upload generation disabled. From the repository root:

```bash
uv run python -m scripts.run_arcgis_review \
  --input-csv inputs/harvest-workflow-inputs/py-arcgis-hub-ca.csv
```

To review the entire Minnesota Geospatial Commons group with catalog-parent
titles, select its harvest record from the regular workflow input:

```bash
uv run python -m scripts.run_arcgis_review \
  --input-csv inputs/harvest-workflow-inputs/py-arcgis-hub.csv \
  --harvest-id harvest_05a-01
```

This fetches all eligible datasets from that one portal. It uses a temporary
filtered input without changing the workflow CSV and requires exactly one
matching ID. Selected-site filenames include `_harvest_05a-01` before
`_primary.csv`, `_distributions.csv`, or `_report.csv` to separate them from
whole-workflow reviews. These are complete review outputs; they are not upload
deltas for updating already-published records.

For this input it writes `outputs/YYYY-MM-DD_review_py-arcgis-hub-ca_primary.csv`,
`outputs/YYYY-MM-DD_review_py-arcgis-hub-ca_distributions.csv`, and
`reports/arcgis/review/YYYY-MM-DD_review_py-arcgis-hub-ca_report.csv`.
The primary and distributions CSVs are the metadata to review; the report shows
fetch outcomes and counts compared with the existing registry (read only).
No registry is created or updated, no upload files are built, and no records are
published. Regular ArcGIS output files and configuration are preserved. Repeating
the same review input on the same day replaces that day's review files.
The output retains the regular pipeline's publication-state values; generating
these local CSVs does not publish them. Keep them for review until the colleague
has selected which portals to publish.

ArcGIS duplicate IDs retain the entire row from the preferred Series `Code`,
using the order in `duplicate_id_code_priority` in `config/arcgis.yaml`.
Unlisted codes rank after the configured codes. Ties retain the
first encountered row. This applies to both regular and review ArcGIS harvests;
only one version of each ID and its distribution links is retained.

ArcGIS titles also include `(sublayer of Parent Title)` when the same feed
contains an explicit parent with the same ArcGIS item ID and a matching root
FeatureServer/MapServer URL. For example:
`C18 2026 (sublayer of Lassen 2026 Roll Year) [California]`.
Parents are checked before dataset filtering, but do not make additional records
eligible for harvest. Missing, ambiguous, or unrelated parents leave titles
unchanged, as do titles already containing the parent wording. This uses catalog
metadata without additional service requests. Set `parent_sublayer_context: false`
in `config/arcgis.yaml` to disable it for regular and review runs.

`oai_pmh_pipeline.py` runs the validated, source-scoped OAI-PMH stages used by
the browser UI: Identify, ListSets discovery, atomic XML download, and output
building. `oai_download.py` remains available as the lower-level acquisition
CLI for downloading raw XML snapshots directly.

`oai_list_sets.py` queries an OAI-PMH `ListSets` endpoint, filters the set list by one or more
keywords such as `atlas` or `plat book`, and can write a CSV of discovered set specs.

Dashboard scripts now live in [`dashboard/`](../dashboard/). The dashboard generator
builds due-date views from `inputs/harvest-records.csv` and `reference_data/websites.csv`,
writes per-workflow CSVs under `inputs/harvest-workflow-inputs/`, and is wired into the
FastAPI job UI.

`arcgis_landing_page_thumbnails.py` scans ArcGIS Hub landing pages from
`inputs/arcgisLandingPages.csv` and writes thumbnail URLs to
`outputs/arcgis_landing_page_thumbnails.csv`.

`inventory_gdrs.py` inventories locally stored dataset artifacts under a GDRS
`data/pub` tree, excludes external/service-only resources, and extracts title,
originator, and publication date from FGDC XML metadata. Run it with the default
December 2017 collection and output path using:

```bash
python scripts/inventory_gdrs.py
```

An alternate archive root (or `pub` directory) and CSV path can be supplied as:

```bash
python scripts/inventory_gdrs.py /path/to/GDRS -o outputs/gdrs_inventory.csv
```

To write a second inventory containing only the preferred available format for
each resource, use `--one-format-per-resource`. The priority is File
Geodatabase, Shapefile, KML/KMZ, GeoJSON, then CSV. Resources available only in
another recognized format are retained using that format.

```bash
python scripts/inventory_gdrs.py --one-format-per-resource
```

`compare_gdrs_archives.py` matches resources between two GDRS snapshots using
`resourceGUID`, then `publisherID` plus `baseName`, then the resource path. It
compares the preferred dataset format and uses SHA-256 to catch same-size
content changes. The complete comparison uses `new`, `changed`, `unchanged`,
and `removed` statuses; removed resources stay out of the new/changed-only
archive candidate CSV:

```bash
python scripts/compare_gdrs_archives.py \
  inputs/GDRS-December-2017 inputs/GDRS-January-2026
```

For a faster preliminary report that compares formats, filenames, and sizes
without reading all dataset bytes, add `--method size`.
