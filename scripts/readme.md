The `scripts` folder contains standalone python scripts that are not integrated into the harvester modules.

`oai_download.py` downloads raw OAI-PMH XML into a local folder so parser development can happen offline.

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
