# Curation

Python scripts replacing the original notebooks for the curation pipeline.

## Setup

Run setup once from the `harvest-operations` repository root:

```sh
uv sync --locked
```

Notes:

- Curation uses the repository root `.venv`, `pyproject.toml`, and `uv.lock`.
  It does not have a separate Python environment or lockfile.
- Python 3.12 is recommended. Python 3.13+ (including 3.14) may try to build `fiona` from source and require a local GDAL install.
- GDAL is required for `osgeo` bindings and for CLI tools like `ogr2ogr`.

### System dependencies

Install GDAL (includes `ogr2ogr`) before installing Python deps:

```sh
# macOS
brew install gdal

# Ubuntu/Debian
sudo apt-get install -y gdal-bin libgdal-dev
```

The ArcGIS curation pipeline uses GDAL command-line tools and does not require
Python `osgeo` bindings. A few retained legacy scripts do require those
bindings. Install a version matching the system library only when running those
scripts:

```sh
uv run --with "gdal==$(gdal-config --version)" python curation/scripts/<legacy-script>.py
```

## `arcgis_curation_pipeline`

`scripts/arcgis_curation_pipeline.py` is the staged workflow for archiving selected
ArcGIS Hub datasets. It imports the mapping, derivation, cleaning, and
validation rules from `harvesters/arcgis.py`, plus the shared schema and website
reference data from the parent repository.

The workflow deliberately has two phases:

1. Harvest selected DCAT records into an Aardvark CSV.
2. Stop for manual cleanup and explicit review confirmation.
3. Download, enrich, and derive files only after that confirmation.

The review confirmation records a SHA-256 checksum in `manifest.json`. If the
metadata CSV changes later, automated stages stop until it is reviewed and
confirmed again. Changes made by the pipeline's enrichment stage refresh the
recorded checksum.

### YAML input contract

Use `jobs/stpaul-2026.yaml` as the first working example. A job defines:

- a job ID and generated work directory;
- Hub name, landing page, DCAT endpoint, and a website reference ID from
  `reference_data/websites.csv`;
- an output CRS authority string for GDAL and its metadata URI;
- curation metadata defaults for `Code`, `Member Of`, and the export date;
- allowed vector resource types (`Polygon data`, `Line data`, and/or
  `Point data`);
- fields that must be populated at the manual-review checkpoint;
- one unique ArcGIS item/sublayer ID and filename per selected record;
- an optional source-specific `basic_theme` used in curated titles and
  descriptions (falling back to the DCAT title when omitted); and
- an optional PMTiles field-selection configuration.

The records list must not be empty. Record IDs and filenames must be unique.
Filenames may be supplied with or without `.gpkg` and cannot contain directory
paths. Numeric-looking codes containing underscores must be quoted in YAML
(for example, `code: "27_58000"`) so the YAML parser does not treat them as
numbers.

Service URLs are intentionally not entered in YAML. The metadata stage resolves
the matching DCAT records and records each current `ArcGIS GeoService` URL in
the generated manifest.

For this first iteration, an eligible record must pass the existing ArcGIS
harvester distribution filter, expose a vector FeatureServer or MapServer layer
through its `ArcGIS GeoService` distribution, and derive to one of the resource
types allowed by the YAML job. Raster/ImageServer curation remains outside this
pipeline.

### Start a new harvest

Copy the commented template and give the new batch its own job ID:

```sh
cp curation/jobs/arcgis_curation_pipeline_template.yaml \
  curation/jobs/<job-id>.yaml
```

Fill every value in angle brackets, set the export date, and add one `records`
block for each selected ArcGIS item or sublayer. In particular, verify:

- the Hub has a matching row in `reference_data/websites.csv` and enter its
  `ID` or `Code` as `website_reference_id`;
- the preferred CRS is available to GDAL/PROJ and has the correct public URI;
- `code` is quoted, especially when it contains underscores or hyphens;
- `basic_theme` contains the topic only, without place or date;
- filenames are final, unique, and omit the `.gpkg` extension; and
- the manual-review required fields are appropriate for the batch.

Validate the completed job before contacting the source:

```sh
uv run --locked python curation/scripts/arcgis_curation_pipeline.py \
  curation/jobs/<job-id>.yaml validate
```

Then replace `<job-id>` in the remaining commands below. Each job writes to its
own ignored `curation/work/<job-id>/` directory, so its manifest, review
checksum, downloads, and derivatives remain isolated from other harvests. Live
manifests use repository- and work-relative paths so they do not contain a
particular user's home directory.

### Run the Saint Paul example

Run commands from the `harvest-operations` repository root:

```sh
uv run --locked python curation/scripts/arcgis_curation_pipeline.py \
  curation/jobs/stpaul-2026.yaml validate

uv run --locked python curation/scripts/arcgis_curation_pipeline.py \
  curation/jobs/stpaul-2026.yaml metadata
```

The metadata command writes:

```text
curation/work/stpaul-2026/
├── manifest.json
└── metadata/
    └── metadata.csv
```

The browser page shows this full metadata CSV path directly in the Metadata
step. Each task card is driven by `manifest.json`: completed tasks have a green
button and card with a checkmark and completion time, the active task is amber,
and a failed browser run is red. The Postprocess card is complete only when all
six of its component stages are complete.

Edit `metadata/metadata.csv` manually. The generated records use the ArcGIS
harvester rules with these curation overrides:

- `Provider`: `BTAA-GIN`
- `Resource Class`: `Datasets`
- `Publication State`: `draft`
- `Format`: `GeoPackage`
- `Display Note`: the archived-copy warning containing the configured Hub name
  and landing page
- `Coordinate Reference System`: the configured CRS URI
- `Code`: the configured urban base layer code
- `Member Of`: the configured collection ID
- `ID`: `b1g_` followed by a stable 12-character alphanumeric Nano ID
- `Provenance`: `Exported from {web service link} as GeoPackage on {date}.`
- `Source`: blank
- `Is Part Of`: blank
- `Harvest Workflow`: `curation_datasets`
- `Title`: `{basic theme} [{controlled place}] {yyyy}`, using the first year in
  `Temporal Coverage`
- `Description`: begins `Historical dataset of {basic theme} in {readable
  place} as of {yyyy}.`, followed by the cleaned source description

`Resource Type` and `Bounding Box` are not required at this checkpoint because
the later enrichment stage derives them from the selected service layer.

After reviewing the CSV, record the checkpoint:

```sh
uv run --locked python curation/scripts/arcgis_curation_pipeline.py \
  curation/jobs/stpaul-2026.yaml review --confirm
```

Run the remaining stages together:

```sh
uv run --locked python curation/scripts/arcgis_curation_pipeline.py \
  curation/jobs/stpaul-2026.yaml postprocess
```

`postprocess` performs the following operations in order:

1. Pages each FeatureServer layer, downloads GeoJSON in EPSG:4326, and uses
   `ogr2ogr` to create the named GeoPackage in the configured preferred CRS.
2. Derives controlled `Resource Type`, decimal-degree `Bounding Box`,
   bbox geometry, and centroid values from the service layer.
3. Writes one field-level data dictionary per layer, including ArcGIS coded
   value domains when present.
4. Embeds selected CSV metadata into each GeoPackage using the existing QGIS
   metadata template.
5. Renders a PNG thumbnail from each GeoPackage.
6. Calls `build_pmtiles_from_gpkg.py` to create EPSG:4326 FlatGeoBuf and PMTiles
   derivatives and a build report.

Each operation is also available as an individual command: `download`,
`enrich`, `dictionaries`, `embed`, `thumbnails`, and `derivatives`. Use
`status` to print the manifest, and use `--overwrite` with `download`,
`derivatives`, or `postprocess` when generated outputs should be replaced.
Without `--overwrite`, Download skips GeoPackages that already exist and
continues downloading any missing records. This supports adding records to an
existing job without rebuilding its earlier GeoPackages.

Generated work is kept under `curation/work/` and ignored by Git. The YAML job
definitions remain versioned inputs. After Postprocess, save a portable run
record from the browser or command line:

```sh
uv run --locked python curation/scripts/arcgis_curation_pipeline.py \
  curation/jobs/stpaul-2026.yaml snapshot
```

The snapshot writes only small, versionable files:

```text
curation/run_records/stpaul-2026/<UTC-run-id>/
├── job.yaml
├── manifest.json
└── metadata.csv
```

The saved manifest lists every generated artifact using a work-relative path,
file size, and SHA-256 checksum. It does not copy GeoPackages, FlatGeoBuf,
PMTiles, thumbnails, dictionaries, or reports out of the ignored work
directory. Hashing large artifacts may take some time, but it provides a record
of the exact files produced by the run.

Commit `curation/jobs/` and `curation/run_records/` to GitHub. Keep
`curation/work/` ignored. A typical shared workflow is to pull before starting
a job, run and save the record, then commit and push the job YAML and new run
record. Avoid having two people generate new IDs for the same job concurrently.

### Generated IDs and checksums

Generated `b1g_` IDs are stable across metadata reruns as long as the ArcGIS
source ID is unchanged. The metadata stage first searches saved run records,
then lets the current work manifest override them. It generates a new
12-character NanoID only when neither source contains that ArcGIS source ID.
Adding records therefore does not change IDs for records already in the job.
Because all saved records are searched newest-first, an ID can still be
recovered when a previously removed record is added back later.

Rerunning Metadata replaces `metadata/metadata.csv`, resets manual review to
pending, and clears downstream stage completion because the generated input
must be reviewed again. It preserves existing IDs before replacing the
manifest. Keep `manifest.json` with the working job; deleting it, corrupting it,
or moving to another computer does not affect ID reuse once at least one run
record has been saved and synced. Changing the job ID starts a separate run
history.

The workflow uses two SHA-256 checksums for different safeguards:

- The manual-review checksum records the exact bytes of `metadata.csv` when
  review is confirmed. Post-review stages stop if the CSV changes. The Enrich
  stage intentionally changes the CSV and refreshes this checksum itself.
- The job-configuration checksum connects the manifest to the YAML used for
  the metadata harvest. If the YAML changes, the workflow requires Metadata to
  run again before review or postprocessing. The browser editor also uses a
  YAML content checksum to prevent overwriting changes made in another editor.

### Run the pipeline from the harvest dashboard

Start the FastAPI harvest dashboard from the repository root:

```sh
./start-fastapi.command
```

Open `http://localhost:8000/`, then choose **ArcGIS Curation Pipeline** under
**Other Harvesters**. The page discovers runnable YAML files in
`curation/jobs/`; it deliberately omits
`arcgis_curation_pipeline_template.yaml`.

From the browser page you can:

- create a new job by entering a safe job ID and copying the canonical YAML
  template;
- edit and save existing job YAML directly in the browser;
- validate the selected YAML job;
- harvest metadata;
- explicitly confirm the manual CSV review checkpoint;
- run `download`, `enrich`, `dictionaries`, `embed`, `thumbnails`, and
  `derivatives` separately; or
- run all six post-review stages with **Run all postprocess tasks**; and
- select **Save Run Record** to create a Git-friendly provenance snapshot after
  Postprocess completes.

The live log is streamed to the browser. The dashboard launches each task in
the `curation` uv project so it uses the same geospatial dependencies as the
command-line workflow. The metadata CSV still needs to be opened and edited in
an external editor before confirming review in the browser.

The browser editor leaves `arcgis_curation_pipeline_template.yaml` read-only.
Creating a job replaces its `<job-id>` placeholders and writes a new
`curation/jobs/<job-id>.yaml`. Saves check YAML syntax and require `job.id` to
match the filename. They also use a content checksum to prevent overwriting a
file that changed in another editor after it was loaded. Saving a partially
filled template is allowed; run **Validate** when it is complete to check all
pipeline-specific requirements.

## Embed QGIS Metadata in GeoPackages

Use `scripts/embed_qgis_metadata.py` to write QGIS-style XML metadata into every
GeoPackage in a directory. The script reads one CSV row per GeoPackage, renders
`scripts/templates/qgis-metadata.xml` with values from that row, fills the CRS
and bounding box from the GeoPackage itself, and stores the XML in the
GeoPackage metadata extension tables.

For the Milwaukee urban base layers:

```sh
uv run --locked python curation/scripts/embed_qgis_metadata.py \
  mke-ubl \
  mke-ubl/b1g_55-53000_primary.csv
```

The third positional argument is optional and can point to a different XML
template:

```sh
uv run --locked python curation/scripts/embed_qgis_metadata.py \
  path/to/geopackages \
  path/to/metadata.csv \
  path/to/qgis-metadata.xml
```

### CSV Structure

The default match column is `filename`. Each value in that column must exactly
match a GeoPackage filename in the target directory, including the `.gpkg`
extension:

```csv
filename,Title,Description,ID,Date Range,Theme,Provenance,Rights,Source
mke_boundary_2026.gpkg,Municipal boundary [Wisconsin--Milwaukee] {2026},...,b1g_5XPUIjJ9q7Z8,2026-2026,Boundaries,...,...,...
```

The script accepts OpenGeoMetadata-style CSVs like
`mke-ubl/b1g_55-53000_primary.csv`. Extra columns are ignored unless the XML
template references them.

Use `--match-column` if the filename is stored in a different column:

```sh
uv run --locked python curation/scripts/embed_qgis_metadata.py \
  path/to/geopackages \
  path/to/metadata.csv \
  --match-column "Identifier"
```

The match column must be unique. Blank match values are ignored, and duplicate
values stop the run with an error.

### XML Template Tokens

The default template is `scripts/templates/qgis-metadata.xml`. Any text or
attribute value in the template can include tokens in braces. Tokens are
case-insensitive CSV column names:

```xml
<identifier>{ID}</identifier>
<title>{Title}</title>
<abstract>{Description}</abstract>
<rights>{Rights}</rights>
```

The default template currently uses these CSV columns:

- `ID`
- `Source`
- `Title`
- `Description`
- `Theme`
- `Provenance`
- `Rights`
- `Date Range`

Two special token forms are available for range-like fields:

```xml
<start>{Date Range first value}</start>
<end>{Date Range last value}</end>
```

For a value like `2024-2026`, the first token resolves to `2024` and the last
token resolves to `2026`. The resolver first looks for four-digit years. If no
years are present, it splits on `|`, `;`, `,`, or `/`.

The special `{now}` token resolves to the current date in `YYYY-MM-DD` format.

### GeoPackage Behavior

For each matched GeoPackage, the script:

- reads the first feature table in `gpkg_contents` to get the extent and SRS;
- replaces the template `<crs><spatialrefsys>` block with values from
  `gpkg_spatial_ref_sys`;
- replaces the template `<extent><spatial>` attributes with the GeoPackage
  bounding box;
- drops and recreates existing `gpkg_metadata` and
  `gpkg_metadata_reference` tables;
- inserts one dataset metadata record and references it from every feature table
  in the GeoPackage;
- refreshes `gpkg_extensions` rows for the GeoPackage metadata extension when
  `gpkg_extensions` exists.

Unmatched GeoPackages are skipped and left unchanged. Metadata rows that do not
match any GeoPackage are reported at the end of the run.

## Build PMTiles from GeoPackages

Use `build_pmtiles_from_gpkg.py` to recursively convert GeoPackage vector layers
to EPSG:4326 FlatGeoBuf files, then to PMTiles with Tippecanoe. The script
supports multi-layer GeoPackages, configurable field dropping, resumable runs,
and CSV or JSON reports.

Install the required command-line tools first:

```sh
brew install gdal tippecanoe
```

Start with a field inventory report so you can review large attribute tables:

```sh
uv run --locked python curation/scripts/build_pmtiles_from_gpkg.py \
  --input-dir ./gpkg \
  --fgb-dir ./fgb \
  --pmtiles-dir ./pmtiles \
  --config pmtiles_config.json \
  --report pmtiles_build_report.csv \
  --field-report-only
```

Copy `pmtiles_config.sample.json` to `pmtiles_config.json`, then edit layer
rules and field keep/drop settings.

Run the conversion:

```sh
uv run --locked python curation/scripts/build_pmtiles_from_gpkg.py \
  --input-dir ./gpkg \
  --fgb-dir ./fgb \
  --pmtiles-dir ./pmtiles \
  --config pmtiles_config.json \
  --report pmtiles_build_report.csv
```

Rerun without rebuilding completed outputs:

```sh
uv run --locked python curation/scripts/build_pmtiles_from_gpkg.py \
  --input-dir ./gpkg \
  --fgb-dir ./fgb \
  --pmtiles-dir ./pmtiles \
  --config pmtiles_config.json \
  --report pmtiles_build_report.csv \
  --skip-existing
```
