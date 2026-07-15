# ArcGIS Harvester

The ArcGIS harvester gathers dataset records from configured ArcGIS Hub DCAT
endpoints, maps them into the project metadata schema, appends updated harvest
record rows, and writes primary and distribution CSV outputs.

## Main Pipeline

The harvester follows the shared `BaseHarvester` pipeline. The ArcGIS-specific
class methods are limited to the same template methods defined in
`harvesters/base.py`.

- `__init__`
  - Enables `build_uploads` by default unless the config explicitly disables it.
  - Enables `use_registry` by default unless the config explicitly disables it.
  - Stores the workflow input CSV path.
  - Stores the ArcGIS hub metadata defaults CSV path.
  - Stores the ArcGIS primary and distribution registry CSV paths.

- `load_reference_data`
  - Runs the base reference-data loading.
  - Loads website metadata defaults into a lookup table.
  - Uses `Code`, `Identifier`, and `ID` values to match harvest records to
    website defaults.

- `fetch`
  - Reads each configured harvest record from the ArcGIS input CSV.
  - Matches each harvest record to its website metadata defaults.
  - Requests the row's `Endpoint URL` as DCAT JSON.
  - Yields status messages for logging and structured harvested records for
    downstream processing.

- `flatten`
  - Expands each fetched ArcGIS Hub catalog into one item per dataset.
  - Preserves three pieces of context for each item:
    - The source harvest-record row.
    - The matched website metadata defaults.
    - The individual dataset resource from the DCAT feed.

- `build_dataframe`
  - Converts flattened records to a dataframe.
  - Uses a pipe-based sequence of ArcGIS transforms:
    - Filters unsupported or placeholder dataset rows.
    - Maps ArcGIS and workflow fields into the target schema.
    - Extracts ArcGIS service URLs into distribution columns.

- `derive_fields`
  - Runs the shared base derived-field logic first.
  - Uses a pipe-based sequence of ArcGIS transforms:
    - Parses raw ArcGIS identifiers into `Identifier` and `ID`.
    - Infers temporal coverage.
    - Builds normalized date ranges.
    - Computes bounding boxes from dataset spatial fields, with hub defaults as
      fallback values.
    - Cleans creator values.
    - Reformats titles with place labels.
    - Sets resource types from ArcGIS-specific keyword matches.

- `add_defaults`
  - Applies base defaults.
  - Sets ArcGIS defaults for fields such as `Display Note`, `Language`, and
    `Resource Class`.

- `add_provenance`
  - Adds harvest date provenance to dataset rows.
  - Reads harvest-record rows from the workflow input CSV.
  - Updates only `Last Harvested` on those harvest-record rows.
  - Appends the updated harvest-record rows to the primary dataframe.

- `clean`
  - Delegates to the shared base cleanup pipeline.
  - Runs the common dataframe and spatial cleanup behavior.

- `validate`
  - Delegates to the shared base validation pipeline.
  - Checks the final dataframe before output writing.

- `write_outputs`
  - Builds the secondary distributions table from the primary dataframe.
  - Delegates dated CSV writing to the base harvester.
  - Writes the ArcGIS harvest report CSV.

- `build_uploads`
  - When `use_registry` is enabled, compares the current local output files to
    the ArcGIS registry CSVs in `registry/`.
  - Writes upload-ready CSVs for new records, retired records, new distribution
    rows, and deleted distribution rows.
  - Updates the registry CSVs after upload deltas are built.
  - Falls back to the shared base upload builder only when `use_registry` is
    disabled.

## Inputs

- Workflow input CSV
  - Config key: `input_csv`
  - Default path:
    `inputs/harvest-workflow-inputs/py-arcgis-hub.csv`
  - Contains the hub records to fetch, including endpoint URLs and workflow
    metadata.

- Hub metadata defaults CSV
  - Config key: `hub_metadata_csv`
  - Default path: `reference_data/websites.csv`
  - Supplies stable website metadata and fallback values for
    child dataset records. The harvester loads the full websites reference
    table and matches ArcGIS harvest records by `Code`, `Identifier`, or `ID`.

- ArcGIS DCAT endpoint
  - Read from each workflow row's `Endpoint URL`.
  - Expected to return JSON with a `dataset` list.

- Primary registry CSV
  - Config key: `primary_registry_csv`
  - Default path: `registry/arcgis_primary_registry.csv`
  - Git-trackable previous-run state for current primary records. It keeps
    compact citation fields for ArcGIS records, including `Title`,
    `Alternative Title`, `Creator`, `Publisher`, `Resource Class`,
    `Temporal Coverage`, `Date Issued`, `Date Accessioned`, `ID`,
    `Identifier`, and `Code`.

- Distributions registry CSV
  - Config key: `distributions_registry_csv`
  - Default path: `registry/arcgis_distributions_registry.csv`
  - Git-trackable previous-run state for distribution rows. It stores
    `friendlier_id`, `reference_type`, `distribution_url`, and `label`.

## Outputs

- Primary CSV
  - Config key: `output_primary_csv`
  - Contains harvested dataset rows and appended harvest-record rows.

- Distributions CSV
  - Config key: `output_distributions_csv`
  - Built from service URL columns such as `featureService`, `mapService`,
    `imageService`, and `tileService`.

- Harvest report CSV
  - Config key: `output_report_csv`
  - Contains one row per harvest record with code, title, identifier, harvest
    run status/message, total records found, new records, and unpublished
    records.
  - Includes a final `TOTAL` row with run and record tallies.

- Upload delta files
  - Built automatically when `build_uploads` is enabled and the ArcGIS primary
    registry exists.
  - Written to `outputs/to_upload/`.
  - Include updated harvest-record rows from the current run.
  - Include new dataset rows from the current run.
  - Include retired dataset rows constructed from the primary registry.
  - Include distribution add/delete files constructed by comparing the current
    distributions output to the distributions registry.

- Updated registry CSVs
  - The harvester updates `registry/arcgis_primary_registry.csv` and
    `registry/arcgis_distributions_registry.csv` after building upload deltas.
  - The registries are rewritten as current snapshots, so records and
    distribution rows missing from the current harvest are pruned.
  - `Date Accessioned` is preserved from the existing primary registry.
  - Descriptive fields such as title, creator, publisher, temporal coverage,
    and identifier are refreshed from the current harvest for active records.

## ArcGIS-Specific Behavior

- Dataset filtering
  - Keeps records with supported distribution titles, currently `Shapefile`.
  - Keeps records with supported `ArcGIS GeoService` URL patterns, currently
    `ImageServer`.
  - Drops placeholder titles and malformed distribution lists.

- Identifier handling
  - Parses ArcGIS Hub query-string identifiers.
  - Uses the `id` query parameter as the stable item ID.
  - Appends the `sublayer` value to the ID when present.
  - Builds canonical `https://hub.arcgis.com/datasets/{id}` identifiers.

- Distribution extraction
  - Detects ArcGIS GeoService URLs.
  - Maps service URL patterns to output columns and format labels:
    - `FeatureServer` to `featureService` and `ArcGIS FeatureLayer`.
    - `MapServer` to `mapService` and `ArcGIS DynamicMapLayer`.
    - `ImageServer` to `imageService` and `ArcGIS ImageMapLayer`.
    - `TileServer` to `tileService` and `ArcGIS TiledMapLayer`.

- Harvest-record rows
  - Read directly from the workflow input CSV.
  - Appended to the primary output after setting `Last Harvested` to the current
    run date.
  - Included in the primary upload delta file on each run.
  - No parent website records are updated by this harvester.

- Registry-based retirement
  - Active registry records missing from the current harvest are treated as
    retired.
  - Retired upload rows preserve compact citation fields from the registry.
  - Retired upload rows set `Publication State` to `unpublished`,
    `Access Rights` to `Public`, and `Resource Class` to `Web services`.
  - Retired records are removed from the primary registry after the upload
    delta is written, so they are not repeatedly emitted as newly retired on
    later runs.

## Code Organization

- Reusable helpers live in `utils/harvester_helpers.py`.
  - `first_non_empty`
  - `read_csv_rows`

- ArcGIS-only helpers live at the end of `harvesters/arcgis.py` under:

```python
# Custom functions for this harvester
```

- The harvester class calls those helper functions from base-template methods,
  usually through `DataFrame.pipe(...)` where the step is a dataframe transform.
