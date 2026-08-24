# Socrata curation pipeline reference

The Socrata workflow mirrors the staged ArcGIS curation pipeline but replaces
ArcGIS service queries with Socrata DCAT, view metadata, and SODA2 endpoints.
It reuses `harvesters/socrata.py` for the initial Aardvark mapping and uses the
same manual-review, GeoPackage metadata, thumbnail, derivative, ZIP, and run
record stages as the ArcGIS workflow.

## Why the pipeline uses SODA2 GeoJSON pages

Socrata's SODA endpoints return 1,000 rows by default. The SODA2 endpoint
supports `$limit` and `$offset`; SODA2 allows at most 50,000 rows per request,
while 2.1 and 3.0 have no such maximum. Large responses can still time out, so
the pipeline defaults to ordered 10,000-row pages:

```text
https://<portal>/resource/<id>.geojson
  ?$limit=10000
  &$offset=0
  &$order=:id
```

The pipeline queries `count(*)` before downloading, appends each page to a
temporary GeoPackage with `ogr2ogr`, then repeats the count and source revision
checks before promoting the completed file. A changed dataset or a short page
fails the stage instead of silently creating an incomplete archive.

The DCAT record may advertise a SODA3 GeoJSON export. SODA3 is a good option
for interactive and token-backed clients, but its current query/export API
requires authentication or an application token. Ordered SODA2 pages remain a
simple anonymous default for public curation jobs. Set
`download.app_token_environment` to the name of an environment variable when
an application token is available; the secret is not stored in YAML or the
manifest.

See Socrata's documentation for [paging past 1,000 rows](https://support.socrata.com/hc/en-us/articles/202949268-How-to-query-more-than-1000-rows-of-a-dataset),
the [`LIMIT` clause](https://dev.socrata.com/docs/queries/limit.html), and
[SODA3 query and export endpoints](https://dev.socrata.com/docs/queries/).

## Bloomington example

`bloomington-socrata-2026.yaml` selects TreeKeeper Inventory (`ndfd-h5qf`) from the
City of Bloomington portal. The existing website record is `01c-01`, the
urban-base-layer harvest record is `b1g_18_05860`, and the target CRS is
NAD83 / Indiana West (ftUS), EPSG:2966.

Run commands from the repository root:

```sh
uv run --locked python curation/scripts/socrata_curation_pipeline.py \
  curation/jobs/socrata/bloomington-socrata-2026.yaml validate

uv run --locked python curation/scripts/socrata_curation_pipeline.py \
  curation/jobs/socrata/bloomington-socrata-2026.yaml metadata
```

The metadata stage:

1. Selects `ndfd-h5qf` from the portal's existing `data.json` catalog.
2. Passes that DCAT resource through the existing Socrata harvester.
3. Verifies that `/api/views/ndfd-h5qf` has a supported geometry column.
4. Records the source revision and exact SODA2 row count in `manifest.json`.
5. Writes `curation/work/bloomington-socrata-2026/metadata/metadata.csv`.

The job defines its filename prefix and suffix once:

```yaml
file_naming:
  city_abbreviation: blm
  download_year: "2026"
```

Each selected record supplies only its filename theme:

```yaml
records:
  - id: ndfd-h5qf
    filename_theme: trees
    basic_theme: Trees
```

The pipeline assembles this as `blm_trees_2026.gpkg`. The download year must
match the year in `metadata.export_date`; `temporal_year` remains independent
because it describes the data rather than the archive download.

Edit the CSV, especially creator, description, rights, title, and other
curatorial fields. Then record the checksum-protected review checkpoint:

```sh
uv run --locked python curation/scripts/socrata_curation_pipeline.py \
  curation/jobs/socrata/bloomington-socrata-2026.yaml review --confirm
```

Run the remaining stages together:

```sh
uv run --locked python curation/scripts/socrata_curation_pipeline.py \
  curation/jobs/socrata/bloomington-socrata-2026.yaml postprocess
```

`postprocess` downloads every row, projects the GeoPackage, derives geometry
and bounds from the completed file, writes the Socrata data dictionary, embeds
metadata, creates a thumbnail, builds FlatGeoBuf and PMTiles derivatives, and
creates an upload ZIP. Each operation is also available separately as
`download`, `enrich`, `dictionaries`, `embed`, `thumbnails`, `derivatives`, or
`zip`. Use `--overwrite` only with the commands that expose it.

The dictionary stage reads every `columns` entry from `/api/views/<id>`:

- `fieldName` becomes `field_name` because it matches the downloaded data;
- `dataTypeName` becomes `field_type`;
- the public display name and description become `definition`; and
- enumerated values are retained when Socrata explicitly supplies them.

Cached top values are deliberately excluded because they are samples, not a
controlled value domain.

After all postprocess stages complete, save the same portable, Git-friendly
run record used by the ArcGIS pipeline:

```sh
uv run --locked python curation/scripts/socrata_curation_pipeline.py \
  curation/jobs/socrata/bloomington-socrata-2026.yaml snapshot
```

## Starting another Socrata job

Copy `socrata_curation_pipeline_template.yaml`, choose a unique job ID, and
replace each angle-bracket value. Socrata IDs must use the four-by-four form
`abcd-1234`. Set `file_naming.city_abbreviation` and
`file_naming.download_year` once, then give each record a unique
`filename_theme`. Constructed output filenames must be unique. The source must
expose a point, line, polygon, or equivalent multi-geometry column.

The configured page size must be between 1 and 50,000. Ten thousand is a good
starting point; lower it for very wide datasets or unreliable connections.
