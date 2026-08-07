# ArcGIS curation job reference

Copy `arcgis_curation_pipeline_template.yaml` to `<job-id>.yaml`, or create a
job from the ArcGIS Curation Pipeline dashboard. Fill every value in angle
brackets, set the export date, and delete any starter record that the Hub does
not provide.

Run commands from the `harvest-operations` repository root. Validate the job
before harvesting metadata:

```sh
uv run --locked python curation/scripts/arcgis_curation_pipeline.py \
  curation/jobs/<job-id>.yaml validate
```

## Job and Hub fields

- `job.id` is a unique, filesystem-friendly name for the batch. The dashboard
  fills it when creating a job.
- `job.work_directory` holds generated files. Paths are relative to the job
  YAML; the standard value creates one ignored directory per job.
- `provider` is the organization serving the archived datasets. Keep
  `BTAA-GIN` for this workflow.
- `hub.name` and `hub.landing_page` appear in the archived-copy display note.
- `hub.dcat_api` is the ArcGIS Hub DCAT-US 1.1 catalog containing the selected
  records.
- `hub.website_reference_id` is the Hub's `ID` or `Code` in
  `reference_data/websites.csv`.
- `hub.websites_csv` normally keeps the supplied repository-relative path.

## CRS and shared metadata

- `coordinate_reference_system.authority` is the GDAL/PROJ authority code used
  for the archived GeoPackages, such as `EPSG:3435`.
- `coordinate_reference_system.uri` is the matching public URI written to the
  Aardvark metadata.
- `metadata.code` is the urban base layer code shared by the batch. Keep it
  quoted so YAML preserves numeric-looking values containing underscores or
  hyphens.
- `metadata.member_of` normally remains `b1g_urbanBaseLayers`.
- `metadata.export_date` is the archive export date in `YYYY-MM-DD` form.

## Selection, review, and derivatives

`selection_criteria.allowed_resource_types` lists the vector geometries that
the job may accept. Delete a type only when it should be rejected for the
batch.

`manual_review.required_fields` controls the metadata review checkpoint. The
pipeline stops when any listed CSV field is blank. Add or remove fields when a
batch has different acceptance criteria.

`derivatives.pmtiles_config` points to the PMTiles field-selection rules. Copy
the sample configuration and change this path when layers need source-specific
field filtering.

## Records

The template includes common urban base layer themes. For each layer that is
available, fill these fields:

- `id`: the ArcGIS item ID followed by `_<layer-number>`;
- `filename`: the final, unique archive filename without `.gpkg`; and
- `basic_theme`: the topic only, without a place or date. The supplied theme
  can be changed to match a more specific layer, such as `Major Streets`.

Delete unused starter rows and duplicate a three-line row for additional
themes:

```yaml
- id: "<arcgis-item-id>_<layer-number>"
  filename: "<prefix>_<theme>_<yyyy>"
  basic_theme: "<theme>"
```

Add `temporal_year` when the source title does not identify the dataset year:

```yaml
- id: "<arcgis-item-id>_<layer-number>"
  filename: "<prefix>_parcels_2024"
  basic_theme: Parcels
  temporal_year: "2024"
```

Record IDs and filenames must be unique. A filename cannot include a directory
path. If `basic_theme` is omitted, the pipeline falls back to the DCAT title.

## Layers outside the Hub catalog

A vector layer omitted from the Hub catalog can be curated from its numbered
ArcGIS REST layer endpoint. Use the service item ID when REST supplies one:

```yaml
- id: "<service-item-id>_<layer-number>"
  filename: "<prefix>_<theme>_<yyyy>"
  basic_theme: "<theme>"
  temporal_year: "YYYY"
  source:
    type: arcgis_rest
    service_url: "https://<host>/arcgis/rest/services/<service>/FeatureServer/0"
```

The URL must end in a numbered `FeatureServer` or `MapServer` layer, not a
service root. ArcGIS Online is the default portal. For other cases, add one or
both optional values under `source`:

```yaml
portal_url: "https://<enterprise-portal-host>"
item_id: "<service-item-id>"
```

REST and item metadata are used first. Supply overrides only for missing or
incorrect values; the generated CSV still requires manual review:

```yaml
metadata_overrides:
  description: "<description>"
  creator: "<creator>"
  rights: "<rights>"
  keywords:
    - "<keyword>"
  landing_page: "https://<public-information-page>"
```

Supported overrides are `title`, `description`, `creator`, `rights`,
`keywords`, and `landing_page`.
