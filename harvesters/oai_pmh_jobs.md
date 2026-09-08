# OAI-PMH source jobs

The OAI-PMH browser page treats each university repository as a source-scoped
YAML job. Existing `type: oai_qdc` jobs in `config/` remain available, and new
jobs can be created from `config/templates/oai_pmh.yaml` without changing
Python code.

The staged workflow is:

1. **Validate** checks the YAML, endpoint, request settings, and output fields
   without contacting the repository. An empty `sets` list is valid for initial
   discovery, but Download and Build outputs still require a selected set.
2. **Identify** calls the OAI-PMH `Identify` and `ListMetadataFormats` verbs,
   records the repository's protocol details, and confirms the configured
   metadata prefix is advertised.
3. **Discover sets** calls `ListSets`, prints every exact `setSpec` and title in
   the browser log, and writes `discovered_sets.csv` inside the source's
   download directory. Copy the rows you want into the YAML `sets` list.
4. **Download XML** follows every `ListRecords` resumption token. A complete
   set is staged before it replaces the prior local snapshot, so an interrupted
   request cannot mix old and new pages.
5. **Build outputs** runs the normal harvester against the local XML snapshot.
   **Download + build outputs** performs both steps in sequence.

Job status is stored as `job-status.json` inside `oai_download_dir`. It records
the YAML checksum and each stage's start, completion, failure, scope, and output
paths. The browser ignores stale stage status when the YAML checksum changes.

## Set definitions

New jobs normally keep their selected sets directly in YAML:

```yaml
sets:
  - set: maps
    title: Map Collection
  - set: aerials
    title: Aerial Photography
```

The legacy `sets_csv`, `sets_csv_set_column`, and
`sets_csv_title_column` fields remain supported. Use this special inline entry
to request all records without a `set` parameter:

```yaml
sets:
  - set: __all__
    title: Entire repository
```

## Metadata formats

The parser accepts both standard Dublin Core (`metadata_prefix: oai_dc`) and
WorldCat qualified Dublin Core (`metadata_prefix: oai_qdc`, or the prefix
advertised by the repository). Generic DC fields are mapped automatically.
Use `metadata_crosswalk` for source-specific qualified fields and
`hardcoded_values` for registry constants such as `Code` and `Member Of`.

The default `Date Issued` mapping extracts a single four-digit year, preferring
`dcterms:issued` when supplied and otherwise using the source date fields. It
accepts publication statements, ISO dates, and bracketed/qualified years. Values
with multiple distinct years, incomplete years, or unknown dates produce a blank;
the source XML and existing temporal/date-range mapping retain the fuller context.
Explicit custom crosswalks and manual enrichment overrides can replace this value.

The default creator mapping keeps name strings from `dc:creator` and
`dcterms:creator` in `Creator`, and routes HTTP/HTTPS URLs to `Creator ID`.
This includes standalone URLs and authority URLs appended to names. Names and
URLs are deduplicated separately and stored as pipe-separated values.

`Subject` keeps the original labels from `dc:subject` and `dcterms:subject`,
removing standalone or appended HTTP/HTTPS linked-data URLs. Subject URLs are
discarded rather than routed to another field. The Michigan reconciliation
command uses these source subjects and excludes manual `Subject` enrichments.

Scale statements from `dc:source`, `dcterms:source`, and spatial coverage fields
populate `Spatial Resolution as Text`, excluding parenthesized MARC coordinates.
Readable scale text takes precedence over compact MARC scale denominators and
preserves approximation, unknown-scale statements, and vertical-scale wording.
If only a coded denominator is present, it is formatted as `1:48,000`, for example.
Original `Source` statements remain intact for subsequent coordinate processing.

## Request controls

`oai_request.delay_seconds` throttles resumption-token pages. Transient HTTP
429 and 5xx responses use the configured retry count and backoff. Optional
`from` and `until` values are inclusive OAI datestamp filters in `YYYY-MM-DD`
format.

If Identify reports that Cloudflare presented an interactive browser
challenge, the endpoint is blocking machine-to-machine OAI-PMH access. Changing
the YAML or imitating browser headers will not make that endpoint harvestable.
The repository owner must exempt the OAI-PMH route from browser challenges or
allowlist the harvester's public IP address.

Run the same stages from the terminal when needed:

```sh
uv run python scripts/oai_pmh_pipeline.py config/<job-id>.yaml validate
uv run python scripts/oai_pmh_pipeline.py config/<job-id>.yaml identify
uv run python scripts/oai_pmh_pipeline.py config/<job-id>.yaml discover
uv run python scripts/oai_pmh_pipeline.py config/<job-id>.yaml all
```

Add `--set-spec <set-spec>` to `download`, `harvest`, or `all` to limit a run
to one configured set. Single-set output filenames include the set slug so a
test run cannot overwrite the source's all-sets output from the same day;
upload-delta generation is also disabled for that partial run.

## Michigan multipart items

Michigan's image OAI feed repeats item metadata for each page or sheet. Set
`record_granularity: umich_catalog_item` to consolidate records by collection and
catalog number before field derivation and output. This option is enabled in
`config/umich.yaml`; the default `oai_record` preserves other jobs' behavior.

The item keeps all image/OAI identifiers as aliases and gets one catalog-level
IIIF manifest distribution containing all pages. Repeatable metadata is combined;
conflicting scalar values cause an error for review. Matching by title or removing
page suffixes is insufficient because different editions can share titles and
sheets can have different barcodes. The harvester also writes an
`*_item_membership.csv` audit mapping the source image rows to item IDs.

Michigan also sets `local_id_registry: enrichments/07d-01_local_ids.csv`. This
preserves established local IDs and assigns new items the prefix `um_` plus a
12-character alphanumeric Nano ID. Each suffix includes a digit, an uppercase
letter, and a lowercase letter. The registry retains historical `Identifier`
aliases for future matching and must be preserved between builds. A dated
`*_id_assignments.csv` log records which assignments were created or reused.

Build outputs, then follow the [enrichment workflow](../enrichments/README.md) to
restore existing canonical IDs and manual metadata before uploading.
