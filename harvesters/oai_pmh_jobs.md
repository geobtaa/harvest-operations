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
