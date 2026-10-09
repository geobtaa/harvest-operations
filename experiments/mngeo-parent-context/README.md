# MnGeo parent and sublayer harvesting proof of concept

This experiment runs locally on `mngeo-harvest-tests` using the saved feeds. Nothing was
uploaded, deployed, merged, or written into the normal outputs directory.

## Plain-language walkthrough

Think of a regional parcel dataset as a folder containing county layers. MnGeo's
feed describes both the folder and its contents. Our previous rules kept the
county layers but skipped the regional record. Without the regional title,
different annual county layers looked alike.

The experiment keeps the regional record and the county records, adding the
regional name to county titles. Existing county IDs stay the same; regional
records use their own supplied IDs. This improves representation rather than
removing duplicates. Download links also now reach the separate distributions
table, which already supported multiple links per record.

## Which files to review first

1. `representative_comparison.csv`: the easiest demo. Filter `example` to
   `parcels`, `precipitation`, `temperature`, or `lakeshore`. Compare `feed_title`,
   `before_title`, and `after_title`. A blank `before_title` means the old filter
   excluded that record. `role` says parent or sublayer; `parent_ID` connects a
   sublayer to its parent. `links` is a JSON list of resulting URLs.
2. `mngeo_after_primary.csv`: all resulting metadata. Search `Title` for
   `Hennepin County Parcels` to compare years, or `Metropolitan 7-County Parcel`
   to compare regional datasets and their county layers.
3. `mngeo_after_distributions.csv`: resulting links. Filter `friendlier_id` to
   an `ID` from the primary table. Multiple rows with that ID are expected:
   they are several links belonging to one record.
4. `counts.csv`: totals. `unmapped_links_review.csv`: links needing human review.
   The Minneapolis before/after files provide the unchanged comparison source.

## Filename guide

The top-level output names follow `SOURCE_PHASE_TABLE.csv`:

- `SOURCE`: `mngeo` or `minneapolis`.
- `PHASE`: `before` uses the old rules; `after` enables the experimental policy.
  Both use the same saved feed. These are different rules, not different dates.
- `TABLE`: `primary` contains one metadata row per resource; `distributions`
  contains one row per mapped link, joined by `friendlier_id` = primary `ID`.

The `SOURCE_PHASE_writer` folders exercise the actual CSV-writing functions in
isolation. Their `outputs/YYYY-MM-DD_arcgis_primary.csv` and
`YYYY-MM-DD_arcgis_distributions.csv` use the normal writer naming convention:
the date is the generation date, and `arcgis` names the harvester. The enclosing
folder identifies the source and scenario. For review, use the simpler top-level
CSVs; they are checked against the writer-produced tables.

Nested `reports/arcgis/YYYY-MM-DD_arcgis_report.csv` files are technical writer
checks, not live harvest reports. Because this experiment bypasses live fetching,
their fetch totals are zero. Use the top-level `counts.csv` for comparison totals.

Previously, the ArcGIS filter accepted a dataset with a distribution titled
`Shapefile`, or an `ArcGIS GeoService` containing `ImageServer`. Regional parcel
parents had neither, so county layers appeared without their parent's year and
regional title. These were distinct resources with distinct identifiers, rather
than duplicate records representing the same layer.

The experimental policy retains each explicit eligible parent and its useful
layers. It matches the item ID and root/numbered service URLs, then appends parent
context to layer titles where absent. Repeated sibling titles receive layer
numbers. It never invents a parent, merges layers, or infers geometry from a name.
Conflicting feed entries sharing an identifier stop processing with an error;
identical entries are emitted once.

The policy is enabled only for `05a-01` through
`source_policies.05a-01.parent_sublayer_context` in `config/arcgis.yaml`.
Removing or disabling that setting restores the previous behavior. Other sources
retain the previous eligibility and distribution mapping.

The CSV schemas and shared distribution writer are unchanged. Each record keeps
its own service and download URLs. The existing writer already accepts lists;
the opt-in ArcGIS mapping now supplies those lists and download format labels.
It includes recognized file downloads and explicit `downloadURL` values. Parent
records do not receive copies of child downloads. Descriptions and extents pass
through the existing cleanup without any additional modifications.

| Dataset records (administrative website rows omitted) | Before | After |
| --- | ---: | ---: |
| MnGeo | 2,201 | 2,435 |
| MnGeo unique ArcGIS items | 923 | 923 |
| MnGeo distribution rows | 4,402 | 16,316 |
| Minneapolis | 269 | 269 |
| Minneapolis unique ArcGIS items | 253 | 253 |
| Minneapolis distribution rows | 538 | 538 |

All 2,201 previously eligible MnGeo IDs remain. There are 234 additional parent
records and 1,467 renamed existing records; none were consolidated or filtered
out. Retaining both levels increases record count while making results easier to
distinguish. Parcel county titles now include their regional year and geometry,
for example:

> Hennepin County Parcels - Metropolitan 7-County Parcel Polygons - 2025 [Minnesota]

## Demo files

- `counts.csv`: compact source totals.
- `representative_comparison.csv`: original feed titles, previous/output titles,
  parent/child roles, stable identifiers and links. Covers parcels by year and
  geometry, precipitation, temperature and sensitive lakeshore assessment.
- `mngeo_{before,after}_{primary,distributions}.csv`: full dataset-only outputs.
- `minneapolis_{before,after}_{primary,distributions}.csv`: regression comparison.
- `*_writer/`: CSVs and reports emitted by the actual ArcGIS/base output writers
  inside this experiment directory. They are checked against the comparison CSVs.
- `unmapped_links_review.csv`: 242 source links on 232 included records that are
  not confidently mapped to a service/download/documentation type. This audit
  keeps the experimental mapping's limits visible.

## Validation and rerunning

From the repository root:

```powershell
.venv/Scripts/python.exe -m scripts.demo_mngeo_parent_context
.venv/Scripts/python.exe -m pytest tests/test_arcgis_parent_context.py tests/test_arcgis_harvester.py -q --basetemp=experiments/pytest-mngeo-check
```

The focused regression suite passes: **16 tests**. The full-feed offline runner
also checks stable primary/distribution results across repeat and reversed-feed
runs, unique IDs and identifiers, one occurrence of each admitted parent, no lost
eligible records, unchanged descriptions/extents/identifiers, parcel years and
geometry, distribution foreign keys, recognized links, and CSV writer results.
Minneapolis primary and distributions also matched the pre-implementation Git
version during validation, not merely a run with the policy disabled. Ruff and whitespace
checks pass for the new code.

An initial fixture-order mistake in a new test was corrected. Initial legacy
test setup errors came from pytest's inaccessible default temporary directory;
using an isolated workspace temporary directory resolved them. A broader
application-route test run stalled and was interrupted; that suite is not claimed
as passing. The focused ArcGIS suite has no outstanding failures.

## Human review before any operational use

- Parent service-root URLs are preserved with existing ArcGIS reference types.
  Their behavior in the legacy Geoportal viewer has not been tested.
- Precipitation source metadata conflicts: the parent says 1961–1990 while its
  layers say 1969–1990. Both supplied titles are preserved. Existing temporal
  inference remains in use; this experiment does not resolve contradictory dates.
- Three repeated-title instances remain among five unrelated address/road records
  without suitable parent context. Similar titles are not grounds for merging.
- Review long contextual titles and unmapped links, including unnamed links and
  application pages. No new classification is guessed for them.
- Link preservation is checked against the saved feed, not live availability.
  The newer DCAT versions are not parsed by this proof of concept.
- Upload generation, registry behavior and manual upload procedures are unchanged.
  The demo uses dataset-only records and a fixed accession date for reproducibility;
  routine harvests still append their normal administrative website rows.
