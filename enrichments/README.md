# Retaining manual metadata through a reharvest

Keep the manual enrichment CSV as a sparse overlay keyed by your **canonical ID**.
Treat source identifiers and URLs as aliases. For Michigan, first consolidate the
image-level OAI records into **one record per catalog item**, then reconcile IDs
and apply enrichments. The item's IIIF manifest displays all its images/pages.

## Run the Michigan workflow

From the repository root, rebuild from the downloaded XML:

```bash
uv run python harvesters/oai_qdc.py config/umich.yaml
```

`config/umich.yaml` enables `record_granularity: umich_catalog_item`. Other OAI
jobs retain their existing behavior. Grouping uses collection plus catalog number,
not title or a barcode with its suffix stripped: some multipart items have entirely
different barcodes for each sheet. All page identifiers remain in `Identifier` for
matching. Repeatable metadata values are combined; conflicting scalar fields stop
the build instead of being silently discarded.

The build writes dated `umich_items_primary.csv`, `umich_items_distributions.csv`,
and `umich_items_primary_item_membership.csv` files. The membership table maps each
original image record to its item and manifest. New item IDs use `um_` followed
by a 12-character Nano ID containing digits, uppercase letters, and lowercase
letters, with no symbols in the suffix. Established IDs remain unchanged.

`local_id_registry: enrichments/07d-01_local_ids.csv` enables persistent assignment.
The registry matches normalized source aliases from `Identifier`, stores the local
ID and historical identifiers, and reuses the assignment on future builds. Keep
this file: deleting it would lose the saved random IDs. It was seeded with the
1,139 established item IDs, including the 10 missing maps. The collection-level
record is managed separately. Registry `created_at` records when the entry was
created in the registry, not the item's accession date.

Each build also writes `*_primary_id_assignments.csv` with the temporary grouping
ID, assigned local ID, `created`/`reused` status, and source identifiers. IDs are
assigned before writing primary, distribution, and membership outputs so all three
use the same IDs. Collisions are retried; conflicting source identity matches stop
the build for review. Registry writes are locked and atomic.

Then reconcile the item-level harvest (substitute the build date as needed):

```bash
uv run python scripts/reconcile_umich_enrichments.py \
  --harvested-primary outputs/2026-09-07_umich_items_primary.csv \
  --enrichments enrichments/07d-01_enrichments.csv \
  --existing-distributions enrichments/07d-01_distributions.csv \
  --harvested-distributions outputs/2026-09-07_umich_items_distributions.csv \
  --xml-dir inputs/oai-downloads/umich \
  --output-dir outputs/2026-09-07_umich_items_reconciled
```

Both commands use local XML/CSV inputs and do not publish anything. The original
image-level exports remain unchanged. **Use the `umich_items_reconciled` outputs;
the earlier `umich_reconciled` results and their page-level identity registry are
superseded.** Do not feed the earlier registry into this item-level reconciliation:
it treated additional pages as separate canonical records.

## Matching and enrichment rules

1. Match exact image/item keys or known ID aliases, requiring a one-to-one match
   across the entire harvest. Since the grouped record retains every page alias,
   an old link to any page can identify its parent item. Michigan URL formats,
   encoded `%5D`, and catalog-number zero padding are normalized. Image suffixes
   remain intact as aliases.
2. Conflicting identities and multiple existing IDs stay unresolved. A shared
   catalog number alone supplies a review candidate rather than silently merging
   separate existing catalog records.
3. For accepted matches, keep your existing canonical `ID` and replace each
   harvested field with the **nonblank** value in the enrichment CSV. Blank cells
   mean “no manual override.” Pipe-separated lists and literal `null` strings are
   preserved exactly. Intentional field deletion is not encoded by a blank cell.
   **Exception: `Subject` always uses the harvested terms, with HTTP/HTTPS URLs
   removed.** The Michigan reconciliation command excludes `Subject` enrichments
   by default, including when the source has no subject. The original enrichment
   CSV remains intact. Additional fields can be excluded with repeated
   `--exclude-enrichment-field "Field Name"` arguments; exclusions appear in
   `summary.json` and excluded fields are absent from the applied-cell audit.
4. Retain all harvested items. Unmatched items keep their harvested IDs and values.
   Mark rows `matched`, `new`, or `needs_review` in review exports. Here `new`
   means absent from the supplied inventory, not recently published.
5. Report existing records absent from the active harvest separately. XML deletion
   notices supply review evidence; the workflow does not retire records. An active
   match takes precedence over a historical deletion notice.

## Files to use

| Output | Purpose |
| --- | --- |
| `reconciled_primary.csv` | One row per catalog item, with canonical IDs and manual enrichments; original primary schema. |
| `reconciled_distributions.csv` | Harvested landing pages and catalog-level IIIF manifests remapped to canonical IDs. |
| `review_primary.csv` | Items plus status, original harvested ID, candidates, evidence, and applied field names. |
| `new_items.csv` | New catalog items with no unresolved candidate. |
| `unmatched_items.csv` | All items without an accepted match, including any unresolved candidates. |
| `needs_review.csv` | Unresolved items; no enrichments applied. Review these before uploading. |
| `review_decisions.csv` | Editable decision template. |
| `crosswalk.csv` | Harvested-to-canonical ID mapping, status, evidence, and title. |
| `identity_aliases.csv` | Cumulative registry for future runs, including missing old records and newly recognized items; unresolved identities are excluded. |
| `enrichment_audit.csv` | Each applied cell with harvested value, enriched value, and whether it changed. |
| `missing_existing.csv` | Absent existing IDs, retained enrichments, and source deletion evidence. |
| `summary.json` | Counts, overlay policy, input paths, and SHA-256 fingerprints, including XML. |

The distribution output now includes one `iiif_manifest` per item, using
`https://quod.lib.umich.edu/cgi/i/image/api/manifest/clark1ic:<catalog-number>`.
The old `/api/search/clark1ic:<catalog-number>` URL and the image-specific manifest
URLs resolve to the same catalog-level manifest in verified examples. A single
catalog manifest can therefore display the entire multipart resource.

The distribution output contains current harvested items; it does not include
links for the collection record or missing maps. Retain those separately and
review distribution updates before replacing existing inventory.

## Corrected September 6 XML, rebuilt September 7, 2026

The downloaded XML contains **2,043 active image records representing 1,332 catalog
items**. The earlier parsed CSV had 2,042 rows; regrouping directly from XML also
retains image `39015091888753_02-2`, absent from that draft export.

- **1,129 existing items matched**, preserving their IDs and **9,566 enriched cells**
  after excluding the 152 previously applied manual Subject cells.
- **203 new catalog items**, each with a persistent `um_` Nano ID and one IIIF manifest.
- **0 unresolved matches**.
- **10 existing maps have source deletion notices dated August 20, 2026**.
- **`07d-01` is the collection-level record**, absent from the item harvest; retain it separately.

The earlier 335 new image rows and 578 related-image review rows are superseded by
these item-level results. There are still four pairs of new items with identical
titles but distinct catalog IDs. Their dates, editions, or descriptive notes differ
(for example, the two *South Dakota* maps date to 1909 and 1910). These remain
separate items; matching by title would incorrectly merge them.

Live checks confirmed that catalog manifests contain 24, 15, 9, and 1 canvases for
four sampled items, matching the XML image counts. The saved
`manifest_verification.json` records those checks; this was a sample check, not a
network validation of all 1,332 manifests.

## Review and repeat

Copy `review_decisions.csv` to a durable location, such as
`enrichments/07d-01_review_decisions.csv`, if review is needed. Leave unreviewed
`action` cells blank. Use `accept_new` with blank `canonical_id` to accept a
separate item, or `match` with an existing `canonical_id` after confirming identity.
The command rejects two harvested items assigned to one canonical record.

Rerun with `--decisions enrichments/07d-01_review_decisions.csv` and a new output
directory. Once accepted, retain the item-level `identity_aliases.csv` as
`enrichments/07d-01_identity_aliases.csv`. Pass it to subsequent harvests with
`--aliases enrichments/07d-01_identity_aliases.csv`, updating it from each accepted
run. The command refuses to overwrite input files.

Continue adding manual edits to the enrichment CSV under canonical IDs, including
newly accessioned items. Keep fields you intend to preserve, such as
`Date Accessioned`, in that overlay. Archive raw XML, primary/distribution harvests,
overlay, aliases, decisions, and results per run. Generated `outputs/` files are
ignored by Git; keep durable registries and decisions in `enrichments/`.

This run restores identity and enrichment; it cannot establish which source fields
changed because the old export contains only manual enrichments. For future source
change detection, compare archived raw item-level harvests by canonical ID before
applying the overlay. The enrichment audit compares source values to manual values;
it is not a source-change log.
