# Harvest Registries

This folder is for compact, Git-trackable harvest state that helps rerun large
harvests without syncing bulky `inputs/` caches or dated `outputs/` files.

PASDA writes these files here by default:

- `pasda_metadata_registry.csv`
- `pasda_normalized_registry.jsonl`

The raw PASDA XML cache remains local in `inputs/pasda/metadata_xml/`.

ArcGIS uses these compact registries as the previous-run state for upload deltas:

- `arcgis_primary_registry.csv`
- `arcgis_distributions_registry.csv`

The ArcGIS harvester reads these files before building upload CSVs, then updates
them from the current full local outputs. The registries are current snapshots,
not history tables: records and distribution rows missing from the current
harvest are pruned after upload deltas are written. `Date Accessioned` is
preserved from the existing primary registry.
