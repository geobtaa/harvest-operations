# Harvest Registries

This folder is for compact, Git-trackable harvest state that helps rerun large
harvests without syncing bulky `inputs/` caches or dated `outputs/` files.

PASDA writes these files here by default:

- `pasda_metadata_registry.csv`
- `pasda_normalized_registry.jsonl`

The raw PASDA XML cache remains local in `inputs/pasda/metadata_xml/`.
