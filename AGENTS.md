# Repository Guidance

## Harvester Structure

Harvesters in `harvesters/` should follow the template method pattern defined by
`harvesters/base.py`.

For any harvester class, such as `ArcGISHarvester(BaseHarvester)`, the only
methods defined on the class should be methods that also exist on
`BaseHarvester`:

- `__init__`
- `load_reference_data`
- `fetch`
- `parse`
- `flatten`
- `build_dataframe`
- `derive_fields`
- `add_defaults`
- `add_provenance`
- `clean`
- `validate`
- `write_outputs`
- `build_uploads`
- `harvest_pipeline`

Do not add new helper methods directly to harvester classes. If helper behavior
is reusable across harvesters, move it into an appropriate module under
`utils/` and import it into the harvester.

## Custom Harvester Helpers

If a helper is truly specific to one harvester and should not be generalized,
define it as a module-level function at the end of that harvester file, after
the class definition.

Clearly label that section with:

```python
# Custom functions for this harvester
```

Class methods may call those module-level custom functions, but the helper
functions themselves should not be class methods unless they match the
`BaseHarvester` method surface listed above.

## Pipeline Style

Prefer the pipeline format used by `BaseHarvester.harvest_pipeline()`.

Within harvester template methods, structure dataframe transformations so they
can be called through `DataFrame.pipe(...)` wherever practical. Keep the
templated class methods responsible for orchestration and source-specific
mapping, and keep reusable transformations in `utils/`.

When adding or modifying harvesters:

- Preserve the base pipeline order unless a source requires a documented
  exception.
- Put shared normalization, matching, parsing, and formatting logic in `utils/`.
- Keep source-specific one-off logic in clearly labeled module-level custom
  functions at the end of the harvester file.
- Avoid expanding the public method surface of individual harvester classes.
