# Changelog

## 0.1.0 (unreleased)

First release.

- `resources`, `infer_columns`, `columns_json`: discover a dlt source's resources, hints and column types.
- `read_resource`, `read_items`, `final_state`: iterate a resource without a pipeline (typed rows, raw
  JSON, or the final incremental state).
- `column_type`, `infer_type`: the type mapping as scalars.
- Sources: dlt's declarative `rest_api` (JSON config) and any `@dlt.source` by module path, from a
  directory or an isolated cached `pip_url` environment.
- `dlt` secret type (JSON or TOML), scoped `dlt://rest_api/<host>` / `dlt://<module:function>`.
- Projection pushdown; incremental-cursor lower bounds push down as the incremental start.
- ATTACH a source: one table per resource.

Targets: vgi-python 0.51 · dlt 1.32 · VGI protocol 3.3 · Haybarn 2.0.0rc11 · vgi-lint-check 2.x (unpinned)
