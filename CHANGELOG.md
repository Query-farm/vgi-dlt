# Changelog

## 0.2.0 (2026-11-25)

- Speaks VGI protocol 3.4: require vgi-python `>=0.55.2,<0.56`; uv.lock re-locked. This release
  needs Haybarn 2.0.1 (or stock DuckDB 2.0 with `INSTALL vgi FROM query_farm`): Haybarn 2.0.0
  loads a 3.3 `vgi` extension that cannot attach it.
- SQL tests run on haybarn-unittest 2.0.1rc3 (CI and Makefile).
- New `serve` extra (`vgi-python[http,oauth]`); the Docker image installs it, so the image runs in
  Orchard hosted mode (`VGI_ORCHARD_HOSTED=1` with VGI_JWT_ISSUER, VGI_JWT_AUDIENCE,
  VGI_JWT_JWKS_URI and VGI_SIGNING_KEY). The Docker HEALTHCHECK uses `HEAD /health`, which
  stays anonymous in hosted mode.
- **Breaking: module-source secret scopes.** The scope path for a module source is now
  `dlt://<package>/<module>/<function>` (dots and the colon become `/`) instead of
  `dlt://<package.module:function>`, because DuckDB 2.0.1 (duckdb#28684) matches an
  authority-only scope such as `dlt://github` only up to a `/`. `SCOPE 'dlt://github'` still
  covers every source in the package; old exact scopes like `dlt://pkg.mod:fn` stop matching:
  rewrite them as `dlt://pkg/mod/fn`. rest_api scopes (`dlt://rest_api/<host>`) are unchanged.

## 0.1.2 (2026-11-16)

- Move to Python 3.14: `requires-python >=3.14`, CI, the Docker image and lint tooling target 3.14.

## 0.1.1 (2026-11-16)

- Bump vgi-python to 0.52.2.

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
