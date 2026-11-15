<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

# Query Any REST API or dlt Source as Tables in DuckDB

> **vgi-dlt** · a [Query.Farm](https://query.farm) VGI worker

[![CI](https://github.com/Query-farm/vgi-dlt/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-dlt/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

[dlt](https://dlthub.com) (data load tool, Apache-2.0) describes data sources as Python
generators called *resources*. This worker runs dlt sources from DuckDB and returns their
resources as typed tables, **without a pipeline or a destination**: the resources are
iterated directly and their items stream back as Arrow batches.

- **Zero-code REST APIs.** dlt's declarative [`rest_api`](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api/basic)
  source takes a JSON config (client, auth, paginator, resources, incremental parameters)
  and covers a large share of SaaS APIs with no code at all.
- **Any `@dlt.source`.** `dlt init` verified sources or pip packages load by module path
  (`package.module:function`) from a directory or an isolated, cached virtual environment,
  never the worker's own.
- **Credentials from secrets only.** A DuckDB secret of type `dlt` holds them; it is merged
  into a `rest_api` config and handed to dlt's config resolution in memory, like a
  `secrets.toml`. Nothing is written to disk or the environment.
- **Real types.** dlt column hints plus inference from sampled items give `BIGINT`,
  `DOUBLE`, `BOOLEAN`, `VARCHAR`, `TIMESTAMPTZ`, `DATE`, `DECIMAL`, `STRUCT`, `LIST`;
  `JSON` only for genuinely mixed shapes. Arrow tables a resource yields keep their types.
- **Incremental.** dlt `incremental` cursors are exposed as `initial_value`, `end_value`
  and `state` arguments, and `WHERE <cursor> >= …` pushes down as the API's start parameter.
- **Resumable.** The scan cursor is externalized, so a scan continues in another worker
  process (HTTP deployments).

## Quick start

```sql
INSTALL vgi FROM community;   -- Haybarn 2.0; on DuckDB 2.0 use the query_farm repository (below)
LOAD vgi;
ATTACH 'dlt' (TYPE vgi, LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-dlt@v0.1.0 vgi-dlt-worker');

-- A REST API described with dlt's rest_api config.
SET VARIABLE gh = '{
  "client": {"base_url": "https://api.github.com/repos/dlt-hub/dlt/"},
  "resource_defaults": {"endpoint": {"params": {"per_page": 100}}},
  "resources": [
    {"name": "issues", "primary_key": "id",
     "endpoint": {"path": "issues", "params": {"state": "all",
       "since": {"type": "incremental", "cursor_path": "updated_at", "initial_value": "2026-01-01T00:00:00Z"}}}}
  ]}';

-- The token never appears in the query: it is merged into the config from a secret.
CREATE SECRET github (TYPE dlt, SCOPE 'dlt://rest_api/api.github.com',
    CONFIG '{"client": {"auth": {"type": "bearer", "token": "ghp_..."}}}');

SELECT name, primary_key, incremental_cursor FROM dlt.main.resources('rest_api', config := getvariable('gh'));

SELECT user.login, count(*) AS issues
FROM dlt.main.read_resource('rest_api', 'issues', config := getvariable('gh'))
WHERE updated_at >= TIMESTAMPTZ '2026-06-01'      -- sent to GitHub as ?since=2026-06-01T00:00:00Z
GROUP BY ALL ORDER BY issues DESC;
```

On stock DuckDB 2.0:

```sql
SET allow_extension_repositories = 'allowed';
CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm';
INSTALL vgi FROM query_farm;
LOAD vgi FROM query_farm;
```

From a source checkout: `make build`, then `LOCATION '/abs/path/to/.venv/bin/vgi-dlt-worker'`.

### A verified source

```sh
dlt init github duckdb          # copies the github verified source into ./github
```

```sql
CREATE SECRET gh_source (TYPE dlt, SCOPE 'dlt://github', CONFIG '
[sources.github]
access_token = "ghp_..."');

SELECT count(*) FROM dlt.main.read_resource('github:github_reactions', 'issues',
    path := '/abs/path/to/project', pip_url := 'PyGithub',
    config := '{"owner": "dlt-hub", "name": "dlt"}');
```

`path` puts the project directory on the source's `sys.path`; `pip_url` (the source's
`requirements.txt` entries, or a package that ships the source) is installed once into an
isolated environment under the cache directory together with the worker's dlt version.

## Functions

All in `dlt.main`. Source-running functions take the source first (`'rest_api'` or
`'package.module:function'`) and the named arguments `config`, `path`, `pip_url`, `python`,
`secret_name` and `timeout_seconds`.

| Function | Returns |
|---|---|
| `resources(source, ...)` | One row per resource: selected, transformer/parent, write disposition, primary/merge key, incremental cursor and default start, hinted columns. Reads no items. |
| `infer_columns(source, resource :=, sample_rows := 100, ...)` | One row per column `read_resource` would return, with its DuckDB type and origin (`hint` / `sample`). |
| `columns_json(source, ...)` | The same, as one JSON object for the ATTACH `columns` option. |
| `read_resource(source, resource, columns :=, sample_rows :=, initial_value :=, end_value :=, state :=, ...)` | The resource as a typed table. |
| `read_items(source, resource, ...)` | Each item as raw `JSON` (no hints, no sampling). |
| `final_state(source, resource, initial_value :=, state :=, ...)` | Runs the resource to the end: item count, incremental cursor, `last_value` and the resource `state` to resume from. |
| `column_type(type_name)` | Scalar: the DuckDB type a dlt data type or type text maps to. |
| `infer_type(json_value)` | Scalar: the DuckDB type inference picks for a JSON value. |

### Attach a source as a database

```sql
SET VARIABLE cols = (SELECT columns::VARCHAR FROM dlt.main.columns_json('rest_api', config := getvariable('gh')));
ATTACH 'dlt' AS gh (TYPE vgi, LOCATION '...', source 'rest_api', config getvariable('gh'), columns getvariable('cols'));
SELECT count(*) FROM gh.issues WHERE updated_at >= TIMESTAMPTZ '2026-06-01';
```

Every selected resource becomes a table. ATTACH builds the source but never reads items
and never sees secrets, so column types come from the resources' dlt hints plus the
`columns` option (`{"resource": {"column": "TYPE"}}`, e.g. from `columns_json`). A module
source that cannot be built without its secret is attached from `columns` alone. Options:
`source`, `config`, `columns` (JSON or a file path), `source_path` (the function argument
`path`; DuckDB reserves `path`), `pip_url`, `python`, `secret_name`, `timeout_seconds`.
Table scans run `read_resource`, so they resolve the secret and push down like a direct call.

## Secrets

```sql
CREATE SECRET name (TYPE dlt, CONFIG '<JSON object or TOML>', SCOPE 'dlt://...');
```

- `rest_api`: `CONFIG` is deep-merged into the REST config, typically
  `{"client": {"auth": {...}}}` (any dlt auth: bearer, api_key, http_basic, oauth2…). The
  scope path is `dlt://rest_api/<base_url without scheme>`.
- Module sources: `CONFIG` is read like dlt's `secrets.toml`
  (`[sources.<name>] access_token = "…"`), through an in-memory provider placed first in
  dlt's provider chain. The scope path is `dlt://<package.module:function>`, so
  `SCOPE 'dlt://github'` covers every source in the `github` package.
- A secret without a scope applies to every source; `secret_name :=` picks one by name.
- The value is redacted in `duckdb_secrets()` and never written to a file, an environment
  variable, the scan cursor or an error message. dlt's own config lookup (environment
  variables, `.dlt/secrets.toml` in `path` and `~/.dlt`) still works as usual.

## What pushes down

| SQL | Pushed into |
|---|---|
| Projection (selected columns) | The runner keeps only those item keys before serializing (Arrow items: column selection). |
| `WHERE cursor >= X`, `> X`, `BETWEEN X AND …` on the resource's incremental cursor | The incremental `initial_value` (the later of it and X): a `rest_api` resource sends it as its start parameter (`?updated_since=…`), a module source starts its incremental there. Timestamps are rendered as ISO-8601 UTC (`2026-02-01T00:00:00Z`). |
| everything else | Evaluated by DuckDB. |

DuckDB re-applies every pushed filter, so results are exact even when the API's parameter
is coarser. Other `rest_api` query parameters (state, labels, …) belong in the config's
`params`. Upper bounds are not pushed (dlt's `end_value` needs a start); use `end_value :=`.

## Type mapping

Columns, in order: the resource's dlt column hints, then keys seen in the first
`sample_rows` items (default 100; `0` = hints only). `columns := '{"col": "TYPE"}'`
overrides any of them with a DuckDB type (`BIGINT`, `VARCHAR[]`, `STRUCT(login VARCHAR)`,
`DECIMAL(18,2)`, …) or a dlt data type (`text`, `bigint`, `double`, `bool`, `timestamp`,
`date`, `time`, `decimal`, `json`, `binary`, `wei`).

| Item value | DuckDB type |
|---|---|
| whole number | `BIGINT` |
| other number (or mixed) | `DOUBLE` |
| `true` / `false` | `BOOLEAN` |
| ISO-8601 timestamp string | `TIMESTAMP WITH TIME ZONE` (offset-less = UTC) |
| ISO-8601 date string | `DATE` |
| other string | `VARCHAR` |
| object | `STRUCT` of the keys seen in the sample (3 levels deep) |
| array | `T[]` |
| mixed / deeper | `JSON` |
| Arrow column | its Arrow type |

Primary-key hint columns are `NOT NULL`. A value that does not fit its column becomes
`NULL` (counted in the worker log); keys outside the bound columns are dropped, so use
`read_items` to see an item exactly as the API returned it.

## How scans run and resume

Each operation runs `runner.py` in a child process with the source's interpreter: the
worker's own for `rest_api` and `path` sources, or the cached environment for `pip_url`
sources (`$VGI_DLT_CACHE_DIR`, else `~/Library/Caches/vgi-dlt` / `$XDG_CACHE_HOME/vgi-dlt`;
delete it to reinstall). The request, secret included, goes over stdin; items come back as
framed JSON or Arrow IPC over a bounded queue, so a fast source blocks instead of filling
memory. A source that produces nothing for `timeout_seconds` (default 300) is stopped.
HTTP retries, `Retry-After` and backoff are dlt's own (`rest_api` uses dlt's retrying
client).

The scan cursor holds a random token and the number of items returned, never config or
secrets. In one process the live runner continues between batches; in another process the
source is restarted with the same request (same incremental start and state) and the items
already returned are skipped. This relies on the source returning the same items in the
same order for the same request; a source that returns fewer fails the scan instead of
silently dropping rows.

## Testing

`make check` runs ruff, mypy, pydoclint, the unit tests, the SQL suite over the stdio,
launch and HTTP transports, and the metadata lint. Everything is offline:

- `test/mock_api/server.py`: a SaaS-like API with page-number pagination and a total,
  a `Link`-header paginator, a child endpoint, a bearer-token endpoint, an
  `updated_since` incremental parameter, a request log (to assert pushdown) and a 429 with
  `Retry-After` on every page's first request.
- `test/dlt_sources/vgi_dlt_fixture`: a tiny `@dlt.source` (incremental dict items with
  DECIMAL/TIMESTAMP hints, a transformer, a resource yielding Arrow tables, a secret-backed
  `api_key`), loaded with `path :=` and installed with `pip_url :=`.
- The metadata lint runs with execution disabled (`vgi-lint.toml`): the examples call live
  APIs and verified sources need credentials. The two scalars' executable examples are
  asserted in `test/sql/dlt.test`.

## Support

Open an issue at https://github.com/Query-farm/vgi-dlt/issues.

## Authorship & License

Copyright 2026 Query Farm LLC - https://query.farm

Licensed under the BSD Zero Clause License (0BSD); see [LICENSE](LICENSE).
dlt is © ScaleVector / dltHub, licensed under Apache-2.0.
