"""Discovery metadata: the one file to edit when you rename or extend the worker.

DuckDB, coding agents and the ``vgi-lint`` metadata checker all read these
tags. Keep every object documented and ``make lint`` stays at 100.

* Catalog-level tags (title, license, author, links, long description) live in
  ``CATALOG_TAGS`` below.
* Every function and table carries ``object_tags(...)``: a title, a category
  from ``SCHEMA_CATEGORIES``, search keywords, and LLM- and human-facing docs.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

CATALOG_NAME = "dlt"

SOURCE_URL = "https://github.com/Query-farm/vgi-dlt"

# Categories group functions in docs and agent tools. Every object's
# ``vgi.category`` must name one of these.
SCHEMA_CATEGORIES = [
    {"name": "sources", "title": "Sources", "description": "Read the items of a dlt source's resources as tables."},
    {"name": "discovery", "title": "Discovery", "description": "List resources, their hints and inferred columns."},
    {"name": "state", "title": "Incremental State", "description": "Run a resource and keep its incremental state."},
]


def keywords_array(keywords: str) -> str:
    """Turn ``"a, b, c"`` into the JSON array string ``vgi.keywords`` requires."""
    return json.dumps([k.strip() for k in keywords.split(",") if k.strip()])


def object_tags(*, title: str, category: str, keywords: str, doc_llm: str, doc_md: str) -> dict[str, str]:
    """Build the standard per-object discovery tags."""
    if category not in {c["name"] for c in SCHEMA_CATEGORIES}:
        raise ValueError(f"unknown category {category!r}; add it to SCHEMA_CATEGORIES")
    return {
        "vgi.title": title,
        "vgi.category": category,
        "vgi.keywords": keywords_array(keywords),
        "vgi.doc_llm": doc_llm,
        "vgi.doc_md": doc_md,
    }


def column(name: str, type_: pa.DataType, comment: str, *, nullable: bool = True) -> pa.Field[Any]:
    """Build an Arrow field whose ``comment`` shows up in DuckDB's ``DESCRIBE`` and agent tools."""
    return pa.field(name, type_, nullable=nullable, metadata={b"comment": comment.encode()})


def result_columns(schema: pa.Schema) -> str:
    """``vgi.result_columns_schema`` JSON for a fixed output schema."""
    from .coltypes import type_name

    return json.dumps(
        [
            {
                "name": f.name,
                "type": type_name(f.type),
                "description": (f.metadata or {}).get(b"comment", b"").decode(),
            }
            for f in schema
        ]
    )


CATALOG_COMMENT = (
    "Run dlt (dlthub) sources from DuckDB without a pipeline: the declarative rest_api source covers most "
    "REST APIs with a JSON config, and any @dlt.source package loads in an isolated environment."
)

CATALOG_DOC_LLM = (
    "Runs dlt (data load tool, dlthub) sources and returns their resources as DuckDB tables, iterating the "
    "resources directly (no pipeline, no destination). Two kinds of source: 'rest_api', dlt's declarative REST "
    "API source, configured with a JSON object (client.base_url, client.auth, client.paginator, "
    "resources[].endpoint with path, params, data_selector, incremental) that covers most SaaS APIs with zero "
    "code; and 'package.module:function', any @dlt.source (dlt verified sources from `dlt init`, or pip "
    "packages) imported from `path` or installed from `pip_url` into an isolated cached environment, never into "
    "the worker. Credentials come only from a DuckDB secret of type `dlt` (CONFIG = JSON or TOML; deep-merged "
    "into a rest_api config, and served to dlt's config resolution like secrets.toml), scoped to "
    "'dlt://rest_api/<host>' or 'dlt://<package>/<module>/<function>'. Columns come from dlt column hints plus "
    "inference from sampled items (`BIGINT`, `DOUBLE`, `BOOLEAN`, `VARCHAR`, `TIMESTAMPTZ`, `DATE`, `STRUCT`, `LIST`, "
    "`JSON`); "
    "Arrow data a resource yields keeps its types. dlt incremental cursors map to `initial_value`, `end_value`, "
    "`state` arguments and to WHERE <cursor> >= X pushdown. ATTACH with `source` and `config` options exposes "
    "every resource as a table."
)

CATALOG_DOC_MD = (
    "# dlt sources in DuckDB\n\n"
    "Run any [dlt](https://dlthub.com) source and query its resources with SQL, without a pipeline or a "
    "destination.\n\n"
    "Describe a REST API with dlt's declarative `rest_api` JSON config, or load any `@dlt.source` from a "
    "directory or a pip package into an isolated, cached environment. Credentials live in DuckDB secrets; "
    "items stream back as typed Arrow batches; dlt incremental cursors become arguments and filter pushdown.\n\n"
    "Attach a source to see every resource as a table."
)

SCHEMA_COMMENT = "Functions that run dlt sources; attached source resources appear here as tables."

SCHEMA_DOC_LLM = (
    "`main` holds the worker's functions and, when the catalog is attached with `source` and `config` options, "
    "one table per selected resource of that source. A source is 'rest_api' (dlt's declarative REST API "
    "source, described by a JSON config) or 'package.module:function' (any @dlt.source). Functions that run a "
    "source take it first, then a resource name where one is needed, and share the named arguments `config`, "
    "`path`, `pip_url`, `python`, `secret_name` and `timeout_seconds`; reads also take `initial_value`, "
    "`end_value` and `state` for incremental loads. Start by listing a source's resources, preview the column "
    "types, then read typed rows (or raw JSON items for unfamiliar responses). Credentials go in a `dlt` secret."
)

SCHEMA_DOC_MD = (
    "## main\n\n"
    "Functions that list, read and checkpoint dlt sources.\n\n"
    "When a source is attached, each of its resources is also a table here, and scanning it runs the source."
)

DYNAMIC_COLUMNS_MD = (
    "One column per item key, in this order: the resource's dlt column hints, then keys seen in the first "
    "`sample_rows` items. `columns := '{...}'` overrides any type. Typing rules:\n\n"
    "| Name | Type | Description |\n"
    "| --- | --- | --- |\n"
    "| hinted `decimal` column | DECIMAL(10,2) | a dlt column hint; `text`, `bigint`, `double`, `bool`, "
    "`timestamp`, `date`, `time`, `json`, `binary` map the same way (see column_type); primary keys are NOT NULL |\n"
    "| integer key | BIGINT | whole numbers |\n"
    "| number key | DOUBLE | numbers with a fraction (or mixed) |\n"
    "| boolean key | BOOLEAN | true / false |\n"
    "| timestamp key | TIMESTAMP WITH TIME ZONE | ISO-8601 strings with a time; offset-less values are UTC |\n"
    "| date key | DATE | ISO-8601 dates |\n"
    "| text key | VARCHAR | other strings |\n"
    "| object key | STRUCT(login VARCHAR, id BIGINT) | e.g. a nested user object; keys from the sample |\n"
    "| array key | VARCHAR[] | e.g. a list of labels; elements typed by the same rules |\n"
    "| mixed key | JSON | values of different types, or nesting deeper than 3 levels |\n"
    "| Arrow `int32` column | INTEGER | resources that yield Arrow tables keep their Arrow types |\n"
)

POKEAPI_CONFIG = (
    '{"client": {"base_url": "https://pokeapi.co/api/v2/", "paginator": "json_link"}, '
    '"resource_defaults": {"endpoint": {"params": {"limit": 100}}}, "resources": ["pokemon", "berry"]}'
)

AGENT_TEST_TASKS = [
    {
        "name": "pokeapi_berry_count",
        "prompt": (
            "Using the dlt worker and dlt's rest_api source with this config: "
            f"{POKEAPI_CONFIG} - how many berries does the berry resource return?"
        ),
    },
    {
        "name": "pokeapi_resources",
        "prompt": (
            f"Using the dlt worker, which resources does the rest_api config {POKEAPI_CONFIG} define? "
            "List their names alphabetically."
        ),
    },
    {
        "name": "pokeapi_berry_columns",
        "prompt": (
            f"With the dlt worker and the rest_api config {POKEAPI_CONFIG}, which DuckDB type does the `url` "
            "column of the berry resource get?"
        ),
    },
    {
        "name": "pokeapi_columns_for_attach",
        "prompt": (
            f"With the dlt worker, produce the JSON of inferred column types for every resource of the rest_api "
            f"config {POKEAPI_CONFIG}, ready for the ATTACH columns option. Which resources does it cover?"
        ),
    },
    {
        "name": "pokeapi_first_berry_raw",
        "prompt": (
            f"With the dlt worker and the rest_api config {POKEAPI_CONFIG}, look at the raw JSON items of the "
            "berry resource. What is the `name` of the berry whose url ends in '/berry/1/'?"
        ),
    },
    {
        "name": "pokeapi_item_count_state",
        "prompt": (
            f"Use the dlt worker to run the pokemon resource of the rest_api config {POKEAPI_CONFIG} to the end "
            "and report how many items it yielded, together with the resource's final dlt state."
        ),
    },
    {
        "name": "dlt_type_of_timestamp",
        "prompt": "Using the dlt worker, which DuckDB type does a column hinted with dlt data type 'timestamp' get?",
    },
    {
        "name": "infer_nested_object",
        "prompt": (
            'Using the dlt worker, which DuckDB type would it infer for an item value {"login": "ada", "id": 7}?'
        ),
    },
]

CATALOG_TAGS = {
    "vgi.title": "dlt Sources",
    "vgi.subtitle": "Run dlt sources - the declarative rest_api source or any @dlt.source - as DuckDB tables.",
    "vgi.doc_llm": CATALOG_DOC_LLM,
    "vgi.doc_md": CATALOG_DOC_MD,
    "vgi.source_url": SOURCE_URL,
    "vgi.icon_url": "https://query.farm/media-kit/products/vgi/vgi-logo.png",
    "vgi.license": "0BSD",
    "vgi.author": "Query.Farm",
    "vgi.copyright": "Copyright 2026 Query Farm LLC - https://query.farm",
    "vgi.keywords": keywords_array(
        "dlt, dlthub, rest api, saas, connectors, elt, extract, incremental, verified sources, json"
    ),
    "vgi.support_contact": f"{SOURCE_URL}/issues",
    "vgi.support_policy_url": f"{SOURCE_URL}#support",
    # Public analyst tasks for `vgi-lint simulate`; private graders live in vgi-agent-tests.yaml.
    "vgi.agent_test_tasks": json.dumps(AGENT_TEST_TASKS),
}
