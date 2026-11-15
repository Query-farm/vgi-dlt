"""Discovery: ``resources``, ``infer_columns`` and ``columns_json``.

``resources`` builds the source (no items are read) and lists every resource
with its dlt hints: primary key, merge key, write disposition, incremental
cursor and the hinted columns. ``infer_columns`` additionally samples each
resource and reports the column types ``read_resource`` would use;
``columns_json`` packs the same result into the JSON object the ATTACH
``columns`` option takes.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Annotated, Any

import pyarrow as pa
from vgi.arguments import Arg
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_function import (
    BindParams,
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)

from .coltypes import JSON_TYPE, Coercer, dlt_hint_type, rows_to_batch, type_name
from .common import (
    DEFAULT_SAMPLE_ROWS,
    SourceArgs,
    base_request,
    bind_secret,
    discover,
    find_resource,
    load_config,
    process_secret,
    resolve_schema,
    sample_items,
)
from .meta import column, object_tags, result_columns
from .read_resource import DoneState

_HINT_COLUMN = pa.struct(
    [
        pa.field("name", pa.string()),
        pa.field("data_type", pa.string()),
        pa.field("duckdb_type", pa.string()),
        pa.field("nullable", pa.bool_()),
    ]
)

RESOURCES_SCHEMA = pa.schema(
    [
        column("name", pa.string(), "Resource name; pass it to read_resource.", nullable=False),
        column("table_name", pa.string(), "Table name the resource's hints declare (often the resource name)."),
        column("selected", pa.bool_(), "Whether the source selects the resource by default.", nullable=False),
        column("is_transformer", pa.bool_(), "True for a transformer fed by another resource.", nullable=False),
        column("parent", pa.string(), "The resource feeding this transformer, if any."),
        column("write_disposition", pa.string(), "append, replace or merge, from the resource hints."),
        column("primary_key", pa.list_(pa.string()), "Primary key columns from the hints.", nullable=False),
        column("merge_key", pa.list_(pa.string()), "Merge key columns from the hints.", nullable=False),
        column("incremental_cursor", pa.string(), "Column the incremental cursor tracks, if the resource has one."),
        column("incremental_initial_value", pa.string(), "The incremental cursor's default start value, as text."),
        column("incremental_end_value", pa.string(), "The incremental cursor's default end value, as text."),
        column(
            "columns",
            pa.list_(_HINT_COLUMN),
            "Hinted columns: name, dlt data_type (NULL if only partly hinted), DuckDB type, nullable.",
            nullable=False,
        ),
    ]
)


def _text(v: Any) -> str | None:
    return None if v is None else str(v) if not isinstance(v, dict | list) else json.dumps(v)


def resource_row(res: dict[str, Any]) -> dict[str, Any]:
    """One ``resources`` row from a discovery entry."""
    cols = []
    for c in res.get("columns") or []:
        t = dlt_hint_type(c)
        cols.append(
            {
                "name": c["name"],
                "data_type": c.get("data_type"),
                "duckdb_type": None if t is None else type_name(t),
                "nullable": c.get("nullable", True) is not False,
            }
        )
    return {
        "name": res["name"],
        "table_name": res.get("table_name"),
        "selected": bool(res.get("selected")),
        "is_transformer": bool(res.get("is_transformer")),
        "parent": res.get("parent"),
        "write_disposition": res.get("write_disposition"),
        "primary_key": list(res.get("primary_key") or []),
        "merge_key": list(res.get("merge_key") or []),
        "incremental_cursor": res.get("incremental_cursor"),
        "incremental_initial_value": _text(res.get("incremental_initial_value")),
        "incremental_end_value": _text(res.get("incremental_end_value")),
        "columns": cols,
    }


@init_single_worker
class Resources(TableFunctionGenerator[SourceArgs, DoneState]):
    """List the resources of a dlt source with their hints."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "resources"
        description = "List the resources of a dlt source with keys, write disposition, incremental cursor and hints"
        tags = {
            **object_tags(
                title="List dlt Resources",
                category="discovery",
                keywords="dlt, resources, discover, schema, hints, primary key, incremental, rest api",
                doc_llm=(
                    "Build a dlt source without reading any items and return one row per resource: `name` "
                    "(pass to read_resource), `table_name`, `selected`, `is_transformer`, `parent` (the resource "
                    "feeding a transformer / rest_api child resource), `write_disposition`, `primary_key`, "
                    "`merge_key`, `incremental_cursor`, `incremental_initial_value`, `incremental_end_value` and "
                    "`columns` (list of hinted columns: name, data_type, duckdb_type, nullable). Same source "
                    "arguments as read_resource."
                ),
                doc_md=(
                    "## resources(source, ...)\n\n"
                    "What a dlt source offers: its resources, their keys, incremental cursors and column hints."
                ),
            ),
            "vgi.result_columns_schema": result_columns(RESOURCES_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT name, primary_key, incremental_cursor FROM dlt.main.resources('rest_api', "
                    'config := \'{"client": {"base_url": "https://pokeapi.co/api/v2/"}, '
                    '"resources": ["pokemon", "berry"]}\') ORDER BY name'
                ),
                description="List the resources a rest_api config defines",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[SourceArgs]) -> BindResponse:
        """Resolve the secret and validate the config."""
        load_config(params.args.config)
        bind_secret(params.secrets, params.args, load_config(params.args.config))
        return BindResponse(output_schema=RESOURCES_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[SourceArgs]) -> DoneState:
        """Not started."""
        return DoneState()

    @classmethod
    def process(cls, params: ProcessParams[SourceArgs], state: DoneState, out: OutputCollector) -> None:
        """Discover the source and emit one row per resource."""
        if state.done:
            out.finish()
            return
        args = params.args
        config = load_config(args.config)
        request = base_request(args, config, process_secret(params.secrets, args, config))
        found = discover(args.env_spec(), request, args.timeout_seconds)
        rows = [resource_row(r) for r in found["resources"]]
        out.emit(rows_to_batch(rows, params.output_schema, Coercer()))
        state.done = True


# ---------------------------------------------------------------- infer_columns


@dataclass(slots=True, frozen=True, kw_only=True)
class InferArgs(SourceArgs):
    """Arguments for ``infer_columns`` and ``columns_json``."""

    resource: Annotated[
        str, Arg("resource", default="", doc="Only this resource; empty = every resource the source selects.")
    ]
    sample_rows: Annotated[
        int,
        Arg("sample_rows", default=DEFAULT_SAMPLE_ROWS, doc="Items read from each resource to infer types.", ge=0),
    ]


INFER_SCHEMA = pa.schema(
    [
        column("resource", pa.string(), "Resource name.", nullable=False),
        column("ordinal", pa.int32(), "Position of the column in the resource's table, from 1.", nullable=False),
        column("column_name", pa.string(), "Column name (the item key).", nullable=False),
        column("data_type", pa.string(), "DuckDB type read_resource uses for the column.", nullable=False),
        column("nullable", pa.bool_(), "False only for columns the hints declare NOT NULL.", nullable=False),
        column("origin", pa.string(), "'hint' (dlt column hint) or 'sample' (inferred from items).", nullable=False),
    ]
)


def _infer(params: ProcessParams[InferArgs]) -> dict[str, list[dict[str, Any]]]:
    args = params.args
    config = load_config(args.config)
    request = base_request(args, config, process_secret(params.secrets, args, config))
    spec = args.env_spec()
    found = discover(spec, request, args.timeout_seconds)
    resources = (
        [find_resource(found, args.resource)] if args.resource else [r for r in found["resources"] if r["selected"]]
    )
    out: dict[str, list[dict[str, Any]]] = {}
    for res in resources:
        rows: list[dict[str, Any]] = []
        schemas: list[pa.Schema] = []
        if args.sample_rows:
            rows, schemas = sample_items(spec, request, res["name"], args.sample_rows, args.timeout_seconds, {})
        schema, origin = resolve_schema(res, {}, rows, schemas)
        out[res["name"]] = [
            {
                "resource": res["name"],
                "ordinal": i + 1,
                "column_name": f.name,
                "data_type": type_name(f.type),
                "nullable": f.nullable,
                "origin": origin[f.name],
            }
            for i, f in enumerate(schema)
        ]
    return out


@init_single_worker
class InferColumns(TableFunctionGenerator[InferArgs, DoneState]):
    """Report the column types read_resource would use for each resource."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "infer_columns"
        description = "Sample each resource of a dlt source and report the DuckDB column types it maps to"
        tags = {
            **object_tags(
                title="Infer dlt Resource Columns",
                category="discovery",
                keywords="dlt, schema, columns, types, infer, sample, describe, hints",
                doc_llm=(
                    "For each selected resource (or only `resource`), combine the dlt column hints with types "
                    "inferred from the first `sample_rows` items and return one row per column: `resource`, "
                    "`ordinal`, `column_name`, `data_type` (DuckDB type), `nullable` and `origin` ('hint' or "
                    "'sample'). These are exactly the columns read_resource returns. Same source arguments as "
                    "read_resource."
                ),
                doc_md=(
                    "## infer_columns(source, resource := '', sample_rows := 100)\n\n"
                    "The table each resource becomes, column by column, before you read it."
                ),
            ),
            "vgi.result_columns_schema": result_columns(INFER_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT column_name, data_type FROM dlt.main.infer_columns('rest_api', resource := 'pokemon', "
                    'config := \'{"client": {"base_url": "https://pokeapi.co/api/v2/"}, '
                    '"resources": ["pokemon"]}\') ORDER BY ordinal'
                ),
                description="The column types a public REST endpoint's items map to",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[InferArgs]) -> BindResponse:
        """Resolve the secret and validate the config."""
        bind_secret(params.secrets, params.args, load_config(params.args.config))
        return BindResponse(output_schema=INFER_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[InferArgs]) -> DoneState:
        """Not started."""
        return DoneState()

    @classmethod
    def process(cls, params: ProcessParams[InferArgs], state: DoneState, out: OutputCollector) -> None:
        """Sample the resources and emit one row per column."""
        if state.done:
            out.finish()
            return
        rows = [row for cols in _infer(params).values() for row in cols]
        out.emit(rows_to_batch(rows, params.output_schema, Coercer()))
        state.done = True


COLUMNS_JSON_SCHEMA = pa.schema(
    [
        column(
            "columns",
            JSON_TYPE,
            'JSON object {"resource": {"column": "DUCKDB TYPE", ...}, ...} for the ATTACH columns option.',
            nullable=False,
        ),
    ]
)


@init_single_worker
class ColumnsJson(TableFunctionGenerator[InferArgs, DoneState]):
    """The inferred columns of every resource as the JSON the ATTACH ``columns`` option takes."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "columns_json"
        description = "Infer every resource's columns and return them as JSON for the ATTACH columns option"
        tags = {
            **object_tags(
                title="Columns JSON for ATTACH",
                category="discovery",
                keywords="dlt, attach, columns, schema, json, catalog, tables",
                doc_llm=(
                    "Same inference as infer_columns, returned as one row with one JSON value: "
                    '{"resource": {"column": "DUCKDB TYPE", ...}, ...}. Store it with SET VARIABLE and pass it '
                    "as the `columns` option of ATTACH (TYPE vgi, source ..., config ..., columns ...) so every "
                    "resource becomes a table with fixed columns, even when ATTACH cannot read the API "
                    "(ATTACH never sees secrets)."
                ),
                doc_md=(
                    "## columns_json(source, ...)\n\n"
                    "Inferred columns for every resource, as the JSON the ATTACH `columns` option expects."
                ),
            ),
            "vgi.result_columns_schema": result_columns(COLUMNS_JSON_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT json_keys(columns) AS resources FROM dlt.main.columns_json('rest_api', "
                    'config := \'{"client": {"base_url": "https://pokeapi.co/api/v2/"}, '
                    '"resources": ["pokemon", "berry"]}\')'
                ),
                description="Which resources the inferred ATTACH columns cover",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[InferArgs]) -> BindResponse:
        """Resolve the secret and validate the config."""
        bind_secret(params.secrets, params.args, load_config(params.args.config))
        return BindResponse(output_schema=COLUMNS_JSON_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[InferArgs]) -> DoneState:
        """Not started."""
        return DoneState()

    @classmethod
    def process(cls, params: ProcessParams[InferArgs], state: DoneState, out: OutputCollector) -> None:
        """Sample the resources and emit the JSON object."""
        if state.done:
            out.finish()
            return
        doc = {name: {c["column_name"]: c["data_type"] for c in cols} for name, cols in _infer(params).items()}
        out.emit(rows_to_batch([{"columns": doc}], params.output_schema, Coercer()))
        state.done = True
