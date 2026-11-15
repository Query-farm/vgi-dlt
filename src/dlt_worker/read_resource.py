"""``read_resource``, ``read_items`` and ``final_state``: iterate one resource of a dlt source.

No dlt pipeline or destination is involved: the resource is iterated directly
(``source.with_resources(name)``) in a runner child process, and its items are
streamed back in batches of up to 1,000. Items a resource yields as Arrow
tables (or pandas frames) stay Arrow; dict items are typed with the bound
schema (see ``types.py``).

**Pushdown.** Projection: only the columns the query uses are kept in the
runner (an ``add_map``-style key filter before the items are serialized).
Filters: a ``>=`` / ``>`` (or ``BETWEEN``) bound on the resource's incremental
cursor column becomes the incremental ``initial_value``, so a rest_api resource
sends it as its ``start_param`` (e.g. ``?updated_since=...``) and a module
source starts its incremental there. DuckDB still re-applies every filter, so
the result is exact either way.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
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
from vgi_rpc import ArrowSerializableDataclass

from .coltypes import JSON_TYPE, Coercer, rows_to_batch
from .common import (
    DEFAULT_SAMPLE_ROWS,
    ScanCursor,
    SourceArgs,
    base_request,
    bind_secret,
    discover,
    find_resource,
    frames_to_batch,
    load_config,
    parse_columns_arg,
    parse_json_arg,
    process_secret,
    read_frames,
    resolve_schema,
    sample_items,
)
from .meta import DYNAMIC_COLUMNS_MD, column, object_tags, result_columns

log = logging.getLogger(__name__)

_PLACEHOLDER = pa.schema([pa.field("pending", pa.string())])


@dataclass(slots=True, frozen=True, kw_only=True)
class ResourceArgs(SourceArgs):
    """Arguments of the functions that iterate one resource."""

    resource: Annotated[str, Arg(1, doc="Name of the resource to read, as listed by dlt.main.resources.")]
    initial_value: Annotated[
        str,
        Arg(
            "initial_value",
            default="",
            doc="Start the resource's incremental cursor here (overrides its initial_value). A JSON number "
            "is passed as a number, anything else as a string such as '2026-01-01T00:00:00Z'.",
        ),
    ]
    end_value: Annotated[
        str,
        Arg("end_value", default="", doc="Stop the incremental cursor before this value (exclusive); empty = none."),
    ]
    state: Annotated[
        str,
        Arg(
            "state",
            default="",
            doc="Resource state JSON to resume from, e.g. the state column of dlt.main.final_state; empty = fresh.",
        ),
    ]


@dataclass(slots=True, frozen=True, kw_only=True)
class ReadResourceArgs(ResourceArgs):
    """Arguments for ``read_resource``."""

    columns: Annotated[
        str,
        Arg(
            "columns",
            default="",
            doc="JSON object mapping column names to type names (DuckDB or dlt); these take precedence over the "
            "resource's hints and over inference.",
        ),
    ]
    sample_rows: Annotated[
        int,
        Arg(
            "sample_rows",
            default=DEFAULT_SAMPLE_ROWS,
            doc="Items read at bind time to infer columns the resource's hints do not type; 0 = hints and "
            "columns only.",
            ge=0,
        ),
    ]


def _scalar_arg(text: str) -> Any:
    if not text:
        return None
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        return text
    return value if isinstance(value, int | float) and not isinstance(value, bool) else text


def incremental_overrides(args: ResourceArgs) -> dict[str, Any]:
    """Return the incremental/state part of a read request taken from the arguments."""
    extra: dict[str, Any] = {}
    if args.initial_value:
        extra["initial_value"] = _scalar_arg(args.initial_value)
    if args.end_value:
        extra["end_value"] = _scalar_arg(args.end_value)
    if args.state:
        state = parse_json_arg(args.state, "state")
        if not isinstance(state, dict):
            raise ValueError("state must be a JSON object (a resource state)")
        extra["state"] = state
    return extra


def _render(value: Any) -> Any:
    if isinstance(value, dt.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(dt.UTC).replace(tzinfo=None)
        text = value.isoformat(timespec="microseconds" if value.microsecond else "seconds")
        return text + "Z"
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, int | float | str) and not isinstance(value, bool):
        return value
    return None


def _later(a: Any, b: Any) -> Any:
    """Return the later of two incremental start values (``a`` when they are not comparable)."""
    if a is None:
        return b
    if isinstance(a, int | float) and isinstance(b, int | float):
        return max(a, b)
    if isinstance(a, str) and isinstance(b, str):
        try:
            da = dt.datetime.fromisoformat(a.replace("Z", "+00:00"))
            db = dt.datetime.fromisoformat(b.replace("Z", "+00:00"))
            if da.tzinfo is None:
                da = da.replace(tzinfo=dt.UTC)
            if db.tzinfo is None:
                db = db.replace(tzinfo=dt.UTC)
            return b if db > da else a
        except ValueError:
            return a
    return a


def pushed_initial_value(params: ProcessParams[Any], cursor_column: str | None, configured: Any) -> Any:
    """Return a start value implied by a lower bound on the incremental cursor column, or None."""
    filters = params.current_pushdown_filters
    if not cursor_column or filters is None:
        return None
    try:
        bounds = filters.get_column_bounds(cursor_column)
    except Exception:
        return None
    if bounds is None or bounds.min_value is None:
        return None
    pushed = _render(bounds.min_value.as_py())
    if pushed is None:
        return None
    chosen = _later(configured, pushed)
    return chosen if chosen is pushed else None


@dataclass(kw_only=True)
class _Bound:
    resource: str
    cursor: str | None
    initial: Any


def _bind_opaque(res: dict[str, Any]) -> bytes:
    return json.dumps(
        {
            "resource": res["name"],
            "cursor": res.get("incremental_cursor"),
            "initial": res.get("incremental_initial_value"),
        }
    ).encode()


def _bound(params: ProcessParams[Any]) -> _Bound:
    assert params.init_call is not None and params.init_call.bind_opaque_data
    d = json.loads(params.init_call.bind_opaque_data)
    return _Bound(resource=d["resource"], cursor=d.get("cursor"), initial=d.get("initial"))


def _read_request(params: ProcessParams[Any], bound: _Bound, columns: list[str] | None) -> dict[str, Any]:
    args = params.args
    config = load_config(args.config)
    request = base_request(args, config, process_secret(params.secrets, args, config))
    extra = incremental_overrides(args)
    configured = extra.get("initial_value", bound.initial)
    pushed = pushed_initial_value(params, bound.cursor, configured)
    if pushed is not None:
        log.info("dlt: pushing %s >= %r down as the incremental initial_value", bound.cursor, pushed)
        extra["initial_value"] = pushed
    return {**request, **extra, "resource": bound.resource, "columns": columns}


@init_single_worker
class ReadResource(TableFunctionGenerator[ReadResourceArgs, ScanCursor]):
    """Iterate one resource of a dlt source and stream it as typed rows."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "read_resource"
        description = "Run a dlt source (rest_api or any @dlt.source) and return one resource as a typed table"
        projection_pushdown = True
        filter_pushdown = True
        # Pushed filters only narrow the incremental start; re-apply every one exactly.
        auto_apply_filters = True
        tags = {
            **object_tags(
                title="Read dlt Resource",
                category="sources",
                keywords="dlt, dlthub, rest api, resource, source, extract, elt, incremental, pagination",
                doc_llm=(
                    "Iterate one resource of a dlt source and return its items as a table (no pipeline, no "
                    "destination). `source` is 'rest_api' (then `config` is the dlt rest_api JSON config: "
                    "client.base_url, client.paginator, resources[].endpoint) or 'package.module:function' for "
                    "any @dlt.source (then `config` is its keyword arguments; import it from `path` or install "
                    "it with `pip_url` into an isolated cached env). `resource` is the resource name. Columns "
                    "come from `columns` (JSON {name: type}), the resource's dlt column hints, then inference "
                    "from the first `sample_rows` items: integers -> `BIGINT`, floats -> `DOUBLE`, ISO timestamps "
                    "-> `TIMESTAMPTZ`, objects -> `STRUCT`, lists -> `LIST`, mixed -> `JSON`. Credentials come from a "
                    "`dlt` secret (CONFIG JSON/TOML), never the query. Incremental: `initial_value`, "
                    "`end_value`, `state` (from final_state); a WHERE <cursor> >= X filter becomes the "
                    "incremental start (sent as the rest_api start_param)."
                ),
                doc_md=(
                    "## read_resource(source, resource, ...)\n\n"
                    "One resource of a dlt source as a typed table.\n\n"
                    "Use dlt's declarative `rest_api` source with a JSON config, or any installable "
                    "`@dlt.source`. Credentials live in a `dlt` secret."
                ),
            ),
            "vgi.result_dynamic_columns_md": DYNAMIC_COLUMNS_MD,
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT name, stargazers_count FROM dlt.main.read_resource('rest_api', 'repos', "
                    'config := \'{"client": {"base_url": "https://api.github.com/"}, "resources": '
                    '[{"name": "repos", "endpoint": {"path": "orgs/dlt-hub/repos"}}]}\') '
                    "ORDER BY stargazers_count DESC LIMIT 5"
                ),
                description="The five most-starred repositories of a GitHub organization via the rest_api source",
            ),
            FunctionExample(
                sql=(
                    "SELECT count(*) AS issues FROM dlt.main.read_resource('github:github_reactions', 'issues', "
                    'path := \'/srv/dlt_project\', config := \'{"owner": "dlt-hub", "name": "dlt"}\') '
                    "WHERE updated_at >= TIMESTAMPTZ '2026-01-01'"
                ),
                description="Count recently updated issues with a `dlt init github` verified source",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[ReadResourceArgs]) -> BindResponse:
        """Discover the resource, sample it if needed, and publish its columns."""
        args = params.args
        config = load_config(args.config)
        secret = bind_secret(params.secrets, args, config)
        if secret is None:
            return BindResponse(output_schema=_PLACEHOLDER)
        overrides = parse_columns_arg(args.columns)
        extra = incremental_overrides(args)
        request = base_request(args, config, secret)
        spec = args.env_spec()
        res = find_resource(discover(spec, request, args.timeout_seconds), args.resource)
        rows: list[dict[str, Any]] = []
        schemas: list[pa.Schema] = []
        if args.sample_rows:
            rows, schemas = sample_items(spec, request, res["name"], args.sample_rows, args.timeout_seconds, extra)
        schema, _ = resolve_schema(res, overrides, rows, schemas)
        if not len(schema):
            raise ValueError(
                f"resource {res['name']!r} has no column hints and its sample was empty; "
                'pass columns := \'{"id": "BIGINT", ...}\' or a larger sample_rows'
            )
        return BindResponse(output_schema=schema, opaque_data=_bind_opaque(res))

    @classmethod
    def initial_state(cls, params: ProcessParams[ReadResourceArgs]) -> ScanCursor:
        """Start a fresh scan."""
        return ScanCursor()

    @classmethod
    def process(cls, params: ProcessParams[ReadResourceArgs], state: ScanCursor, out: OutputCollector) -> None:
        """Read the next items of the resource and emit them as one batch."""
        if state.done:
            out.finish()
            return
        bound = _bound(params)
        schema = params.output_schema
        request = _read_request(params, bound, list(schema.names))
        coercer = Coercer()
        frames = read_frames(
            state,
            spec=params.args.env_spec(),
            request=request,
            timeout_seconds=params.args.timeout_seconds,
            label=f"resource {bound.resource}",
        )
        batch = frames_to_batch(frames, schema, coercer)
        if batch is not None and batch.num_rows:
            out.emit(batch)
        elif state.done:
            out.finish()
            return
        if coercer.rejected:
            log.warning("dlt: %d values did not match their column type and were set to NULL", coercer.rejected)


# ---------------------------------------------------------------- read_items

ITEMS_SCHEMA = pa.schema(
    [
        column("item", JSON_TYPE, "One item exactly as the resource yielded it, as a JSON object.", nullable=False),
    ]
)


@init_single_worker
class ReadItems(TableFunctionGenerator[ResourceArgs, ScanCursor]):
    """Iterate one resource and return every item as untyped JSON."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "read_items"
        description = "Return each item of a dlt resource as one JSON value, without typing or sampling"
        tags = {
            **object_tags(
                title="Read dlt Items as JSON",
                category="sources",
                keywords="dlt, json, items, raw, explore, rest api, resource",
                doc_llm=(
                    "Like read_resource but returns one row per item with a single `item` JSON column, exactly "
                    "as the resource yielded it (no column hints, no sampling, no type coercion). Use it to "
                    "explore an unfamiliar API response, or for items whose shape changes from item to item; "
                    "extract fields with `item->>'$.field'`. Same arguments as read_resource except `columns` "
                    "and `sample_rows`."
                ),
                doc_md=(
                    "## read_items(source, resource, ...)\n\n"
                    "Every item of a resource as raw JSON, for exploring an API before typing it."
                ),
            ),
            "vgi.result_columns_schema": result_columns(ITEMS_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT item->>'$.login' AS login FROM dlt.main.read_items('rest_api', 'members', "
                    'config := \'{"client": {"base_url": "https://api.github.com/"}, "resources": '
                    '[{"name": "members", "endpoint": {"path": "orgs/dlt-hub/public_members"}}]}\') '
                    "ORDER BY login LIMIT 10"
                ),
                description="Explore raw GitHub API items and pull one field out of the JSON",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[ResourceArgs]) -> BindResponse:
        """Resolve the secret and check the resource exists."""
        args = params.args
        config = load_config(args.config)
        secret = bind_secret(params.secrets, args, config)
        if secret is not None:
            incremental_overrides(args)
            res = find_resource(
                discover(args.env_spec(), base_request(args, config, secret), args.timeout_seconds), args.resource
            )
            return BindResponse(output_schema=ITEMS_SCHEMA, opaque_data=_bind_opaque(res))
        return BindResponse(output_schema=ITEMS_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[ResourceArgs]) -> ScanCursor:
        """Start a fresh scan."""
        return ScanCursor()

    @classmethod
    def process(cls, params: ProcessParams[ResourceArgs], state: ScanCursor, out: OutputCollector) -> None:
        """Read the next items and emit them as JSON."""
        if state.done:
            out.finish()
            return
        bound = _bound(params)
        frames = read_frames(
            state,
            spec=params.args.env_spec(),
            request=_read_request(params, bound, None),
            timeout_seconds=params.args.timeout_seconds,
            label=f"resource {bound.resource}",
        )
        items: list[dict[str, Any]] = []
        for frame in frames:
            rows = frame.to_pylist() if isinstance(frame, pa.Table) else frame
            items.extend({"item": r} for r in rows)
        if items:
            out.emit(rows_to_batch(items, params.output_schema, Coercer()))
        elif state.done:
            out.finish()


# ---------------------------------------------------------------- final_state


@dataclass(kw_only=True)
class DoneState(ArrowSerializableDataclass):
    """Cursor: whether the single result row was produced."""

    done: bool = False


FINAL_STATE_SCHEMA = pa.schema(
    [
        column("resource", pa.string(), "The resource that was read.", nullable=False),
        column("items", pa.int64(), "Items the resource yielded in this run.", nullable=False),
        column("incremental_cursor", pa.string(), "The resource's incremental cursor column, if any."),
        column("last_value", pa.string(), "The incremental cursor's last value after the run, if any."),
        column(
            "state",
            JSON_TYPE,
            "The resource state after the run (incremental last_value and dedup hashes); pass it as `state`.",
            nullable=False,
        ),
    ]
)


@init_single_worker
class FinalState(TableFunctionGenerator[ResourceArgs, DoneState]):
    """Run a resource to the end and return its incremental state."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "final_state"
        description = "Run a dlt resource to the end and return its incremental state and item count"
        tags = {
            **object_tags(
                title="Final dlt Resource State",
                category="state",
                keywords="dlt, state, incremental, last_value, cursor, bookmark, resume, checkpoint",
                doc_llm=(
                    "Iterate a resource to completion (items are counted, not returned) and return one row: "
                    "`resource`, `items`, `incremental_cursor`, `last_value` (the cursor's maximum, as text) and "
                    "`state` (JSON resource state: incremental last_value, start_value, unique_hashes). Pass "
                    "`state` to read_resource / final_state next time to read only new items. Same arguments as "
                    "read_items; `initial_value` / `state` choose where this run starts."
                ),
                doc_md=(
                    "## final_state(source, resource, ...)\n\n"
                    "Run a resource and keep only its incremental state, for incremental loads: pass the returned "
                    "`state` as `state` next time."
                ),
            ),
            "vgi.result_columns_schema": result_columns(FINAL_STATE_SCHEMA),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT items, last_value FROM dlt.main.final_state('rest_api', 'issues', "
                    'config := \'{"client": {"base_url": "https://api.github.com/repos/dlt-hub/dlt/"}, '
                    '"resources": [{"name": "issues", "endpoint": {"path": "issues", "params": {"since": '
                    '{"type": "incremental", "cursor_path": "updated_at", "initial_value": '
                    '"2026-01-01T00:00:00Z"}}}}]}\')'
                ),
                description="Read GitHub issues updated since a date and keep the cursor to resume from",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[ResourceArgs]) -> BindResponse:
        """Resolve the secret and find the resource."""
        args = params.args
        config = load_config(args.config)
        secret = bind_secret(params.secrets, args, config)
        if secret is not None:
            incremental_overrides(args)
            res = find_resource(
                discover(args.env_spec(), base_request(args, config, secret), args.timeout_seconds), args.resource
            )
            return BindResponse(output_schema=FINAL_STATE_SCHEMA, opaque_data=_bind_opaque(res))
        return BindResponse(output_schema=FINAL_STATE_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[ResourceArgs]) -> DoneState:
        """Not started."""
        return DoneState()

    @classmethod
    def process(cls, params: ProcessParams[ResourceArgs], state: DoneState, out: OutputCollector) -> None:
        """Run the resource to the end and emit the one result row."""
        if state.done:
            out.finish()
            return
        bound = _bound(params)
        cursor = ScanCursor()
        while not cursor.done:
            # Only the count matters; read through without keeping items.
            read_frames(
                cursor,
                spec=params.args.env_spec(),
                request=_read_request(params, bound, []),
                timeout_seconds=params.args.timeout_seconds,
                label=f"resource {bound.resource}",
                max_items=10_000,
            )
        result = json.loads(cursor.result or "{}")
        res_state = result.get("state") or {}
        last = None
        if bound.cursor:
            last = ((res_state.get("incremental") or {}).get(bound.cursor) or {}).get("last_value")
        row = {
            "resource": bound.resource,
            "items": int(result.get("items", cursor.emitted)),
            "incremental_cursor": bound.cursor,
            "last_value": None if last is None else str(last),
            "state": res_state,
        }
        out.emit(rows_to_batch([row], params.output_schema, Coercer()))
        state.done = True
