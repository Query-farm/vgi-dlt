"""Run one dlt source operation in a child process and stream the result on stdout.

This file is executed as a script (``python runner.py``) by an interpreter that
has ``dlt`` installed: the worker's own interpreter for the built-in
``rest_api`` source and for sources found on a ``path``, or the interpreter of
an isolated, cached virtual environment for sources installed from a
``pip_url``. It must therefore import nothing from the worker package, only the
standard library and ``dlt`` (``pyarrow`` when a resource yields Arrow data).

**Request.** One JSON object on stdin::

    {"op": "discover" | "read",
     "source": "rest_api" | "package.module:function",
     "config": {...},          # rest_api config, or keyword args of the source function
     "secrets": {...},         # dlt config document from the VGI secret (in memory only)
     "paths": ["/dir", ...],   # prepended to sys.path
     "resource": "name",       # read: the resource to iterate
     "columns": ["a", "b"],    # read: keep only these top-level keys (projection)
     "initial_value": ..., "end_value": ...,   # read: incremental overrides
     "state": {...},           # read: the resource state to start from
     "max_items": 0}           # read: stop after this many items (0 = all)

Secrets reach dlt through an in-memory ``DictionaryProvider`` placed first in
the provider chain; they are never written to a file or an environment
variable.

**Response.** Frames on stdout, each ``tag (1 byte) + length (4 bytes, big
endian) + payload``:

* ``R`` a JSON array of items (dicts), at most ``ROW_CHUNK`` per frame
* ``A`` one Arrow IPC stream (a table a resource yielded natively)
* ``J`` the final JSON result (discovery output, or the end state of a read)
* ``E`` a JSON error ``{"error": message, "kind": exception class}``

Anything dlt or the source prints goes to stderr: stdout is rebound before any
source code is imported.
"""

from __future__ import annotations

import base64
import datetime as dt
import decimal
import importlib
import inspect
import json
import os
import struct
import sys
import time
import traceback
import uuid
from typing import Any

ROW_CHUNK = 500
FLUSH_SECONDS = 0.5

_OUT = os.fdopen(os.dup(1), "wb", buffering=0)
os.dup2(2, 1)
sys.stdout = sys.stderr


def _frame(tag: bytes, payload: bytes) -> None:
    _OUT.write(tag + struct.pack(">I", len(payload)) + payload)


def _default(value: Any) -> Any:
    if isinstance(value, dt.datetime | dt.date | dt.time):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, bytes | bytearray | memoryview):
        return base64.b64encode(bytes(value)).decode()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, set | frozenset | tuple):
        return list(value)
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _dumps(value: Any) -> bytes:
    return json.dumps(value, default=_default, separators=(",", ":")).encode()


# ---------------------------------------------------------------- source setup


def _install_secrets(secrets: dict[str, Any] | None) -> None:
    import dlt
    from dlt.common.configuration.providers import DictionaryProvider

    if not secrets:
        return
    provider = DictionaryProvider()
    provider._config_doc.update(secrets)
    dlt.secrets.register_provider(provider)
    # register_provider appends; move ours to the front so the secret wins.
    from dlt.common.configuration.container import Container
    from dlt.common.configuration.specs.pluggable_run_context import PluggableRunContext

    providers = Container()[PluggableRunContext].providers
    chain = providers.providers
    if chain and chain[-1] is provider:
        chain.insert(0, chain.pop())


def _load_callable(ref: str) -> Any:
    module_name, _, attr = ref.partition(":")
    if not module_name:
        raise ValueError(f"source {ref!r} must look like 'package.module:function'")
    module = importlib.import_module(module_name)
    if attr:
        obj: Any = module
        for part in attr.split("."):
            obj = getattr(obj, part)
        return obj
    from dlt.extract.decorators import DltSourceFactoryWrapper

    found = [v for k, v in vars(module).items() if not k.startswith("_") and isinstance(v, DltSourceFactoryWrapper)]
    if len(found) != 1:
        raise ValueError(
            f"module {module_name!r} defines {len(found)} dlt sources; name one as '{module_name}:<function>'"
        )
    return found[0]


def _build_source(req: dict[str, Any]) -> Any:
    import dlt
    from dlt.extract.resource import DltResource
    from dlt.extract.source import DltSource

    ref = req["source"]
    config = req.get("config") or {}
    if ref == "rest_api":
        from dlt.sources.rest_api import rest_api_source

        return rest_api_source(config)  # type: ignore[arg-type]
    target = _load_callable(ref)
    obj = target(**config) if callable(target) and not isinstance(target, DltResource | DltSource) else target
    if isinstance(obj, DltSource):
        return obj
    if isinstance(obj, DltResource):
        name = ref.partition(":")[0].rsplit(".", 1)[-1]
        resource = obj

        @dlt.source(name=name)
        def _single() -> Any:
            return resource

        return _single()
    raise ValueError(f"{ref!r} returned {type(obj).__name__}, not a dlt source or resource")


# ---------------------------------------------------------------- discovery


def _rest_api_incrementals(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Incremental settings per rest_api resource, read from the declarative config."""

    def find(endpoint: Any) -> dict[str, Any] | None:
        if not isinstance(endpoint, dict):
            return None
        inc = endpoint.get("incremental")
        if isinstance(inc, dict):
            return {"cursor_path": inc.get("cursor_path"), **inc}
        for param, spec in (endpoint.get("params") or {}).items():
            if isinstance(spec, dict) and spec.get("type") == "incremental":
                return {"start_param": param, **spec}
        return None

    defaults = find((config.get("resource_defaults") or {}).get("endpoint"))
    out: dict[str, dict[str, Any]] = {}
    for res in config.get("resources") or []:
        if not isinstance(res, dict):
            continue
        inc = find(res.get("endpoint")) or defaults
        if inc:
            out[str(res.get("name"))] = inc
    return out


def _signature_incremental(resource: Any) -> Any:
    from dlt.extract.incremental import Incremental

    wrapper = getattr(resource, "incremental", None)
    inc = getattr(wrapper, "incremental", None) if wrapper is not None else None
    if isinstance(inc, Incremental):
        return inc
    gen = getattr(getattr(resource, "_pipe", None), "gen", None)
    for fn in (gen, getattr(gen, "__wrapped__", None), getattr(gen, "func", None)):
        if fn is None or not callable(fn):
            continue
        try:
            sig = inspect.signature(fn)
        except (TypeError, ValueError):
            continue
        for param in sig.parameters.values():
            if isinstance(param.default, Incremental):
                return param.default
    return None


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    return _default(value)


def _names(value: Any) -> list[str]:
    if value is None or callable(value):
        return []
    if isinstance(value, str):
        return [value]
    return [str(v) for v in value]


def _discover(req: dict[str, Any]) -> dict[str, Any]:
    source = _build_source(req)
    rest_incs = _rest_api_incrementals(req.get("config") or {}) if req["source"] == "rest_api" else {}
    resources = []
    for name, res in source.resources.items():
        try:
            table = res.compute_table_schema()
        except Exception:  # dynamic hints need an item; fall back to the static ones
            table = {"columns": {}, "write_disposition": None, "name": name}
        columns = []
        for cname, col in (table.get("columns") or {}).items():
            columns.append(
                {
                    "name": cname,
                    "data_type": col.get("data_type"),
                    "nullable": col.get("nullable", True),
                    "precision": col.get("precision"),
                    "scale": col.get("scale"),
                    "timezone": col.get("timezone"),
                    "primary_key": bool(col.get("primary_key")),
                    "merge_key": bool(col.get("merge_key")),
                }
            )
        cursor = initial = end = None
        if name in rest_incs:
            inc = rest_incs[name]
            cursor, initial, end = inc.get("cursor_path"), inc.get("initial_value"), inc.get("end_value")
        else:
            inc_obj = _signature_incremental(res)
            if inc_obj is not None:
                cursor, initial, end = inc_obj.cursor_path, inc_obj.initial_value, inc_obj.end_value
        wd = table.get("write_disposition")
        if isinstance(wd, dict):
            wd = wd.get("disposition")
        parent = getattr(getattr(res, "_pipe", None), "parent", None)
        resources.append(
            {
                "name": name,
                "table_name": table.get("name") if isinstance(table.get("name"), str) else name,
                "selected": bool(res.selected),
                "is_transformer": bool(res.is_transformer),
                "parent": getattr(parent, "name", None),
                "write_disposition": wd if isinstance(wd, str) else None,
                "primary_key": [c["name"] for c in columns if c["primary_key"]],
                "merge_key": [c["name"] for c in columns if c["merge_key"]],
                "incremental_cursor": cursor if isinstance(cursor, str) else None,
                "incremental_initial_value": _jsonable(initial),
                "incremental_end_value": _jsonable(end),
                "columns": columns,
            }
        )
    return {"source_name": source.name, "resources": resources}


# ---------------------------------------------------------------- read


def _override_rest_api_incremental(req: dict[str, Any]) -> None:
    """Put initial_value/end_value overrides into the rest_api config itself."""
    config = req.get("config") or {}
    name = req["resource"]
    overrides = {k: req[k] for k in ("initial_value", "end_value") if req.get(k) is not None}
    if not overrides:
        return
    incs = _rest_api_incrementals(config)
    if name not in incs:
        raise ValueError(f"resource {name!r} has no incremental in its rest_api config")
    for res in config.get("resources") or []:
        if isinstance(res, dict) and res.get("name") == name:
            endpoint = res.get("endpoint")
            if not isinstance(endpoint, dict):
                endpoint = res["endpoint"] = {"path": endpoint or name}
            if isinstance(endpoint.get("incremental"), dict):
                endpoint["incremental"].update(overrides)
                return
            params = endpoint.get("params") or {}
            for param, spec in list(params.items()):
                if isinstance(spec, dict) and spec.get("type") == "incremental":
                    if "end_value" not in overrides:
                        spec.update(overrides)
                        return
                    # A param-style incremental cannot take end_value: move it to the endpoint level.
                    del params[param]
                    endpoint["incremental"] = {
                        "start_param": param,
                        **{k: v for k, v in spec.items() if k not in ("type", "start_param")},
                        **overrides,
                    }
                    return
            # Inherited from resource_defaults: copy it onto this resource.
            endpoint["incremental"] = {k: v for k, v in incs[name].items() if k != "type"} | overrides
            return


def _read(req: dict[str, Any]) -> dict[str, Any]:
    from dlt.common.configuration.container import Container
    from dlt.common.pipeline import StateInjectableContext

    name = req["resource"]
    if req["source"] == "rest_api":
        _override_rest_api_incremental(req)
    source = _build_source(req)
    if name not in source.resources:
        raise ValueError(f"source has no resource {name!r}; it has: {', '.join(source.resources) or 'none'}")
    resource = source.resources[name]
    if req["source"] != "rest_api" and (req.get("initial_value") is not None or req.get("end_value") is not None):
        import dlt

        inc = _signature_incremental(resource)
        if inc is None:
            raise ValueError(f"resource {name!r} has no incremental cursor")
        resource.apply_hints(
            incremental=dlt.sources.incremental(
                inc.cursor_path,
                initial_value=req.get("initial_value", inc.initial_value),
                end_value=req.get("end_value", inc.end_value),
                last_value_func=inc.last_value_func,
                row_order=inc.row_order,
                on_cursor_value_missing=inc.on_cursor_value_missing,
            )
        )
    state: dict[str, Any] = {"sources": {}}
    if req.get("state"):
        state["sources"][source.name] = {"resources": {name: req["state"]}}
    Container()[StateInjectableContext] = StateInjectableContext(state=state)  # type: ignore[arg-type]

    columns = req.get("columns")
    keep = set(columns) if columns is not None else None
    max_items = int(req.get("max_items") or 0)
    selected = source.with_resources(name)
    rows: list[Any] = []
    count = 0
    last_flush = time.monotonic()

    def flush() -> None:
        nonlocal rows, last_flush
        if rows:
            _frame(b"R", _dumps(rows))
            rows = []
        last_flush = time.monotonic()

    for item in selected:
        batch = item if isinstance(item, list) else [item]
        for one in batch:
            if _is_arrow(one):
                flush()
                table = _to_arrow(one, keep)
                if max_items and count + table.num_rows > max_items:
                    table = table.slice(0, max_items - count)
                _frame(b"A", _ipc(table))
                count += table.num_rows
            else:
                if keep is not None and isinstance(one, dict):
                    one = {k: v for k, v in one.items() if k in keep}
                rows.append(one)
                count += 1
                if len(rows) >= ROW_CHUNK or time.monotonic() - last_flush >= FLUSH_SECONDS:
                    flush()
            if max_items and count >= max_items:
                break
        if max_items and count >= max_items:
            break
    flush()
    res_state = state["sources"].get(source.name, {}).get("resources", {}).get(name, {})
    return {"items": count, "state": res_state, "source_name": source.name}


def _is_arrow(item: Any) -> bool:
    mod = type(item).__module__
    return mod.startswith("pyarrow") or (mod.startswith("pandas") and type(item).__name__ == "DataFrame")


def _to_arrow(item: Any, keep: set[str] | None) -> Any:
    import pyarrow as pa

    if type(item).__module__.startswith("pandas"):
        table = pa.Table.from_pandas(item, preserve_index=False)
    elif isinstance(item, pa.RecordBatch):
        table = pa.Table.from_batches([item])
    else:
        table = item
    if keep is not None:
        table = table.select([c for c in table.column_names if c in keep])
    return table


def _ipc(table: Any) -> bytes:
    import pyarrow as pa

    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return bytes(sink.getvalue().to_pybytes())


# ---------------------------------------------------------------- main


def main() -> int:
    """Read the request from stdin, run it, and write frames to stdout."""
    os.environ.setdefault("RUNTIME__DLTHUB_TELEMETRY", "false")
    req = json.loads(sys.stdin.buffer.read() or b"{}")
    try:
        for p in reversed(req.get("paths") or []):
            sys.path.insert(0, p)
        _install_secrets(req.get("secrets"))
        if req.get("op") == "discover":
            result = _discover(req)
        elif req.get("op") == "read":
            result = _read(req)
        else:
            raise ValueError(f"unknown op {req.get('op')!r}")
        _frame(b"J", _dumps(result))
        return 0
    except BaseException as exc:
        traceback.print_exc(file=sys.stderr)
        message = str(exc) or type(exc).__name__
        # dlt wraps the real cause (an HTTP error, a missing secret) in pipe exceptions.
        cause = exc.__cause__ or exc.__context__
        while cause is not None and getattr(cause, "__cause__", None) is not None:
            cause = cause.__cause__
        if cause is not None and str(cause) and str(cause) not in message:
            message = f"{message}: {cause}"
        _frame(b"E", _dumps({"error": message, "kind": type(exc).__name__}))
        return 1


if __name__ == "__main__":
    sys.exit(main())
