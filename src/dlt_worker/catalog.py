"""Attach a dlt source as a database: one table per resource.

::

    ATTACH 'dlt' AS gh (TYPE vgi, LOCATION '/path/to/vgi-dlt-worker',
                        source 'rest_api', config '/path/to/github.json',
                        columns getvariable('cols'));
    SELECT * FROM gh.issues;

ATTACH builds the source to list its resources and hints, **without** secrets
(catalog hooks never see DuckDB secrets) and without reading any items. Each
selected resource becomes a table whose columns come from its dlt column hints
plus the ``columns`` option: a JSON object ``{"resource": {"column": "TYPE"}}``
such as ``dlt.main.columns_json(...)`` returns. A resource whose columns are
still unknown is an error that says to pass ``columns``. A module source that
cannot even be built without its secret is attached from ``columns`` alone.

Every table scan is redirected to ``read_resource`` with the attach's settings
and that table's columns, so the scan resolves the ``dlt`` secret, pushes
projections and incremental filters down and resumes exactly like a direct call.

Without a ``source`` the catalog holds only the functions.
"""

from __future__ import annotations

import base64
import dataclasses
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from vgi.catalog import (
    AttachOpaqueData,
    CatalogAttachResult,
    ReadOnlyCatalogInterface,
    ScanFunctionResult,
    SchemaInfo,
    SchemaObjectType,
    SerializedSchema,
    TableInfo,
    TransactionOpaqueData,
)

from . import common
from .coltypes import parse_type, type_name
from .env import DEFAULT_TIMEOUT_SECONDS, EnvSpec, SourceError

if TYPE_CHECKING:
    from vgi import SchemaPath
    from vgi.catalog.catalog_interface import FunctionInfo, IndexInfo, MacroInfo, ViewInfo
    from vgi_rpc.rpc import CallContext

MAIN = ["main"]
_ATTACH_PREFIX = b"vgi-dlt-source:"
_KEYS = ("source", "source_path", "pip_url", "python", "secret_name", "timeout_seconds")


@dataclass(frozen=True, slots=True)
class SourceAttach:
    """The source settings of one ATTACH (no credentials: those stay in the secret)."""

    source: str
    config: str
    path: str
    pip_url: str
    python: str
    secret_name: str
    timeout_seconds: int
    tables: list[dict[str, Any]]


def _json_option(raw: str, what: str) -> dict[str, Any]:
    text = raw.strip()
    if not text:
        return {}
    if not text.startswith(("{", "[")):
        p = Path(text).expanduser()
        if not p.is_file():
            raise ValueError(f"{what} {raw[:80]!r} is neither a JSON object nor an existing file")
        text = p.read_text(encoding="utf-8")
    value = common.parse_json_arg(text, what)
    if not isinstance(value, dict):
        raise ValueError(f"{what} must be a JSON object")
    return value


def _table(name: str, schema: pa.Schema, res: dict[str, Any] | None) -> dict[str, Any]:
    return {
        "name": name,
        "schema": base64.b64encode(schema.serialize().to_pybytes()).decode(),
        "primary_key": list((res or {}).get("primary_key") or []),
        "write_disposition": (res or {}).get("write_disposition"),
        "incremental_cursor": (res or {}).get("incremental_cursor"),
    }


def source_attach(options: Mapping[str, Any]) -> SourceAttach | None:
    """Read the options and discover the tables; None when no ``source`` was given."""
    source = str(options.get("source") or "").strip()
    if not source:
        return None
    import re

    if not re.match(common.SOURCE_PATTERN, source):
        raise ValueError(f"source must be 'rest_api' or 'package.module:function', got {source!r}")
    config = _json_option(str(options.get("config") or ""), "config")
    columns = _json_option(str(options.get("columns") or ""), "columns")
    timeout = int(options.get("timeout_seconds") or DEFAULT_TIMEOUT_SECONDS)
    if timeout < 0:
        raise ValueError(f"timeout_seconds must be 0 or more, got {timeout}")
    spec = EnvSpec(
        pip_url=str(options.get("pip_url") or ""),
        python=str(options.get("python") or ""),
        path=str(options.get("source_path") or ""),
    )
    overrides = {str(r): {str(c): parse_type(str(t)) for c, t in (cols or {}).items()} for r, cols in columns.items()}
    tables: list[dict[str, Any]] = []
    try:
        args_like = {"source": source, "config": config, "secrets": {}}
        if source == "rest_api" and not config:
            raise ValueError("ATTACH with source 'rest_api' needs a config option (the rest_api JSON config)")
        found = common.discover(spec, args_like, timeout)
    except SourceError as exc:
        if not overrides:
            raise ValueError(
                f"could not build the source without its secret ({exc}); pass the columns option "
                "(from dlt.main.columns_json) so ATTACH does not need to build it"
            ) from exc
        found = {"resources": [{"name": n, "selected": True} for n in overrides]}
    missing = []
    for res in found["resources"]:
        if not res.get("selected"):
            continue
        schema, _ = common.resolve_schema(res, overrides.get(res["name"], {}), [], [])
        if not len(schema) or any(
            c.get("data_type") is None and c["name"] not in overrides.get(res["name"], {})
            for c in res.get("columns") or []
        ):
            missing.append(res["name"])
            continue
        tables.append(_table(res["name"], schema, res))
    if missing:
        raise ValueError(
            f"no column types for resource(s) {', '.join(missing)}: pass the columns option, e.g. "
            "columns getvariable('cols') after SET VARIABLE cols = (SELECT columns FROM dlt.main.columns_json(...))"
        )
    return SourceAttach(
        source=source,
        config=json.dumps(config),
        path=spec.path,
        pip_url=spec.pip_url,
        python=spec.python,
        secret_name=str(options.get("secret_name") or ""),
        timeout_seconds=timeout,
        tables=tables,
    )


def _schema(table: dict[str, Any]) -> pa.Schema:
    return pa.ipc.read_schema(pa.py_buffer(base64.b64decode(table["schema"])))


def _table_info(table: dict[str, Any], attach: SourceAttach) -> TableInfo:
    parts = [f"Resource `{table['name']}` of the dlt source {attach.source}."]
    if table["primary_key"]:
        parts.append(f"Primary key: {', '.join(table['primary_key'])}.")
    if table["incremental_cursor"]:
        parts.append(f"Incremental cursor: {table['incremental_cursor']} (filters on it push down).")
    return TableInfo(
        name=table["name"],
        schema_path=list(MAIN),
        columns=SerializedSchema(_schema(table).serialize().to_pybytes()),
        constraints=[],
        comment=" ".join(parts),
        tags={
            "vgi.title": f"Resource {table['name']}",
            "dlt.source": attach.source,
            "dlt.primary_key": json.dumps(table["primary_key"]),
            "dlt.write_disposition": str(table["write_disposition"] or ""),
            "dlt.incremental_cursor": str(table["incremental_cursor"] or ""),
        },
    )


class SourceCatalog(ReadOnlyCatalogInterface):
    """The ``dlt`` catalog: functions, plus one table per resource when a source is attached."""

    # Tables depend on the ATTACH options, so the worker must not share one
    # catalog_contents snapshot between attaches.
    catalog_contents_attach_independent = False

    def loggable_attach_options(self, options: Mapping[str, Any]) -> Mapping[str, Any]:
        """Log the source settings (config and columns are long; no credential is an attach option)."""
        return {k: options[k] for k in _KEYS if k in options}

    def catalog_attach(
        self,
        *,
        name: str,
        options: dict[str, Any],
        data_version_spec: str | None,
        implementation_version: str | None,
        client_capabilities: Any = None,
        ctx: CallContext | None = None,
    ) -> CatalogAttachResult:
        """Attach, failing early when the source options are unusable."""
        attach = source_attach(options)
        if attach is not None and not attach.tables:
            raise ValueError("the source selects no resources, so there are no tables to attach")
        return super().catalog_attach(
            name=name,
            options=options,
            data_version_spec=data_version_spec,
            implementation_version=implementation_version,
            client_capabilities=client_capabilities,
            ctx=ctx,
        )

    def attach_opaque_data_for(self, *, name: str, options: dict[str, Any]) -> AttachOpaqueData:
        """Carry the source settings and table list to every hook of this attach (no secrets in them)."""
        attach = source_attach(options)
        if attach is None:
            return super().attach_opaque_data_for(name=name, options=options)
        payload = {"v": 1, **dataclasses.asdict(attach)}
        return AttachOpaqueData(_ATTACH_PREFIX + json.dumps(payload, separators=(",", ":")).encode())

    @staticmethod
    def _attach(attach_opaque_data: AttachOpaqueData) -> SourceAttach | None:
        raw = bytes(attach_opaque_data)
        if not raw.startswith(_ATTACH_PREFIX):
            return None
        d = json.loads(raw[len(_ATTACH_PREFIX) :])
        d.pop("v", None)
        return SourceAttach(**d)

    def _tables(self, attach_opaque_data: AttachOpaqueData) -> list[tuple[TableInfo, dict[str, Any]]]:
        attach = self._attach(attach_opaque_data)
        if attach is None:
            return []
        return [(_table_info(t, attach), t) for t in attach.tables]

    def _with_table_count(self, info: SchemaInfo, attach_opaque_data: AttachOpaqueData) -> SchemaInfo:
        if [p.lower() for p in info.path] != MAIN:
            return info
        counts = dict(info.estimated_object_count or {})
        counts["table"] = len(self._tables(attach_opaque_data))
        return dataclasses.replace(info, estimated_object_count=counts)

    def schemas(
        self, *, attach_opaque_data: AttachOpaqueData, transaction_opaque_data: TransactionOpaqueData | None
    ) -> list[SchemaInfo]:
        """List the schemas, with ``main`` counting the resource tables."""
        return [
            self._with_table_count(info, attach_opaque_data)
            for info in super().schemas(
                attach_opaque_data=attach_opaque_data, transaction_opaque_data=transaction_opaque_data
            )
        ]

    def schema_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        path: SchemaPath,
    ) -> SchemaInfo | None:
        """Look up a schema, with ``main`` counting the resource tables."""
        info = super().schema_get(
            attach_opaque_data=attach_opaque_data, transaction_opaque_data=transaction_opaque_data, path=path
        )
        return None if info is None else self._with_table_count(info, attach_opaque_data)

    def schema_contents(  # type: ignore[override]
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        path: SchemaPath,
        type: SchemaObjectType,  # noqa: A002 - SDK keyword
    ) -> Sequence[TableInfo | ViewInfo | FunctionInfo | MacroInfo | IndexInfo]:
        """List the functions, plus one table per resource in ``main``."""
        base = list(
            super().schema_contents(  # type: ignore[call-overload]
                attach_opaque_data=attach_opaque_data,
                transaction_opaque_data=transaction_opaque_data,
                path=path,
                type=type,
            )
        )
        kind = type if isinstance(type, SchemaObjectType) else SchemaObjectType(type)
        if kind == SchemaObjectType.TABLE and [p.lower() for p in path] == MAIN:
            base.extend(info for info, _ in self._tables(attach_opaque_data))
        return base

    def _find(self, attach_opaque_data: AttachOpaqueData, schema_path: SchemaPath, name: str) -> Any:
        if [p.lower() for p in schema_path] != MAIN:
            return None
        for info, table in self._tables(attach_opaque_data):
            if info.name.lower() == name.lower():
                return info, table
        return None

    def table_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_path: SchemaPath,
        name: str,
        at_unit: str | None = None,
        at_value: str | None = None,
    ) -> TableInfo | None:
        """Look up a resource table by name (case-insensitive)."""
        if at_unit:
            raise ValueError("dlt resources do not support time travel (AT ...)")
        hit = self._find(attach_opaque_data, schema_path, name)
        return None if hit is None else hit[0]

    def table_scan_function_get(
        self,
        *,
        attach_opaque_data: AttachOpaqueData,
        transaction_opaque_data: TransactionOpaqueData | None,
        schema_path: SchemaPath,
        name: str,
        at_unit: str | None,
        at_value: str | None,
    ) -> ScanFunctionResult:
        """Redirect a resource table scan to ``read_resource`` with the attach's settings."""
        attach = self._attach(attach_opaque_data)
        hit = self._find(attach_opaque_data, schema_path, name)
        if attach is None or hit is None:
            raise ValueError(f"no resource table named {name!r} in this source")
        _, table = hit
        columns = {f.name: type_name(f.type) for f in _schema(table)}
        named: dict[str, pa.Scalar[Any]] = {
            "config": pa.scalar(attach.config),
            "columns": pa.scalar(json.dumps(columns)),
            "sample_rows": pa.scalar(0, pa.int64()),
            "timeout_seconds": pa.scalar(attach.timeout_seconds, pa.int64()),
        }
        for key in ("path", "pip_url", "python", "secret_name"):
            value = getattr(attach, key)
            if value:
                named[key] = pa.scalar(value)
        return ScanFunctionResult(
            function_name="read_resource",
            positional_arguments=[pa.scalar(attach.source), pa.scalar(table["name"])],
            named_arguments=named,
            required_extensions=[],
            schema_path=list(MAIN),
        )
