"""Arguments, the ``dlt`` secret, discovery, column resolution and the resumable scan engine.

**Sources.** ``source`` is ``'rest_api'`` (dlt's declarative REST API source;
``config`` is its JSON config: client, auth, paginator, resources) or a
reference ``'package.module:function'`` to any ``@dlt.source`` (or
``@dlt.resource``) function; ``config`` is then a JSON object of keyword
arguments for it. Module sources are imported from ``path`` (a directory, such
as a ``dlt init`` project) and/or installed from ``pip_url`` into an isolated,
cached environment (see ``env.py``).

**Secrets.** Credentials come from a DuckDB secret of type ``dlt``::

    CREATE SECRET gh (TYPE dlt, CONFIG '{"client": {"auth": {"token": "..."}}}',
                      SCOPE 'dlt://rest_api/api.github.com');

``CONFIG`` is a JSON object or TOML text. For ``rest_api`` it is deep-merged
into the source config (so the token stays out of the query text); for every
source it is also handed to dlt as an in-memory config provider, exactly like a
``secrets.toml`` (``[sources.github] access_token = "..."``). The worker asks
for the ``dlt`` secret whose scope best matches ``dlt://rest_api/<base_url
without scheme>`` or ``dlt://<package>/<module>/<function>``; a secret without a
scope applies everywhere; ``secret_name`` picks one by name.

**Resumable scans.** The scan cursor (``ScanCursor``) holds a random token and
the number of items already returned, never config or secrets. In one worker
process the live runner simply continues between batches. When a batch lands
on another process (HTTP), the source is started again with the same request
(same incremental start and state) and the items already returned are
skipped. That relies on the source returning the same items in the same order
for the same request, as paginated APIs ordered by an incremental cursor do.
"""

from __future__ import annotations

import copy
import hashlib
import json
import threading
import time
import tomllib
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any
from urllib.parse import urlparse

import pyarrow as pa
from vgi.arguments import Arg
from vgi.catalog import SecretTypeSpec
from vgi_rpc import ArrowSerializableDataclass

from . import env
from .coltypes import (
    Coercer,
    dlt_hint_type,
    infer_fields,
    normalize_arrow_type,
    parse_type,
    rows_to_batch,
    table_to_batch,
)
from .env import EnvSpec, Run, SourceError

SECRET_TYPE = "dlt"
SCOPE_PREFIX = "dlt://"
BATCH_ROWS = 1_000
FLUSH_SECONDS = 2.0
DEFAULT_SAMPLE_ROWS = 100
DISCOVERY_CACHE_SECONDS = 300
DOC_URL = "https://github.com/Query-farm/vgi-dlt#secrets"
SOURCE_PATTERN = r"^(rest_api|[A-Za-z_][A-Za-z0-9_.]*(:[A-Za-z_][A-Za-z0-9_.]*)?)$"

DLT_SECRET_TYPE = SecretTypeSpec(
    name=SECRET_TYPE,
    description="Credentials and config for a dlt source, kept in memory and handed to dlt's config resolution.",
    schema=pa.schema(
        [
            pa.field(
                "config",
                pa.string(),
                metadata={
                    "redact": "true",
                    "vgi_required": "true",
                    "vgi_doc": (
                        "A JSON object or TOML text. For the rest_api source it is deep-merged into the REST "
                        'config (e.g. {"client": {"auth": {"type": "bearer", "token": "..."}}}); for every '
                        "source it is also read like dlt's secrets.toml (e.g. [sources.github] access_token). "
                        "Bind it with SCOPE 'dlt://rest_api/<host>' or 'dlt://<package>/<module>/<function>' "
                        "(or a prefix of it that ends at a '/')."
                    ),
                },
            )
        ],
        metadata={"vgi_doc_url": DOC_URL},
    ),
)


@dataclass(slots=True, frozen=True, kw_only=True)
class SourceArgs:
    """Arguments every source-running function takes."""

    source: Annotated[
        str,
        Arg(
            0,
            doc="'rest_api' for dlt's declarative REST API source, or 'package.module:function' naming a "
            "@dlt.source function.",
            pattern=SOURCE_PATTERN,
        ),
    ]
    config: Annotated[
        str,
        Arg(
            "config",
            default="",
            doc="JSON: the rest_api config (client, paginator, resources), or keyword arguments for a module "
            "source function. A path to a .json file also works. Put credentials in a dlt secret instead.",
        ),
    ]
    path: Annotated[
        str,
        Arg("path", default="", doc="Directory to import the source module from, e.g. a `dlt init` project."),
    ]
    pip_url: Annotated[
        str,
        Arg(
            "pip_url",
            default="",
            doc="pip requirement(s) providing the source, installed once into an isolated cached environment "
            "with dlt, e.g. 'dlt-source-foo==1.0' or a git URL.",
        ),
    ]
    python: Annotated[
        str, Arg("python", default="", doc="Python version for the pip_url environment, e.g. '3.12'; empty = default.")
    ]
    secret_name: Annotated[
        str,
        Arg(
            "secret_name",
            default="",
            doc="Name of the dlt secret to use; empty picks the one whose SCOPE best matches the source.",
        ),
    ]
    timeout_seconds: Annotated[
        int,
        Arg(
            "timeout_seconds",
            default=env.DEFAULT_TIMEOUT_SECONDS,
            doc="Stop the source if it produces nothing for this many seconds; 0 waits forever.",
            ge=0,
        ),
    ]

    def env_spec(self) -> EnvSpec:
        """Return the environment the source runs in."""
        return EnvSpec(pip_url=self.pip_url, python=self.python, path=self.path)


# ---------------------------------------------------------------- config + secret


def parse_json_arg(text: str, what: str) -> Any:
    """Decode a JSON argument, naming it in the error."""
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{what} is not valid JSON: {exc}") from exc


def load_config(raw: str) -> dict[str, Any]:
    """Parse the ``config`` argument into a dict: JSON text, or a path to a JSON file."""
    text = raw.strip()
    if not text:
        return {}
    if not text.startswith(("{", "[")):
        p = Path(text).expanduser()
        if not p.is_file():
            raise ValueError(f"config {raw[:80]!r} is neither a JSON object nor an existing file")
        text = p.read_text(encoding="utf-8")
    value = parse_json_arg(text, "config")
    if not isinstance(value, dict):
        raise ValueError("config must be a JSON object")
    return value


def scope_for(source: str, config: Mapping[str, Any]) -> str:
    """Return the secret scope path a source asks for."""
    if source == "rest_api":
        base = str(((config.get("client") or {}) if isinstance(config.get("client"), dict) else {}).get("base_url", ""))
        u = urlparse(base)
        where = (u.netloc + u.path).rstrip("/") if u.netloc else base
        return f"{SCOPE_PREFIX}rest_api/{where}"
    # 'package.module:function' -> 'dlt://package/module/function'. Since DuckDB
    # 2.0.1 (duckdb#28684) an authority-only scope such as 'dlt://github' matches
    # only up to a '/', so 'dlt://github.repos:src' would not be covered by it.
    return SCOPE_PREFIX + source.replace(":", ".").replace(".", "/")


def _secret_doc(secret: Mapping[str, Any] | None) -> dict[str, Any]:
    if secret is None:
        return {}
    value = secret.get("config")
    text = value.as_py() if value is not None and hasattr(value, "as_py") else value
    if not text:
        return {}
    text = str(text)
    try:
        doc = json.loads(text) if text.lstrip().startswith("{") else tomllib.loads(text)
    except (json.JSONDecodeError, tomllib.TOMLDecodeError) as exc:
        # Never echo the value: it is a credential.
        raise ValueError(f"the dlt secret's CONFIG is neither a JSON object nor TOML ({type(exc).__name__})") from None
    if not isinstance(doc, dict):
        raise ValueError("the dlt secret's CONFIG must be a JSON object or TOML table")
    return doc


def bind_secret(secrets: Any, args: SourceArgs, config: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return the secret's config document at bind time, or None while DuckDB still has to resolve it."""
    if args.secret_name:
        secret = secrets.get(SECRET_TYPE, name=args.secret_name)
        if secret is None and secrets.all_resolved:
            raise ValueError(f"no dlt secret named {args.secret_name!r}")
    else:
        secret = secrets.get(SECRET_TYPE, scope=scope_for(args.source, config))
    if not secrets.all_resolved:
        return None
    return _secret_doc(secret)


def process_secret(secrets: Any, args: SourceArgs, config: Mapping[str, Any]) -> dict[str, Any]:
    """Return the secret's config document while scanning (resolved at bind)."""
    if args.secret_name:
        return _secret_doc(secrets.lookup(SECRET_TYPE, name=args.secret_name))
    return _secret_doc(secrets.lookup(SECRET_TYPE, scope=scope_for(args.source, config)))


def deep_merge(base: dict[str, Any], extra: Mapping[str, Any]) -> dict[str, Any]:
    """Merge ``extra`` into a copy of ``base`` (objects merge, everything else replaces)."""
    out = copy.deepcopy(base)
    for k, v in extra.items():
        if isinstance(v, Mapping) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def base_request(args: SourceArgs, config: dict[str, Any], secret: dict[str, Any]) -> dict[str, Any]:
    """Build the part of a runner request every operation shares (holds the secret: never persist it)."""
    if args.source == "rest_api":
        if not config:
            raise ValueError("the rest_api source needs a config: client (base_url, auth, paginator) and resources")
        config = deep_merge(config, secret)
    return {"source": args.source, "config": config, "secrets": secret}


# ---------------------------------------------------------------- discovery

_cache: dict[str, tuple[float, Any]] = {}
_cache_lock = threading.Lock()


def _cached(key_parts: list[Any], compute: Any) -> Any:
    key = hashlib.sha256(json.dumps(key_parts, sort_keys=True, default=str).encode()).hexdigest()
    with _cache_lock:
        hit = _cache.get(key)
        if hit and time.monotonic() - hit[0] < DISCOVERY_CACHE_SECONDS:
            return copy.deepcopy(hit[1])
    value = compute()
    with _cache_lock:
        _cache[key] = (time.monotonic(), value)
    return copy.deepcopy(value)


def discover(spec: EnvSpec, request: dict[str, Any], timeout_seconds: int) -> dict[str, Any]:
    """List the source's resources and their hints (cached briefly per source, config and secret)."""
    req = {**request, "op": "discover"}

    def compute() -> dict[str, Any]:
        return env.run_to_result(spec, req, timeout_seconds=timeout_seconds, label=f"source {request['source']}")

    result: dict[str, Any] = _cached(["discover", spec.key, spec.path, req], compute)
    return result


def find_resource(discovery: dict[str, Any], name: str) -> dict[str, Any]:
    """Return the discovery entry of one resource (case-insensitive)."""
    for res in discovery["resources"]:
        if res["name"] == name:
            return dict(res)
    for res in discovery["resources"]:
        if res["name"].lower() == name.lower():
            return dict(res)
    names = ", ".join(r["name"] for r in discovery["resources"]) or "none"
    raise ValueError(f"the source has no resource {name!r}; it has: {names}")


def sample_items(
    spec: EnvSpec, request: dict[str, Any], resource: str, sample_rows: int, timeout_seconds: int, extra: dict[str, Any]
) -> tuple[list[dict[str, Any]], list[pa.Schema]]:
    """Return the first ``sample_rows`` items of a resource (dict items, and the schemas of Arrow items)."""
    req = {**request, **extra, "op": "read", "resource": resource, "max_items": sample_rows, "columns": None}

    def compute() -> tuple[list[dict[str, Any]], list[bytes]]:
        run = Run.start(spec, req, timeout_seconds=timeout_seconds, label=f"resource {resource}")
        rows: list[dict[str, Any]] = []
        schemas: list[bytes] = []
        try:
            while (frame := run.poll(1.0)) is not None:
                if frame is False:
                    continue
                assert not isinstance(frame, bool)
                if frame.rows is not None:
                    rows.extend(r for r in frame.rows if isinstance(r, dict))
                elif frame.table is not None:
                    schemas.append(frame.table.schema.serialize().to_pybytes())
        finally:
            run.close()
        return rows, schemas

    rows, raw_schemas = _cached(["sample", spec.key, spec.path, req], compute)
    return rows, [pa.ipc.read_schema(pa.py_buffer(s)) for s in raw_schemas]


def parse_columns_arg(text: str) -> dict[str, pa.DataType]:
    """Parse the ``columns`` override: ``{"name": "TYPE"}`` (DuckDB or dlt type names)."""
    if not text.strip():
        return {}
    value = parse_json_arg(text, "columns")
    if not isinstance(value, dict):
        raise ValueError('columns must be a JSON object such as {"id": "BIGINT", "created_at": "TIMESTAMPTZ"}')
    return {str(k): parse_type(str(v)) for k, v in value.items()}


def resolve_schema(
    resource: dict[str, Any],
    overrides: dict[str, pa.DataType],
    sampled_rows: list[dict[str, Any]],
    sampled_schemas: list[pa.Schema],
) -> tuple[pa.Schema, dict[str, str]]:
    """Combine overrides, hints and the sample into the output schema; also return each column's origin."""
    types: dict[str, pa.DataType | None] = {}
    origin: dict[str, str] = {}
    not_null: set[str] = set()
    for col in resource.get("columns") or []:
        types[col["name"]] = dlt_hint_type(col)
        origin[col["name"]] = "hint"
        if col.get("nullable") is False:
            not_null.add(col["name"])
    for schema in sampled_schemas:
        for f in schema:
            if types.get(f.name) is None:
                types[f.name] = normalize_arrow_type(f.type)
                origin[f.name] = "sample"
    for name, t in infer_fields(sampled_rows).items():
        if types.get(name) is None:
            types[name] = t
            origin[name] = "sample"
    for name, t in overrides.items():
        types[name] = t
        origin[name] = "columns"
    fields = []
    for name, maybe in types.items():
        # A hinted column the sample never showed: keep it, untyped values become JSON.
        t = maybe if maybe is not None else parse_type("json")
        meta = {b"comment": f"{name} ({origin[name]})".encode()}
        fields.append(pa.field(name, t, nullable=name not in not_null, metadata=meta))
    return pa.schema(fields), origin


# ---------------------------------------------------------------- scan engine


@dataclass(kw_only=True)
class ScanCursor(ArrowSerializableDataclass):
    """Externalized scan position: a token and the items returned so far; no config, no secrets."""

    token: str = ""
    emitted: int = 0
    done: bool = False
    result: str = ""


def read_frames(
    cursor: ScanCursor,
    *,
    spec: EnvSpec,
    request: dict[str, Any],
    timeout_seconds: int,
    label: str,
    max_items: int = BATCH_ROWS,
) -> list[Any]:
    """Read the next frames of a scan (row lists and Arrow tables), resuming or restarting as needed.

    Sets ``cursor.done`` (and ``cursor.result``, the runner's final JSON) once the
    source is exhausted.
    """
    if not cursor.token:
        cursor.token = uuid.uuid4().hex
    run = env.take_run(cursor.token)
    if run is not None and run.produced != cursor.emitted:
        env.drop_run(cursor.token)
        run = None
    skip = 0
    if run is None:
        run = Run.start(spec, {**request, "op": "read"}, timeout_seconds=timeout_seconds, label=label)
        skip = cursor.emitted
        env.keep_run(cursor.token, run)
    out: list[Any] = []
    count = 0
    started = time.monotonic()
    try:
        while count < max_items:
            if run.pending is not None:
                chunk, run.pending = run.pending, None
            else:
                wait = 1.0 if not count else max(0.05, FLUSH_SECONDS - (time.monotonic() - started))
                frame = run.poll(wait)
                if frame is None:
                    cursor.done = True
                    cursor.result = json.dumps(run.result or {})
                    break
                if frame is False:
                    if count and time.monotonic() - started >= FLUSH_SECONDS:
                        break
                    continue
                assert not isinstance(frame, bool)
                chunk = frame.rows if frame.rows is not None else frame.table
                if chunk is None:
                    continue
            size = chunk.num_rows if isinstance(chunk, pa.Table) else len(chunk)
            if skip:
                dropped = min(skip, size)
                chunk, skip, size = chunk[dropped:], skip - dropped, size - dropped
            if not size:
                continue
            room = max_items - count
            if size > room:
                run.pending = chunk[room:]
                chunk, size = chunk[:room], room
            out.append(chunk)
            count += size
    except BaseException:
        env.drop_run(cursor.token)
        raise
    if cursor.done:
        env.drop_run(cursor.token)
        if skip:
            raise SourceError(
                "the source produced fewer items when restarted than before; the scan cannot continue consistently"
            )
    cursor.emitted += count
    run.produced = cursor.emitted
    return out


def frames_to_batch(frames: list[Any], schema: pa.Schema, coercer: Coercer) -> pa.RecordBatch | None:
    """Turn the frames of one scan step into a single batch of ``schema``."""
    batches = []
    for frame in frames:
        if isinstance(frame, pa.Table):
            batches.append(table_to_batch(frame, schema, coercer))
        else:
            batches.append(rows_to_batch(frame, schema, coercer))
    if not batches:
        return None
    if len(batches) == 1:
        return batches[0]
    return pa.Table.from_batches(batches, schema=schema).combine_chunks().to_batches()[0]
