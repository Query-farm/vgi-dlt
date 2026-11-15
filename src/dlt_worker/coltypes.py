"""Column types: dlt hints, inference from sampled items, DuckDB type names, value coercion.

A resource's columns come from three places, in order of precedence:

1. the ``columns`` override (a JSON object ``{"column": "TYPE"}``), where TYPE is
   a DuckDB type (``BIGINT``, ``TIMESTAMPTZ``, ``VARCHAR[]``,
   ``STRUCT(login VARCHAR, id BIGINT)``, ``DECIMAL(18,2)``, ``JSON`` ...) or a
   dlt data type (``text``, ``bigint``, ``double``, ``bool``, ``timestamp``,
   ``date``, ``time``, ``decimal``, ``json``, ``binary``, ``wei``);
2. the resource's own column hints (``columns=`` on ``@dlt.resource``, or
   ``columns`` in a rest_api resource);
3. inference from the first ``sample_rows`` items the resource yields.

Inference from Python values:

======================================  ==================================
value                                   DuckDB type
======================================  ==================================
bool                                    BOOLEAN
int                                     BIGINT
float (or int and float mixed)          DOUBLE
ISO-8601 timestamp string               TIMESTAMPTZ (offset-less = UTC)
ISO-8601 date string                    DATE
other string                            VARCHAR
list of one inferable type              LIST, e.g. VARCHAR[]
object                                  STRUCT of its keys (up to 3 levels deep)
anything mixed, empty or deeper         JSON
======================================  ==================================

Arrow data a resource yields natively keeps its Arrow types.

Values that do not fit their column type become NULL rather than failing the
scan (a count is logged to stderr), except a NULL in a NOT NULL column (a
primary key), which is an error.
"""

from __future__ import annotations

import base64
import datetime as dt
import decimal
import json
import logging
import math
import re
from collections.abc import Callable, Iterable
from typing import Any

import pyarrow as pa

log = logging.getLogger(__name__)

JSON_TYPE: pa.DataType = pa.json_()
TIMESTAMP_TYPE = pa.timestamp("us", tz="UTC")
MAX_STRUCT_DEPTH = 3

_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d{1,9})?)?(Z|z|[+-]\d{2}(:?\d{2})?)?$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class ColumnTypeError(ValueError):
    """A column type could not be parsed."""


# ---------------------------------------------------------------- dlt hints


def dlt_hint_type(col: dict[str, Any]) -> pa.DataType | None:
    """Arrow type of a dlt column hint, or None when the hint has no data_type."""
    kind = col.get("data_type")
    if not kind:
        return None
    if kind == "decimal":
        precision = int(col.get("precision") or 38)
        scale = int(col["scale"]) if col.get("scale") is not None else 9
        return pa.decimal128(min(precision, 38), min(scale, precision))
    if kind == "timestamp":
        return pa.timestamp("us") if col.get("timezone") is False else TIMESTAMP_TYPE
    return parse_type(str(kind))


_SIMPLE: dict[str, pa.DataType] = {
    # dlt data types
    "text": pa.string(),
    "bigint": pa.int64(),
    "double": pa.float64(),
    "bool": pa.bool_(),
    "timestamp": TIMESTAMP_TYPE,
    "date": pa.date32(),
    "time": pa.time64("us"),
    "json": JSON_TYPE,
    "complex": JSON_TYPE,
    "binary": pa.binary(),
    "wei": pa.decimal128(38, 0),
    # DuckDB types
    "varchar": pa.string(),
    "string": pa.string(),
    "integer": pa.int32(),
    "int": pa.int32(),
    "smallint": pa.int16(),
    "tinyint": pa.int8(),
    "hugeint": pa.decimal128(38, 0),
    "ubigint": pa.uint64(),
    "float": pa.float32(),
    "real": pa.float32(),
    "boolean": pa.bool_(),
    "timestamptz": TIMESTAMP_TYPE,
    "timestamp with time zone": TIMESTAMP_TYPE,
    "blob": pa.binary(),
    "uuid": pa.string(),
}


def _split_top(text: str) -> list[str]:
    parts: list[str] = []
    cur: list[str] = []
    depth = 0
    for ch in text:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)
    if "".join(cur).strip():
        parts.append("".join(cur).strip())
    return parts


def parse_type(text: str) -> pa.DataType:
    """Parse a DuckDB type name (or a dlt data type) into an Arrow type."""
    raw = text.strip()
    low = raw.lower()
    if low.endswith("[]"):
        return pa.list_(parse_type(raw[:-2]))
    if low in _SIMPLE:
        return _SIMPLE[low]
    if low == "timestamp":
        return TIMESTAMP_TYPE
    m = re.fullmatch(r"(?:decimal|numeric)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", low)
    if m:
        return pa.decimal128(int(m.group(1)), int(m.group(2)))
    if low in ("decimal", "numeric"):
        return pa.decimal128(38, 9)
    if low.startswith("struct(") and low.endswith(")"):
        fields = []
        for part in _split_top(raw[raw.index("(") + 1 : -1]):
            name, _, ftype = part.strip().partition(" ")
            if not ftype:
                raise ColumnTypeError(f"STRUCT field {part!r} needs a name and a type")
            fields.append(pa.field(name.strip('"'), parse_type(ftype)))
        return pa.struct(fields)
    if low.startswith("list(") and low.endswith(")"):
        return pa.list_(parse_type(raw[5:-1]))
    raise ColumnTypeError(
        f"unknown column type {text!r}; use a DuckDB type such as BIGINT, DOUBLE, BOOLEAN, VARCHAR, "
        "TIMESTAMPTZ, DATE, JSON, VARCHAR[] or STRUCT(a BIGINT), or a dlt data type such as text or bigint"
    )


def type_name(t: pa.DataType) -> str:
    """Return the DuckDB name of an Arrow type (the inverse of ``parse_type``)."""
    if t == JSON_TYPE or isinstance(t, pa.JsonType):
        return "JSON"
    if pa.types.is_timestamp(t):
        return "TIMESTAMP WITH TIME ZONE" if t.tz else "TIMESTAMP"
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        return f"{type_name(t.value_type)}[]"
    if pa.types.is_struct(t):
        inner = ", ".join(f"{_ident(t.field(i).name)} {type_name(t.field(i).type)}" for i in range(t.num_fields))
        return f"STRUCT({inner})"
    if pa.types.is_decimal(t):
        return f"DECIMAL({t.precision},{t.scale})"
    simple = {
        pa.int8(): "TINYINT",
        pa.int16(): "SMALLINT",
        pa.int32(): "INTEGER",
        pa.int64(): "BIGINT",
        pa.uint8(): "UTINYINT",
        pa.uint16(): "USMALLINT",
        pa.uint32(): "UINTEGER",
        pa.uint64(): "UBIGINT",
        pa.float32(): "FLOAT",
        pa.float64(): "DOUBLE",
        pa.bool_(): "BOOLEAN",
        pa.string(): "VARCHAR",
        pa.large_string(): "VARCHAR",
        pa.binary(): "BLOB",
        pa.large_binary(): "BLOB",
        pa.date32(): "DATE",
        pa.time64("us"): "TIME",
    }
    return simple.get(t, str(t).upper())


def _ident(name: str) -> str:
    return name if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name) else '"' + name.replace('"', '""') + '"'


# ---------------------------------------------------------------- inference


def normalize_arrow_type(t: pa.DataType) -> pa.DataType:
    """Canonical type for a column a resource yielded as Arrow."""
    if pa.types.is_timestamp(t):
        return pa.timestamp("us", tz=t.tz) if t.tz else pa.timestamp("us")
    if pa.types.is_large_string(t) or pa.types.is_string_view(t):
        return pa.string()
    if pa.types.is_large_binary(t):
        return pa.binary()
    if pa.types.is_date64(t):
        return pa.date32()
    if pa.types.is_dictionary(t):
        return normalize_arrow_type(t.value_type)
    if pa.types.is_large_list(t) or pa.types.is_list_view(t):
        return pa.list_(normalize_arrow_type(t.value_type))
    if pa.types.is_list(t):
        return pa.list_(normalize_arrow_type(t.value_type))
    if pa.types.is_struct(t):
        return pa.struct([pa.field(f.name, normalize_arrow_type(f.type)) for f in t])
    if pa.types.is_null(t):
        return JSON_TYPE
    return t


def _value_type(v: Any, depth: int) -> pa.DataType | None:
    """Type of one non-null value (None means 'no information')."""
    if isinstance(v, bool):
        return pa.bool_()
    if isinstance(v, int):
        return pa.int64()
    if isinstance(v, float):
        return pa.float64()
    if isinstance(v, str):
        if _TS_RE.match(v):
            return TIMESTAMP_TYPE
        if _DATE_RE.match(v):
            return pa.date32()
        return pa.string()
    if isinstance(v, list):
        return _list_type(v, depth)
    if isinstance(v, dict):
        return _struct_type([v], depth)
    return JSON_TYPE


def merge_types(a: pa.DataType | None, b: pa.DataType | None) -> pa.DataType | None:
    """Return the type that holds values of both ``a`` and ``b`` (JSON when they disagree)."""
    if a is None:
        return b
    if b is None or a == b:
        return a
    numeric = {pa.int64(), pa.float64()}
    if a in numeric and b in numeric:
        return pa.float64()
    temporal = {TIMESTAMP_TYPE, pa.date32()}
    if a in temporal and b in temporal:
        return TIMESTAMP_TYPE
    if {a, b} <= {pa.string(), TIMESTAMP_TYPE, pa.date32()}:
        return pa.string()
    if pa.types.is_list(a) and pa.types.is_list(b):
        inner = merge_types(a.value_type, b.value_type)
        return JSON_TYPE if inner is None or inner == JSON_TYPE else pa.list_(inner)
    if pa.types.is_struct(a) and pa.types.is_struct(b):
        fields: dict[str, pa.DataType | None] = {f.name: f.type for f in a}
        for f in b:
            fields[f.name] = merge_types(fields.get(f.name), f.type)
        return pa.struct([pa.field(n, t or JSON_TYPE) for n, t in fields.items()])
    return JSON_TYPE


def _list_type(values: list[Any], depth: int) -> pa.DataType | None:
    inner: pa.DataType | None = None
    for v in values:
        if v is not None:
            inner = merge_types(inner, _value_type(v, depth + 1))
    if inner is None:
        return None  # an empty list says nothing about the element type
    if inner == JSON_TYPE or depth >= MAX_STRUCT_DEPTH:
        return JSON_TYPE
    return pa.list_(inner)


def _struct_type(objs: list[dict[str, Any]], depth: int) -> pa.DataType | None:
    if depth >= MAX_STRUCT_DEPTH:
        return JSON_TYPE
    fields: dict[str, pa.DataType | None] = {}
    for obj in objs:
        for k, v in obj.items():
            fields.setdefault(str(k), None)
            if v is not None:
                fields[str(k)] = merge_types(fields[str(k)], _value_type(v, depth + 1))
    if not fields:
        return None  # an empty object says nothing about its keys
    return pa.struct([pa.field(n, t if t is not None else JSON_TYPE) for n, t in fields.items()])


def infer_fields(items: Iterable[dict[str, Any]]) -> dict[str, pa.DataType]:
    """Infer one type per top-level key, in first-seen order (JSON when nothing is known)."""
    types: dict[str, pa.DataType | None] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        for k, v in item.items():
            key = str(k)
            types.setdefault(key, None)
            if v is not None:
                types[key] = merge_types(types[key], _value_type(v, 0))
    return {k: (t if t is not None else JSON_TYPE) for k, t in types.items()}


# ---------------------------------------------------------------- coercion


class Coercer:
    """Convert Python values to an Arrow type, counting values that do not fit."""

    def __init__(self) -> None:
        """Start with no rejected values."""
        self.rejected = 0

    def converter(self, t: pa.DataType) -> Callable[[Any], Any]:
        """Return a function mapping one value to a Python value Arrow accepts for ``t`` (or None)."""
        if t == JSON_TYPE or isinstance(t, pa.JsonType):
            return self._guard(_to_json)
        if pa.types.is_list(t):
            inner = self.converter(t.value_type)

            def to_list(v: Any) -> Any:
                if not isinstance(v, list | tuple):
                    raise TypeError("not a list")
                return [None if x is None else inner(x) for x in v]

            return self._guard(to_list)
        if pa.types.is_struct(t):
            convs = {t.field(i).name: self.converter(t.field(i).type) for i in range(t.num_fields)}

            def to_struct(v: Any) -> Any:
                if isinstance(v, str):
                    v = json.loads(v)
                if not isinstance(v, dict):
                    raise TypeError("not an object")
                return {k: (None if v.get(k) is None else c(v.get(k))) for k, c in convs.items()}

            return self._guard(to_struct)
        if pa.types.is_boolean(t):
            return self._guard(_to_bool)
        if pa.types.is_integer(t):
            return self._guard(_to_int)
        if pa.types.is_floating(t):
            return self._guard(_to_float)
        if pa.types.is_decimal(t):
            return self._guard(_to_decimal)
        if pa.types.is_timestamp(t):
            return self._guard(_to_timestamp if t.tz else _to_naive_timestamp)
        if pa.types.is_date(t):
            return self._guard(_to_date)
        if pa.types.is_time(t):
            return self._guard(_to_time)
        if pa.types.is_binary(t) or pa.types.is_large_binary(t):
            return self._guard(_to_bytes)
        return self._guard(_to_str)

    def _guard(self, fn: Callable[[Any], Any]) -> Callable[[Any], Any]:
        def wrapped(v: Any) -> Any:
            if v is None:
                return None
            try:
                return fn(v)
            except (TypeError, ValueError, ArithmeticError, OverflowError, json.JSONDecodeError):
                self.rejected += 1
                return None

        return wrapped


def _to_json(v: Any) -> str:
    return json.dumps(v, separators=(",", ":"), default=str)


def _to_str(v: Any) -> str:
    if isinstance(v, dict | list):
        return _to_json(v)
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def _to_int(v: Any) -> int:
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        if not v.is_integer():
            raise ValueError("not an integer")
        return int(v)
    text = str(v).strip()
    try:
        return int(text)
    except ValueError:
        f = float(text)
        if not f.is_integer():
            raise ValueError("not an integer") from None
        return int(f)


def _to_float(v: Any) -> float:
    if isinstance(v, bool):
        return float(v)
    f = float(v)
    if math.isnan(f) and not isinstance(v, float):
        raise ValueError("nan")
    return f


def _to_decimal(v: Any) -> decimal.Decimal:
    if isinstance(v, bool):
        raise TypeError("bool")
    return decimal.Decimal(str(v))


def _to_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, int | float):
        return bool(v)
    text = str(v).strip().lower()
    if text in ("true", "t", "1", "yes", "y"):
        return True
    if text in ("false", "f", "0", "no", "n"):
        return False
    raise ValueError("not a boolean")


def _parse_datetime(v: Any) -> dt.datetime:
    if isinstance(v, dt.datetime):
        d = v
    elif isinstance(v, dt.date):
        d = dt.datetime(v.year, v.month, v.day)
    elif isinstance(v, int | float) and not isinstance(v, bool):
        d = dt.datetime.fromtimestamp(float(v), tz=dt.UTC)
    else:
        text = str(v).strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        d = dt.datetime.fromisoformat(text)
    return d


def _to_timestamp(v: Any) -> dt.datetime:
    d = _parse_datetime(v)
    return d.replace(tzinfo=dt.UTC) if d.tzinfo is None else d.astimezone(dt.UTC)


def _to_naive_timestamp(v: Any) -> dt.datetime:
    d = _parse_datetime(v)
    return d if d.tzinfo is None else d.astimezone(dt.UTC).replace(tzinfo=None)


def _to_date(v: Any) -> dt.date:
    if isinstance(v, dt.datetime):
        return v.date()
    if isinstance(v, dt.date):
        return v
    text = str(v).strip()
    if _DATE_RE.match(text):
        return dt.date.fromisoformat(text)
    return _to_timestamp(text).date()


def _to_time(v: Any) -> dt.time:
    if isinstance(v, dt.time):
        return v
    return dt.time.fromisoformat(str(v).strip())


def _to_bytes(v: Any) -> bytes:
    if isinstance(v, bytes | bytearray):
        return bytes(v)
    return base64.b64decode(str(v), validate=True)


def rows_to_batch(rows: list[Any], schema: pa.Schema, coercer: Coercer) -> pa.RecordBatch:
    """Build one batch of ``schema`` from dict items (missing keys become NULL)."""
    arrays = []
    for f in schema:
        conv = coercer.converter(f.type)
        values = [conv(r.get(f.name)) if isinstance(r, dict) else None for r in rows]
        if not f.nullable and any(v is None for v in values):
            raise ValueError(f"column {f.name!r} is NOT NULL (a primary key) but an item has no value for it")
        storage = f.type.storage_type if isinstance(f.type, pa.ExtensionType) else f.type
        arr = pa.array(values, type=storage)
        if isinstance(f.type, pa.ExtensionType):
            arr = pa.ExtensionArray.from_storage(f.type, arr)
        arrays.append(arr)
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


def table_to_batch(table: pa.Table, schema: pa.Schema, coercer: Coercer) -> pa.RecordBatch:
    """Fit an Arrow table a resource yielded to ``schema`` (cast, or coerce value by value)."""
    arrays = []
    for f in schema:
        if f.name not in table.column_names:
            if not f.nullable and table.num_rows:
                raise ValueError(f"column {f.name!r} is NOT NULL (a primary key) but the resource did not yield it")
            arrays.append(pa.nulls(table.num_rows, type=f.type))
            continue
        col = table.column(f.name).combine_chunks()
        try:
            if isinstance(f.type, pa.ExtensionType):
                raise pa.ArrowInvalid("extension")
            arr = col.cast(f.type)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError):
            conv = coercer.converter(f.type)
            values = [conv(v) for v in col.to_pylist()]
            storage = f.type.storage_type if isinstance(f.type, pa.ExtensionType) else f.type
            arr = pa.array(values, type=storage)
            if isinstance(f.type, pa.ExtensionType):
                arr = pa.ExtensionArray.from_storage(f.type, arr)
        if not f.nullable and arr.null_count:
            raise ValueError(f"column {f.name!r} is NOT NULL (a primary key) but an item has no value for it")
        arrays.append(arr)
    return pa.RecordBatch.from_arrays(arrays, schema=schema)
