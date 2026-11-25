"""Unit tests. The SQL-level tests live in test/sql and run through DuckDB."""

from __future__ import annotations

import datetime as dt
import decimal
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from vgi.arguments import Arguments
from vgi.client import Client

from dlt_worker import common, env
from dlt_worker.coltypes import (
    JSON_TYPE,
    TIMESTAMP_TYPE,
    Coercer,
    ColumnTypeError,
    dlt_hint_type,
    infer_fields,
    parse_type,
    rows_to_batch,
    table_to_batch,
    type_name,
)
from dlt_worker.read_resource import _later, _render, _scalar_arg

SCHEMA = ["main"]
FIXTURE_SOURCES = str(Path(__file__).resolve().parent.parent / "test" / "dlt_sources")
SHOP_SECRET = {"sources": {"shop": {"api_key": "sk-unit"}}}


# ---------------------------------------------------------------- types


@pytest.mark.parametrize(
    "text",
    [
        "BIGINT",
        "DOUBLE",
        "BOOLEAN",
        "VARCHAR",
        "DATE",
        "JSON",
        "TIMESTAMP WITH TIME ZONE",
        "DECIMAL(10,2)",
        "VARCHAR[]",
        "STRUCT(login VARCHAR, id BIGINT)",
        "STRUCT(a STRUCT(b DOUBLE[]), c JSON)[]",
    ],
)
def test_duckdb_type_names_round_trip(text: str) -> None:
    assert type_name(parse_type(text)) == text


def test_dlt_type_names() -> None:
    assert parse_type("text") == pa.string()
    assert parse_type("bigint") == pa.int64()
    assert parse_type("timestamp") == TIMESTAMP_TYPE
    assert parse_type("json") == JSON_TYPE
    assert dlt_hint_type({"data_type": "decimal", "precision": 10, "scale": 2}) == pa.decimal128(10, 2)
    assert dlt_hint_type({"data_type": "timestamp", "timezone": False}) == pa.timestamp("us")
    assert dlt_hint_type({"name": "id"}) is None
    with pytest.raises(ColumnTypeError):
        parse_type("NOT_A_TYPE")


def test_inference() -> None:
    items: list[dict[str, Any]] = [
        {"i": 1, "f": 1, "b": True, "ts": "2026-01-01T00:00:00Z", "d": "2026-01-01", "s": "x", "l": [], "o": {}},
        {"i": 2, "f": 1.5, "b": False, "ts": "2026-01-02 10:00", "d": "2026-01-02", "s": "y", "l": ["a"]},
        {"i": None, "mixed": 1, "o": {"k": 1, "n": {"z": "q"}}, "nested": [{"a": 1}]},
        {"mixed": "a", "o": {"k": 2, "extra": True}, "never": None},
    ]
    types = infer_fields(items)
    assert types["i"] == pa.int64()
    assert types["f"] == pa.float64()
    assert types["b"] == pa.bool_()
    assert types["ts"] == TIMESTAMP_TYPE
    assert types["d"] == pa.date32()
    assert types["s"] == pa.string()
    assert types["l"] == pa.list_(pa.string())
    assert type_name(types["o"]) == "STRUCT(k BIGINT, n STRUCT(z VARCHAR), extra BOOLEAN)"
    assert type_name(types["nested"]) == "STRUCT(a BIGINT)[]"
    assert types["mixed"] == JSON_TYPE
    assert types["never"] == JSON_TYPE
    assert list(types)[:3] == ["i", "f", "b"]


def test_coercion_rejects_without_failing() -> None:
    fields: list[pa.Field[Any]] = [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("n", pa.int64()),
        pa.field("ts", TIMESTAMP_TYPE),
        pa.field("amount", pa.decimal128(10, 2)),
        pa.field("user", pa.struct([pa.field("login", pa.string())])),
        pa.field("tags", pa.list_(pa.string())),
        pa.field("raw", JSON_TYPE),
    ]
    schema = pa.schema(fields)
    coercer = Coercer()
    batch = rows_to_batch(
        [
            {"id": 1, "n": "12", "ts": "2026-01-01T05:00:00+05:00", "amount": "3.25", "user": {"login": "a", "x": 1}},
            {"id": 2, "n": "abc", "ts": 0, "amount": 1.5, "tags": ["t", 3], "raw": {"a": [1]}},
        ],
        schema,
        coercer,
    )
    rows = batch.to_pylist()
    assert rows[0]["n"] == 12
    assert rows[0]["ts"] == dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
    assert rows[0]["amount"] == decimal.Decimal("3.25")
    assert rows[0]["user"] == {"login": "a"}
    assert rows[1]["n"] is None
    assert rows[1]["ts"] == dt.datetime(1970, 1, 1, tzinfo=dt.UTC)
    assert rows[1]["tags"] == ["t", "3"]
    assert json.loads(rows[1]["raw"]) == {"a": [1]}
    assert coercer.rejected == 1


def test_not_null_column_is_enforced() -> None:
    schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])
    with pytest.raises(ValueError, match="NOT NULL"):
        rows_to_batch([{"id": None}], schema, Coercer())


def test_arrow_tables_are_cast_and_fitted() -> None:
    table = pa.table({"day": pa.array([dt.date(2026, 1, 1)]), "visits": pa.array([3], pa.int32()), "x": [1]})
    fields: list[pa.Field[Any]] = [
        pa.field("day", pa.date32()),
        pa.field("visits", pa.int64()),
        pa.field("gone", pa.string()),
    ]
    schema = pa.schema(fields)
    batch = table_to_batch(table, schema, Coercer())
    assert batch.to_pylist() == [{"day": dt.date(2026, 1, 1), "visits": 3, "gone": None}]


# ---------------------------------------------------------------- config and secrets


def test_secret_documents() -> None:
    assert common._secret_doc({"config": '{"a": {"b": 1}}'}) == {"a": {"b": 1}}
    assert common._secret_doc({"config": '[sources.shop]\napi_key = "sk-x"'}) == {
        "sources": {"shop": {"api_key": "sk-x"}}
    }
    assert common._secret_doc(None) == {}
    with pytest.raises(ValueError) as err:
        common._secret_doc({"config": "token=super-secret-value ="})
    assert "super-secret-value" not in str(err.value)


def test_deep_merge_and_scope() -> None:
    config = {"client": {"base_url": "https://api.example.com/v1/", "paginator": "json_link"}, "resources": ["a"]}
    merged = common.deep_merge(config, {"client": {"auth": {"token": "t"}}})
    assert merged["client"] == {
        "base_url": "https://api.example.com/v1/",
        "paginator": "json_link",
        "auth": {"token": "t"},
    }
    assert "auth" not in config["client"]
    assert common.scope_for("rest_api", config) == "dlt://rest_api/api.example.com/v1"
    assert common.scope_for("pkg.mod:src", {}) == "dlt://pkg/mod/src"
    assert common.scope_for("vgi_dlt_fixture:shop", {}) == "dlt://vgi_dlt_fixture/shop"


def test_rest_api_secret_is_merged_into_the_request() -> None:
    args = common.SourceArgs(
        source="rest_api", config="", path="", pip_url="", python="", secret_name="", timeout_seconds=60
    )
    req = common.base_request(args, {"client": {"base_url": "http://x/"}}, {"client": {"auth": {"token": "t"}}})
    assert req["config"]["client"]["auth"] == {"token": "t"}
    with pytest.raises(ValueError, match="needs a config"):
        common.base_request(args, {}, {})


def test_load_config(tmp_path: Path) -> None:
    f = tmp_path / "c.json"
    f.write_text('{"count": 3}', encoding="utf-8")
    assert common.load_config(str(f)) == {"count": 3}
    assert common.load_config("") == {}
    with pytest.raises(ValueError, match="JSON object"):
        common.load_config("[1]")
    with pytest.raises(ValueError, match="neither"):
        common.load_config("/no/such/file.json")


# ---------------------------------------------------------------- pushdown helpers


def test_pushdown_rendering() -> None:
    assert _render(dt.datetime(2026, 2, 1, 5, tzinfo=dt.timezone(dt.timedelta(hours=5)))) == "2026-02-01T00:00:00Z"
    assert _render(dt.datetime(2026, 2, 1, 0, 0, 0, 1500)) == "2026-02-01T00:00:00.001500Z"
    assert _render(dt.date(2026, 2, 1)) == "2026-02-01"
    assert _render(7) == 7
    assert _later("2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z") == "2026-02-01T00:00:00Z"
    assert _later("2026-03-01T00:00:00Z", "2026-02-01T00:00:00Z") == "2026-03-01T00:00:00Z"
    assert _later(None, 5) == 5
    assert _later(10, 5) == 10
    assert _scalar_arg("100") == 100
    assert _scalar_arg("2026-01-01") == "2026-01-01"


# ---------------------------------------------------------------- runner


def _shop_request(**extra: Any) -> dict[str, Any]:
    return {"source": "vgi_dlt_fixture:shop", "config": {}, "secrets": SHOP_SECRET, **extra}


def test_discover_module_source() -> None:
    spec = env.EnvSpec(path=FIXTURE_SOURCES)
    found = env.run_to_result(spec, _shop_request(op="discover"), timeout_seconds=60, label="test")
    by_name = {r["name"]: r for r in found["resources"]}
    assert set(by_name) == {"orders", "order_lines", "daily_stats"}
    assert by_name["orders"]["incremental_cursor"] == "placed_at"
    assert by_name["orders"]["primary_key"] == ["order_id"]
    assert by_name["order_lines"]["parent"] == "orders"


def test_missing_secret_is_a_clear_error() -> None:
    spec = env.EnvSpec(path=FIXTURE_SOURCES)
    with pytest.raises(env.SourceError, match="api_key"):
        env.run_to_result(spec, {**_shop_request(op="discover"), "secrets": {}}, timeout_seconds=60, label="test")


def _drain(cursor: common.ScanCursor, request: dict[str, Any], *, max_items: int, steps: int = 10_000) -> list[Any]:
    spec = env.EnvSpec(path=FIXTURE_SOURCES)
    out: list[Any] = []
    while not cursor.done and steps:
        for frame in common.read_frames(
            cursor, spec=spec, request=request, timeout_seconds=60, label="test", max_items=max_items
        ):
            out.extend(frame.to_pylist() if isinstance(frame, pa.Table) else frame)
        steps -= 1
    return out


@pytest.mark.parametrize("resource", ["orders", "daily_stats"])
def test_scan_resumes_in_another_process(resource: str) -> None:
    """A scan whose live runner is gone (another worker process) restarts and skips what it returned."""
    request = _shop_request(resource=resource, columns=None, config={"count": 40})
    whole = _drain(common.ScanCursor(), request, max_items=1000)

    cursor = common.ScanCursor()
    first = _drain(cursor, request, max_items=5, steps=2)
    assert not cursor.done and cursor.emitted == len(first)
    env.drop_run(cursor.token)  # the next batch lands on a process without the live run
    rest = _drain(cursor, request, max_items=1000)
    assert first + rest == whole
    assert json.loads(cursor.result)["items"] == len(whole)


def test_projection_keeps_only_requested_keys() -> None:
    rows = _drain(common.ScanCursor(), _shop_request(resource="orders", columns=["order_id"]), max_items=1000)
    assert len(rows) == 30 and all(set(r) == {"order_id"} for r in rows)


def test_incremental_override_and_state() -> None:
    cursor = common.ScanCursor()
    rows = _drain(
        cursor, _shop_request(resource="orders", columns=None, initial_value="2026-03-08T00:00:00Z"), max_items=1000
    )
    assert [r["order_id"] for r in rows] == [28, 29, 30]
    state = json.loads(cursor.result)["state"]
    assert state["incremental"]["placed_at"]["last_value"] == "2026-03-08T12:00:00Z"
    again = _drain(
        common.ScanCursor(),
        _shop_request(resource="orders", columns=None, state=state, config={"count": 32}),
        max_items=1000,
    )
    assert [r["order_id"] for r in again] == [31, 32]


# ---------------------------------------------------------------- through the worker protocol


def _table(client: Client, name: str, *positional: Any, **named: Any) -> pa.Table:
    arguments = Arguments(
        positional=tuple(pa.scalar(v) for v in positional),
        named={k: pa.scalar(v) for k, v in named.items()},
    )
    batches = list(client.table_function(function_name=name, schema_path=SCHEMA, arguments=arguments))
    return pa.Table.from_batches(batches)


# The Python Client does not answer bind-time secret-scope requests, and every
# source-running function asks for its `dlt` secret by scope, so data-returning
# calls are covered through DuckDB in test/sql instead.


def test_bad_source_reference_is_rejected(client: Client) -> None:
    with pytest.raises(Exception, match="pattern"):
        _table(client, "resources", "not a module")


def test_type_scalars() -> None:
    from dlt_worker.scalars import column_type_names, inferred_type_names

    assert column_type_names(["bigint", "wei", "VARCHAR[]", None, "bogus"]) == [
        "BIGINT",
        "DECIMAL(38,0)",
        "VARCHAR[]",
        None,
        None,
    ]
    assert inferred_type_names(['{"a": [1.5]}', "3", "true", "null", "{", None]) == [
        "STRUCT(a DOUBLE[])",
        "BIGINT",
        "BOOLEAN",
        None,
        None,
        None,
    ]
