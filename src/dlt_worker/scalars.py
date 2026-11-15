"""Scalars that explain the type mapping: ``column_type(name)`` and ``infer_type(json)``.

Both run locally (no source, no network) and use exactly the rules
``read_resource`` uses, so they help write a ``columns := '{...}'`` override or
predict what a sampled item becomes.
"""

from __future__ import annotations

import json
from typing import Annotated

import pyarrow as pa
from vgi.arguments import Param, Returns
from vgi.metadata import FunctionExample
from vgi.scalar_function import ScalarFunction

from .coltypes import ColumnTypeError, infer_fields, parse_type, type_name
from .meta import object_tags


def column_type_names(values: list[str | None]) -> list[str | None]:
    """Return DuckDB type names for dlt / DuckDB type texts; NULL or unknown gives NULL."""
    out: list[str | None] = []
    for text in values:
        try:
            out.append(None if text is None else type_name(parse_type(text)))
        except ColumnTypeError:
            out.append(None)
    return out


def inferred_type_names(values: list[str | None]) -> list[str | None]:
    """Return the DuckDB type inferred for each JSON value; NULL, invalid JSON or JSON null gives NULL."""
    out: list[str | None] = []
    for text in values:
        if text is None:
            out.append(None)
            continue
        try:
            value = json.loads(text)
        except json.JSONDecodeError:
            out.append(None)
            continue
        out.append(None if value is None else type_name(infer_fields([{"v": value}])["v"]))
    return out


class ColumnType(ScalarFunction):
    """``column_type(type_name)``: the DuckDB type a dlt data type or type text maps to."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "column_type"
        description = "The DuckDB type a dlt data type (or a DuckDB type text) becomes in read_resource"
        tags = {
            **object_tags(
                title="dlt Data Type To DuckDB Type",
                category="discovery",
                keywords="dlt, data type, type mapping, duckdb type, columns, hints",
                doc_llm=(
                    "Given a dlt data type name (`text`, `bigint`, `double`, `bool`, `timestamp`, `date`, `time`, "
                    "`decimal`, `json`, `binary`, `wei`) or a DuckDB type text (for example `varchar[]` or "
                    "`struct(a bigint)`), return the canonical DuckDB type name read_resource uses for a column "
                    "hinted or overridden with it. Unknown names and NULL give NULL. Use it to check a "
                    "`columns` override before running a source."
                ),
                doc_md=(
                    "## column_type(type_name)\n\n"
                    "`column_type('timestamp')` returns `TIMESTAMP WITH TIME ZONE`; "
                    "`column_type('text')` returns `VARCHAR`."
                ),
            ),
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "description": "dlt data types and a DuckDB type text",
                        "sql": (
                            "SELECT t, dlt.main.column_type(t) AS duckdb_type FROM (VALUES ('bigint'), ('text'), "
                            "('timestamp'), ('struct(login varchar)')) v(t) ORDER BY t"
                        ),
                        "expected_result": [
                            {"t": "bigint", "duckdb_type": "BIGINT"},
                            {"t": "struct(login varchar)", "duckdb_type": "STRUCT(login VARCHAR)"},
                            {"t": "text", "duckdb_type": "VARCHAR"},
                            {"t": "timestamp", "duckdb_type": "TIMESTAMP WITH TIME ZONE"},
                        ],
                    },
                ]
            ),
        }
        examples = [
            FunctionExample(
                sql="SELECT dlt.main.column_type('decimal(18,2)') AS duckdb_type",
                description="Normalize a decimal type text the way a columns override reads it",
            ),
        ]

    @classmethod
    def compute(
        cls,
        type_text: Annotated[
            pa.StringArray, Param(doc="A dlt data type name or a DuckDB type text; NULL in, NULL out.")
        ],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Map every type name in the column."""
        return pa.array(column_type_names(type_text.to_pylist()), type=pa.string())


class InferType(ScalarFunction):
    """``infer_type(json_value)``: the DuckDB type inference picks for one item value."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "infer_type"
        description = "The DuckDB type read_resource infers for a JSON value an item carries"
        tags = {
            **object_tags(
                title="Infer DuckDB Type From A JSON Value",
                category="discovery",
                keywords="infer, type inference, json, struct, list, timestamp, sample",
                doc_llm=(
                    "Given one item value as JSON text, return the DuckDB type read_resource infers for a column "
                    "holding it: whole numbers -> `BIGINT`, other numbers -> `DOUBLE`, true/false -> `BOOLEAN`, "
                    "ISO-8601 timestamps -> `TIMESTAMP WITH TIME ZONE`, ISO dates -> `DATE`, other strings -> "
                    "`VARCHAR`, objects -> `STRUCT(...)`, arrays -> `T[]`, mixed arrays or deep nesting -> `JSON`. "
                    "JSON null, invalid JSON and NULL give NULL."
                ),
                doc_md=(
                    "## infer_type(json_value)\n\n"
                    '`infer_type(\'{"login": "ada", "id": 1}\')` returns `STRUCT(login VARCHAR, id BIGINT)`.'
                ),
            ),
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "description": "A nested object, a timestamp string and a list of labels",
                        "sql": (
                            "SELECT dlt.main.infer_type(v) AS duckdb_type FROM (VALUES "
                            '(\'{"login": "ada", "id": 1}\'), (\'"2026-01-01T00:00:00Z"\'), '
                            '(\'["bug", "ui"]\')) t(v) ORDER BY duckdb_type'
                        ),
                        "expected_result": [
                            {"duckdb_type": "STRUCT(login VARCHAR, id BIGINT)"},
                            {"duckdb_type": "TIMESTAMP WITH TIME ZONE"},
                            {"duckdb_type": "VARCHAR[]"},
                        ],
                    },
                ]
            ),
        }
        examples = [
            FunctionExample(
                sql="""SELECT dlt.main.infer_type('{"user": {"login": "ada"}, "labels": ["bug"]}') AS duckdb_type""",
                description="See what a nested API object becomes before reading the resource",
            ),
        ]

    @classmethod
    def compute(
        cls,
        json_value: Annotated[pa.StringArray, Param(doc="One item value as JSON text; NULL in, NULL out.")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Infer a type for every value in the column."""
        return pa.array(inferred_type_names(json_value.to_pylist()), type=pa.string())
