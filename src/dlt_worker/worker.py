"""Assemble the functions into a catalog and run the worker.

To add a function: write it, then add the class to ``FUNCTIONS`` below.

Run it:

    vgi-dlt-worker                      # stdio: what DuckDB launches by default
    vgi-dlt-worker --http --port 8000   # HTTP: for Docker / hosted deploys
    vgi-dlt-worker --help

Attach it from DuckDB:

    ATTACH 'dlt' (TYPE vgi, LOCATION '/abs/path/to/.venv/bin/vgi-dlt-worker');
    ATTACH 'dlt' AS api (TYPE vgi, LOCATION '...', source 'rest_api', config '/path/config.json');
"""

from __future__ import annotations

import json
from typing import Annotated, Any

from vgi import Worker
from vgi.catalog import Catalog, Schema
from vgi.catalog.attach_option import AttachOption

from .catalog import SourceCatalog
from .common import DLT_SECRET_TYPE
from .discover import ColumnsJson, InferColumns, Resources
from .env import DEFAULT_TIMEOUT_SECONDS
from .meta import (
    CATALOG_COMMENT,
    CATALOG_NAME,
    CATALOG_TAGS,
    SCHEMA_CATEGORIES,
    SCHEMA_COMMENT,
    SCHEMA_DOC_LLM,
    SCHEMA_DOC_MD,
    SOURCE_URL,
    keywords_array,
)
from .read_resource import FinalState, ReadItems, ReadResource
from .scalars import ColumnType, InferType

FUNCTIONS: list[type[Any]] = [
    Resources,
    InferColumns,
    ColumnsJson,
    ReadResource,
    ReadItems,
    FinalState,
    ColumnType,
    InferType,
]


def _stamp_example_queries(functions: list[type[Any]]) -> list[dict[str, str]]:
    """Publish each function's examples as ``vgi.example_queries`` (with descriptions).

    Overloads share one SQL name, so their examples are merged per name.
    Returns every example, for the schema-level list.
    """
    by_name: dict[str, list[dict[str, str]]] = {}
    for fn in functions:
        bucket = by_name.setdefault(fn.Meta.name, [])
        for ex in getattr(fn.Meta, "examples", []):
            entry = {"description": ex.description, "sql": ex.sql}
            if entry not in bucket:
                bucket.append(entry)
    for fn in functions:
        fn.Meta.tags = {**fn.Meta.tags, "vgi.example_queries": json.dumps(by_name[fn.Meta.name])}
    return [entry for entries in by_name.values() for entry in entries]


_ALL_EXAMPLES = _stamp_example_queries(FUNCTIONS)


class DltCatalog(SourceCatalog):
    """The ``dlt`` catalog: the functions, plus one table per resource of an attached source."""

    catalog = Catalog(
        name=CATALOG_NAME,
        comment=CATALOG_COMMENT,
        source_url=SOURCE_URL,
        tags=CATALOG_TAGS,
        schemas=[
            Schema(
                path=["main"],
                comment=SCHEMA_COMMENT,
                functions=FUNCTIONS,
                tags={
                    "vgi.title": "dlt Source Functions",
                    "domain": "data integration",
                    "vgi.keywords": keywords_array("dlt, rest api, resources, incremental, state, sources"),
                    "vgi.doc_llm": SCHEMA_DOC_LLM,
                    "vgi.doc_md": SCHEMA_DOC_MD,
                    "vgi.categories": json.dumps(SCHEMA_CATEGORIES),
                    "vgi.example_queries": json.dumps(_ALL_EXAMPLES),
                },
            )
        ],
    )


class DltWorker(Worker):
    """The worker process: the ``dlt`` catalog, its secret type and ATTACH options."""

    catalog_interface = DltCatalog
    catalog_name = CATALOG_NAME
    secret_types = [DLT_SECRET_TYPE]

    class AttachOptions:
        """Options for ``ATTACH 'dlt' AS name (TYPE vgi, LOCATION ..., source '...', config '...')``."""

        source: Annotated[
            str,
            AttachOption(desc="'rest_api' or 'package.module:function'. Every selected resource becomes a table."),
        ] = ""
        config: Annotated[
            str,
            AttachOption(desc="The rest_api JSON config or the source function's keyword arguments: JSON or a path."),
        ] = ""
        columns: Annotated[
            str,
            AttachOption(desc='Column types per resource, {"resource": {"column": "TYPE"}}: JSON or a path.'),
        ] = ""
        source_path: Annotated[
            str, AttachOption(desc="Directory to import a module source from (the function argument `path`).")
        ] = ""
        pip_url: Annotated[
            str, AttachOption(desc="pip requirement(s) installed into an isolated cached environment.")
        ] = ""
        python: Annotated[str, AttachOption(desc="Python version for the pip_url environment, e.g. '3.12'.")] = ""
        secret_name: Annotated[str, AttachOption(desc="Name of the dlt secret to use; empty picks it by SCOPE.")] = ""
        timeout_seconds: Annotated[
            int, AttachOption(desc="Stop a source that produces nothing for this many seconds; 0 waits forever.")
        ] = DEFAULT_TIMEOUT_SECONDS


def main() -> None:
    """Console-script entry point: parse ``--http`` / ``--unix`` / logging flags and serve."""
    DltWorker.main()


if __name__ == "__main__":
    main()
