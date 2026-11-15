"""An offline ``@dlt.source`` for the vgi-dlt tests.

Load it by module path, either from this directory::

    dlt.main.read_resource('vgi_dlt_fixture:shop', 'orders', path := '<repo>/test/dlt_sources')

or installed into an isolated environment::

    ... pip_url := '<repo>/test/dlt_sources'

The ``api_key`` comes from dlt's config resolution, which the worker feeds from
a ``dlt`` secret (``[sources.shop] api_key = "sk-..."``). Nothing here touches
the network.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import dlt
import pyarrow as pa

BASE = datetime(2026, 3, 1, tzinfo=UTC)
# dlt reads a resource's incremental cursor from its argument default.
PLACED_AT = dlt.sources.incremental("placed_at", initial_value="2026-03-01T00:00:00Z")


@dlt.source(name="shop")
def shop(count: int = 30, api_key: str = dlt.secrets.value) -> Any:
    """Orders (incremental, dict items), their lines (a transformer) and daily stats (Arrow tables)."""
    if not api_key.startswith("sk-"):
        raise ValueError("api_key must start with 'sk-'")

    @dlt.resource(
        primary_key="order_id",
        write_disposition="merge",
        columns={
            "order_id": {"data_type": "bigint", "nullable": False},
            "amount": {"data_type": "decimal", "precision": 10, "scale": 2},
            "placed_at": {"data_type": "timestamp"},
        },
    )
    def orders(
        placed_at: dlt.sources.incremental[str] = PLACED_AT,
    ) -> Iterator[dict[str, Any]]:
        start = placed_at.last_value or placed_at.initial_value
        for i in range(1, count + 1):
            ts = (BASE + timedelta(hours=6 * i)).strftime("%Y-%m-%dT%H:%M:%SZ")
            if ts < start:
                continue
            yield {
                "order_id": i,
                "customer": {"name": f"customer {i % 5}", "vip": i % 5 == 0},
                "amount": f"{i * 3}.25",
                "placed_at": ts,
                "tags": ["gift"] if i % 4 == 0 else ["standard"],
            }

    @dlt.transformer(data_from=orders, primary_key=("order_id", "line"))
    def order_lines(order: dict[str, Any]) -> Iterator[list[dict[str, Any]]]:
        yield [{"order_id": order["order_id"], "line": n, "sku": f"SKU-{order['order_id']}-{n}"} for n in (1, 2)]

    @dlt.resource(primary_key="day")
    def daily_stats() -> Iterator[pa.Table]:
        for week in range(3):
            days = [BASE.date() + timedelta(days=7 * week + d) for d in range(7)]
            yield pa.table(
                {
                    "day": pa.array(days, pa.date32()),
                    "visits": pa.array([100 * week + d for d in range(7)], pa.int32()),
                    "conversion": pa.array([0.01 * (d + 1) for d in range(7)], pa.float64()),
                }
            )

    return orders, order_lines, daily_stats
