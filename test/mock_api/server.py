#!/usr/bin/env python3
"""A tiny, dependency-free SaaS-like JSON API that the dlt rest_api tests drive.

Everything is deterministic, so the suite runs offline.

Endpoints
---------
``GET /issues?page=<n>&per_page=<m>&updated_since=<iso>``
    45 issues, page-number pagination: ``{"data": [...], "page": n, "total_pages": k}``.
    ``updated_since`` keeps issues with ``updated_at >= updated_since`` (the
    incremental start parameter). Issues are ordered by ``updated_at``.
``GET /issues/<id>/comments``
    Two comments per issue, a top-level JSON array (for a child resource).
``GET /users?cursor=<n>``
    12 users, 5 per page, top-level JSON array; the next page is announced in a
    ``Link: <...>; rel="next"`` header (dlt's ``header_link`` paginator).
``GET /secure/projects``
    Requires ``Authorization: Bearer <MOCK_API_TOKEN>`` (default ``test-token``);
    401 otherwise. ``{"projects": [...]}``, no pagination.
``GET /requests``
    The query strings of every ``/issues`` request so far (lets tests see what
    was pushed down), as ``[{"seq": n, "path": ..., "query": ...}]``.

Behaviour switches
------------------
* ``MOCK_API_FLAKY=1``: the first request for each ``/issues`` page returns
  ``429`` with ``Retry-After: 0`` (dlt's client must retry it).

Usage::

    python3 test/mock_api/server.py            # prints PORT:<n> once listening
    python3 test/mock_api/server.py --port 8765
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys
import threading
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

STATES = ("open", "closed")
BASE_TIME = datetime(2026, 1, 1, tzinfo=UTC)


def _iso(d: datetime) -> str:
    return d.strftime("%Y-%m-%dT%H:%M:%SZ")


ISSUES = [
    {
        "id": i,
        "number": 1000 + i,
        "title": f"Issue {i}",
        "state": STATES[i % 2],
        "score": round(i * 0.5, 1),
        "is_pull_request": i % 5 == 0,
        "labels": ["bug"] if i % 3 == 0 else (["feature", "ui"] if i % 3 == 1 else []),
        "user": {"login": f"user{i % 4}", "id": 100 + i % 4},
        "created_at": _iso(BASE_TIME + timedelta(hours=i)),
        "updated_at": _iso(BASE_TIME + timedelta(days=i)),
        "due_on": (BASE_TIME + timedelta(days=30 + i)).date().isoformat(),
        "closed_at": _iso(BASE_TIME + timedelta(days=i, hours=5)) if i % 2 else None,
    }
    for i in range(1, 46)
]

USERS = [{"id": i, "login": f"user{i}", "admin": i == 1} for i in range(1, 13)]

PROJECTS = [
    {"key": "CORE", "name": "Core platform", "budget": 1200.5},
    {"key": "WEB", "name": "Website", "budget": 300.0},
    {"key": "OPS", "name": "Operations", "budget": 50.25},
]

_flaky_seen: set[str] = set()
_requests: list[dict[str, object]] = []
_lock = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    """Serve the fixture endpoints."""

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - stdlib signature
        """Log to stderr only, so stdout carries nothing except the ``PORT:`` line."""
        sys.stderr.write("mock_api: " + (format % args) + "\n")

    def _send(self, status: int, body: object, headers: dict[str, str] | None = None) -> None:
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self) -> None:
        """Handle GET requests."""
        url = urlparse(self.path)
        query = parse_qs(url.query)
        parts = [p for p in url.path.split("/") if p]
        if parts == ["health"]:
            self._send(200, {"status": "ok"})
        elif parts == ["requests"]:
            with _lock:
                self._send(200, list(_requests))
        elif parts == ["issues"]:
            self._issues(url.query, query)
        elif len(parts) == 3 and parts[0] == "issues" and parts[2] == "comments" and parts[1].isdigit():
            issue = int(parts[1])
            self._send(200, [{"id": issue * 10 + k, "issue_id": issue, "body": f"comment {k}"} for k in (1, 2)])
        elif parts == ["users"]:
            self._users(query)
        elif parts == ["secure", "projects"]:
            token = os.environ.get("MOCK_API_TOKEN", "test-token")
            if self.headers.get("Authorization") != f"Bearer {token}":
                self._send(401, {"error": "missing or invalid bearer token"})
                return
            self._send(200, {"projects": PROJECTS})
        else:
            self._send(404, {"error": "not found"})

    def _issues(self, raw_query: str, query: dict[str, list[str]]) -> None:
        try:
            page = int(query.get("page", ["1"])[0])
            per_page = int(query.get("per_page", ["10"])[0])
        except ValueError:
            self._send(400, {"error": "page and per_page must be integers"})
            return
        if os.environ.get("MOCK_API_FLAKY") == "1":
            with _lock:
                if raw_query not in _flaky_seen:
                    _flaky_seen.add(raw_query)
                    self._send(429, {"error": "slow down"}, {"Retry-After": "0"})
                    return
        with _lock:
            _requests.append({"seq": len(_requests) + 1, "path": "/issues", "query": raw_query})
        since = query.get("updated_since", [None])[0]
        rows = [i for i in ISSUES if since is None or str(i["updated_at"]) >= since]
        total_pages = max(1, -(-len(rows) // per_page))
        chunk = rows[(page - 1) * per_page : page * per_page]
        self._send(200, {"data": chunk, "page": page, "total_pages": total_pages})

    def _users(self, query: dict[str, list[str]]) -> None:
        cursor = int(query.get("cursor", ["0"])[0])
        chunk = USERS[cursor : cursor + 5]
        headers = {}
        if cursor + 5 < len(USERS):
            host = self.headers.get("Host", "127.0.0.1")
            headers["Link"] = f'<http://{host}/users?cursor={cursor + 5}>; rel="next"'
        self._send(200, chunk, headers)


def main() -> None:
    """Start the server and print ``PORT:<n>`` once it is listening."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"PORT:{server.server_address[1]}", flush=True)
    with contextlib.suppress(KeyboardInterrupt):
        server.serve_forever()


if __name__ == "__main__":
    main()
