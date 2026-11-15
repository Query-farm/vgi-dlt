"""Isolated source environments and the runner child process.

**Where a source runs.** Every operation runs ``runner.py`` in a child process:

* the built-in ``rest_api`` source, and a source found on a ``path`` (a ``dlt
  init`` project directory or any folder of modules) with no ``pip_url``, run in
  the worker's own interpreter, which already has ``dlt``. Nothing is
  installed; ``path`` is only put on the child's ``sys.path``.
* a source with a ``pip_url`` runs in its own virtual environment under the
  cache directory (``$VGI_DLT_CACHE_DIR``, else the user cache directory),
  created once per (``pip_url``, ``python``) with ``uv`` (or ``venv`` + ``pip``)
  holding the same ``dlt`` version as the worker, ``pyarrow`` and the
  requirement. Nothing is ever installed into the worker's own environment.

**Secrets.** The request, including the secret's config document, is written to
the child's stdin and never touches disk or the environment.

**Streaming.** A reader thread decodes frames from the child's stdout into a
*bounded* queue, so a fast source blocks on its pipe instead of filling memory.
stderr is kept in a small ring buffer and quoted when the child fails. A child
that sends nothing for ``timeout_seconds`` is killed.
"""

from __future__ import annotations

import atexit
import collections
import contextlib
import fcntl
import hashlib
import json
import logging
import os
import queue
import shutil
import struct
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from importlib.metadata import version
from pathlib import Path
from typing import IO, Any

import pyarrow as pa

log = logging.getLogger(__name__)

CACHE_ENV_VAR = "VGI_DLT_CACHE_DIR"
DEFAULT_TIMEOUT_SECONDS = 300
QUEUE_FRAMES = 64
STDERR_LINES = 40
IDLE_RUN_SECONDS = 600
RUNNER = Path(__file__).with_name("runner.py")
_EOF = object()


class SourceError(RuntimeError):
    """A source could not be installed, failed, or answered with something unusable."""


@dataclass(frozen=True, slots=True)
class EnvSpec:
    """Which interpreter runs a source, and what it can import."""

    pip_url: str = ""
    python: str = ""
    path: str = ""

    @property
    def key(self) -> str:
        """Stable identifier of the installed environment."""
        raw = json.dumps([self.pip_url, self.python, _dlt_version()], separators=(",", ":"))
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def paths(self) -> list[str]:
        """Directories to prepend to the child's ``sys.path``."""
        if not self.path:
            return []
        p = Path(self.path).expanduser()
        if not p.is_dir():
            raise SourceError(f"path {self.path!r} is not a directory")
        return [str(p.resolve())]


def _dlt_version() -> str:
    return version("dlt")


def cache_dir() -> Path:
    """Where source environments live: ``$VGI_DLT_CACHE_DIR`` or the user cache directory."""
    explicit = os.environ.get(CACHE_ENV_VAR)
    if explicit:
        return Path(explicit)
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Caches" / "vgi-dlt"
    base = os.environ.get("XDG_CACHE_HOME") or str(Path.home() / ".cache")
    return Path(base) / "vgi-dlt"


def _run_install(cmd: list[str], what: str) -> None:
    log.info("dlt: %s", " ".join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=900, check=False)
    if proc.returncode != 0:
        tail = (proc.stderr or proc.stdout or "").strip().splitlines()[-15:]
        raise SourceError(f"could not {what} (exit {proc.returncode}):\n" + "\n".join(tail))


def interpreter(spec: EnvSpec) -> str:
    """Return the Python that runs the source, creating its isolated environment on first use."""
    if not spec.pip_url:
        return sys.executable
    env_dir = cache_dir() / "envs" / spec.key
    py = env_dir / "venv" / "bin" / "python"
    ready = env_dir / ".ready"
    if ready.is_file() and py.is_file():
        return str(py)
    env_dir.mkdir(parents=True, exist_ok=True)
    with open(env_dir / ".lock", "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if ready.is_file() and py.is_file():
            return str(py)
        venv = env_dir / "venv"
        shutil.rmtree(venv, ignore_errors=True)
        reqs = [f"dlt=={_dlt_version()}", f"pyarrow=={version('pyarrow')}", *spec.pip_url.split()]
        uv = shutil.which("uv")
        if uv:
            cmd = [uv, "venv", "--quiet", str(venv)]
            if spec.python:
                cmd += ["--python", spec.python]
            _run_install(cmd, "create the source environment")
            _run_install([uv, "pip", "install", "--quiet", "--python", str(py), *reqs], f"install {spec.pip_url}")
        else:
            base = shutil.which(f"python{spec.python}") if spec.python else sys.executable
            if not base:
                raise SourceError(f"python {spec.python} is not installed")
            _run_install([base, "-m", "venv", str(venv)], "create the source environment")
            _run_install([str(py), "-m", "pip", "install", "--quiet", *reqs], f"install {spec.pip_url}")
        ready.write_text(spec.pip_url)
    return str(py)


def _child_env() -> dict[str, str]:
    env = dict(os.environ)
    env["RUNTIME__DLTHUB_TELEMETRY"] = "false"
    env.setdefault("RUNTIME__LOG_LEVEL", "WARNING")
    return env


def _stderr_tail(lines: collections.deque[str]) -> str:
    return "\n".join(lines) if lines else "(no stderr output)"


def _error_text(payload: bytes) -> str:
    try:
        err = json.loads(payload)
        text = str(err.get("error") or err)
    except ValueError:
        text = payload.decode(errors="replace")
    lines = text.strip().splitlines()
    return "\n".join(lines[:12]) + ("\n..." if len(lines) > 12 else "")


@dataclass
class Frame:
    """One decoded message from the runner."""

    tag: str
    rows: list[Any] | None = None
    table: pa.Table | None = None
    result: dict[str, Any] | None = None


@dataclass
class Run:
    """One running ``runner.py`` child process."""

    label: str
    proc: subprocess.Popen[bytes]
    frames: queue.Queue[object] = field(default_factory=lambda: queue.Queue(maxsize=QUEUE_FRAMES))
    stderr: collections.deque[str] = field(default_factory=lambda: collections.deque(maxlen=STDERR_LINES))
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    last_used: float = field(default_factory=time.monotonic)
    last_output: float = field(default_factory=time.monotonic)
    finished: bool = False
    result: dict[str, Any] | None = None
    # Scan position: items this run has produced so far.
    produced: int = 0
    # Items read from the runner but not yet returned (the rest of a split frame).
    pending: Any = None

    @classmethod
    def start(cls, spec: EnvSpec, request: dict[str, Any], *, timeout_seconds: int, label: str) -> Run:
        """Start the runner and hand it the request on stdin."""
        python = interpreter(spec)
        request = {**request, "paths": spec.paths()}
        cwd = spec.paths()[0] if spec.path else None
        try:
            proc = subprocess.Popen(
                [python, "-I", str(RUNNER)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=_child_env(),
                cwd=cwd,
            )
        except OSError as exc:
            raise SourceError(f"could not start the dlt runner: {exc}") from exc
        run = cls(label=label, proc=proc, timeout_seconds=timeout_seconds)
        assert proc.stdin is not None and proc.stdout is not None and proc.stderr is not None
        threading.Thread(target=run._pump_stdout, args=(proc.stdout,), daemon=True).start()
        threading.Thread(target=run._pump_stderr, args=(proc.stderr,), daemon=True).start()
        try:
            proc.stdin.write(json.dumps(request, default=str).encode())
            proc.stdin.close()
        except BrokenPipeError:
            pass
        return run

    def _pump_stdout(self, stream: IO[bytes]) -> None:
        try:
            while True:
                head = stream.read(5)
                if len(head) < 5:
                    break
                tag = head[:1].decode()
                (size,) = struct.unpack(">I", head[1:])
                payload = stream.read(size)
                if tag == "R":
                    self.frames.put(Frame("R", rows=json.loads(payload)))
                elif tag == "A":
                    self.frames.put(Frame("A", table=pa.ipc.open_stream(payload).read_all()))
                elif tag == "J":
                    self.frames.put(Frame("J", result=json.loads(payload)))
                else:
                    self.frames.put(Frame("E", result={"error": _error_text(payload)}))
        except Exception as exc:
            self.frames.put(Frame("E", result={"error": f"could not read the runner's output: {exc}"}))
        finally:
            self.frames.put(_EOF)

    def _pump_stderr(self, stream: IO[bytes]) -> None:
        for line in stream:
            self.stderr.append(line.decode(errors="replace").rstrip("\n"))

    def poll(self, wait: float) -> Frame | bool | None:
        """Return the next data frame; None at the end; False if nothing arrived within ``wait`` seconds.

        Args:
            wait: Seconds to wait for a frame before returning False.

        Returns:
            A rows or Arrow frame, None once the runner finished cleanly, or False when nothing arrived yet.

        Raises:
            SourceError: The source failed, or sent nothing for ``timeout_seconds``.

        """
        if self.finished:
            return None
        try:
            item = self.frames.get(timeout=wait)
        except queue.Empty:
            if self.timeout_seconds and time.monotonic() - self.last_output > self.timeout_seconds:
                self.close()
                raise SourceError(
                    f"{self.label} produced nothing for {self.timeout_seconds} seconds and was stopped; "
                    "raise timeout_seconds for slow sources"
                ) from None
            return False
        self.last_output = self.last_used = time.monotonic()
        if item is _EOF:
            self.finished = True
            code = self.proc.wait()
            time.sleep(0.05)
            if code != 0 or self.result is None:
                raise SourceError(
                    f"{self.label}: the dlt runner exited with code {code}. stderr:\n{_stderr_tail(self.stderr)}"
                )
            return None
        assert isinstance(item, Frame)
        if item.tag == "E":
            self.close()
            raise SourceError(f"{self.label}: {(item.result or {}).get('error')}")
        if item.tag == "J":
            self.result = item.result
            return self.poll(wait)
        return item

    def wait_result(self) -> dict[str, Any]:
        """Drain the run to its end and return the final JSON result (discovery)."""
        while True:
            frame = self.poll(1.0)
            if frame is None:
                assert self.result is not None
                return self.result

    def close(self) -> None:
        """Stop the runner if it is still going."""
        self.finished = True
        if self.proc.poll() is None:
            self.proc.kill()
        with contextlib.suppress(Exception):
            self.proc.wait(timeout=5)


def run_to_result(spec: EnvSpec, request: dict[str, Any], *, timeout_seconds: int, label: str) -> dict[str, Any]:
    """Run a request whose only output is its final result (discovery)."""
    run = Run.start(spec, request, timeout_seconds=timeout_seconds, label=label)
    try:
        return run.wait_result()
    finally:
        run.close()


# ------------------------------------------------- live runs, keyed by scan token

_runs: dict[str, Run] = {}
_runs_lock = threading.Lock()


def take_run(token: str) -> Run | None:
    """Return the live run of a scan in this process, if any."""
    with _runs_lock:
        _reap_locked()
        return _runs.get(token)


def keep_run(token: str, run: Run) -> None:
    """Remember a live run so the scan's next batch continues it."""
    with _runs_lock:
        _runs[token] = run


def drop_run(token: str) -> None:
    """Forget and stop a scan's run."""
    with _runs_lock:
        run = _runs.pop(token, None)
    if run is not None:
        run.close()


def _reap_locked() -> None:
    """Stop runs nobody has read from for a while (a LIMIT query stops reading early)."""
    now = time.monotonic()
    for token, run in list(_runs.items()):
        if now - run.last_used > IDLE_RUN_SECONDS:
            run.close()
            del _runs[token]


@atexit.register
def _close_all() -> None:
    with _runs_lock:
        for run in _runs.values():
            run.close()
        _runs.clear()
