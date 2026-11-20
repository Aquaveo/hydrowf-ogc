"""Where the processes' legos come from: a YAML file, or the hydrowf-registry service.

A file (`load_legos`) is checked as a whole (hydrowf_core.registry.check) when it is loaded, so a broken
file fails at startup instead of on a user's request.

The service (`RemoteRegistry`) is read over HTTP: its public legos, one request, kept for `ttl` seconds.

    registry = RemoteRegistry("http://hydrowf-registry:8000", ttl=60)
    registry.processes()      # {"ngiab-simulation": Pipeline, "ngiab-simulation@0.1.0": Pipeline, ...}
    registry.legos()          # every public lego that validates, for render() and the descriptions

Each public pipeline version is a process `<id>@<version>`, and `<id>` is its latest version. Only
public legos are used, whatever the service answers. A lego that doesn't validate with this
hydrowf-core, or a pipeline whose tools aren't among the public legos, is left out and logged: one bad
entry never takes the others down.

Once there is a snapshot, refreshing never holds requests up: one thread refreshes while the others
keep the current snapshot. Before the first one, requests wait for the first read, which is bounded (see
fetch_public_legos): at most MAX_BYTES, http(s) only, no redirects, and cut at DEADLINE seconds once
connected. When the service can't be reached, the last snapshot is served; before any, RegistryError, and
the next try waits RETRY_AFTER seconds (or `ttl`, if shorter) instead of happening on every request.
Versions are immutable and never deleted, so a cached lego is never wrong; only new ones wait up to `ttl`.
"""
from __future__ import annotations

import json
import logging
import socket
import threading
import time
from collections.abc import Callable, ItemsView, Iterator, KeysView, Mapping, ValuesView
from dataclasses import dataclass
from http.client import HTTPConnection, HTTPSConnection
from pathlib import Path
from urllib.parse import urlsplit

import yaml
from pydantic import TypeAdapter, ValidationError

from hydrowf_core.models import LEGOS, Pipeline
from hydrowf_core.models.lego import Lego, Visibility
from hydrowf_core.registry import check
from hydrowf_core.registry.validate import check_pipeline

LOGGER = logging.getLogger(__name__)
LEGO = TypeAdapter(Lego)
TIMEOUT = 5                               # seconds for each socket operation
DEADLINE = 15                             # seconds for a whole read of the registry
MAX_BYTES = 32 * 1024 * 1024              # a registry answer bigger than this is refused
RETRY_AFTER = 5                           # seconds between tries while there is no snapshot yet


class RegistryError(ValueError):
    """The registry has problems, can't be read, or doesn't hold the lego asked for."""


def load_legos(path: str | Path) -> list[Lego]:
    legos = LEGOS.validate_python(yaml.safe_load(Path(path).read_text()))
    problems = check(legos)
    if problems:
        raise RegistryError(f"{path}: " + "; ".join(problems))
    return legos


def find_pipeline(legos: list[Lego], id: str, version: str) -> Pipeline:
    found = next((x for x in legos if isinstance(x, Pipeline) and (x.id, x.version) == (id, version)), None)
    if found is None:
        raise RegistryError(f"pipeline {id}@{version} is not in the registry")
    return found


def version_key(version: str) -> tuple:
    """SemVer order (0.10.0 after 0.9.0); never raises, so one odd version can't sink a snapshot."""
    try:
        return (0, tuple(int(part) for part in version.split(".")))
    except ValueError:
        return (1, version)


@dataclass(frozen=True)
class Snapshot:
    legos: list[Lego]                      # every public lego that validates
    processes: dict[str, Pipeline]         # process id -> pipeline


def snapshot_of(bodies: object) -> Snapshot:
    """The usable legos and processes in the registry's answer: public legos that validate, and the
    pipelines whose tools and data are among them."""
    if not isinstance(bodies, list):
        raise RegistryError(f"the registry answered {type(bodies).__name__}, not a list of legos")
    legos: list[Lego] = []
    for body in bodies:
        if not isinstance(body, dict):
            LOGGER.warning("registry answer: an element that isn't a lego left out")
            continue
        try:
            lego = LEGO.validate_python(body)
        except ValidationError as err:
            LOGGER.warning("registry lego %s@%s left out: it doesn't validate here: %s",
                           body.get("id"), body.get("version"), err)
            continue
        if lego.visibility != Visibility.PUBLIC:   # never trust the answer for this: /ogc runs public legos only
            LOGGER.warning("registry lego %s@%s left out: it isn't public", lego.id, lego.version)
            continue
        legos.append(lego)
    by_key = {(lego.id, lego.version): lego for lego in legos}
    pipelines = []
    for lego in legos:
        if isinstance(lego, Pipeline):
            problems = check_pipeline(lego, by_key)
            if problems:
                LOGGER.warning("registry pipeline %s@%s left out: %s", lego.id, lego.version, "; ".join(problems))
            else:
                pipelines.append(lego)
    processes: dict[str, Pipeline] = {}
    for pipeline in sorted(pipelines, key=lambda p: (p.id, version_key(p.version))):
        processes[f"{pipeline.id}@{pipeline.version}"] = pipeline
        processes[pipeline.id] = pipeline   # sorted oldest first: the last one written is the latest
    return Snapshot(legos, processes)


class _Watchdog:
    """Cuts a read at `deadline` seconds by shutting its socket, which ends a read blocked anywhere in the
    answer. It holds the socket itself: http.client drops connection.sock once a response will close the
    connection, while the response goes on reading from it. The lock and `done` keep a late cut away from
    a socket the reader has already closed (its file descriptor could be someone else's by then)."""

    def __init__(self, deadline: float):
        self.expired = threading.Event()
        self._lock, self._done, self._sockets = threading.Lock(), False, []
        self._timer = threading.Timer(deadline, self._cut)
        self._timer.daemon = True

    def start(self) -> None:
        self._timer.start()

    def watch(self, sock: socket.socket) -> None:
        with self._lock:
            self._sockets.append(sock)

    def _cut(self) -> None:
        with self._lock:
            if self._done:
                return
            self.expired.set()
            for sock in self._sockets:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass                   # already closed

    def finish(self) -> None:
        """Before the reader closes anything: no cut after this."""
        with self._lock:
            self._done = True
        self._timer.cancel()


def fetch_public_legos(url: str) -> object:
    """GET <url>/legos, anonymously: the registry's public legos (anonymous callers see only those).

    http(s) only; a redirect or any status but 200 is a RegistryError (nothing is followed), and so is an
    answer declared or read past MAX_BYTES, or cut shorter than its Content-Length. HTTP(S)_PROXY isn't
    used, and the URL's query string is dropped.

    Time: connecting (DNS, TCP, TLS) is bounded by the system resolver and TIMEOUT per address and for
    the TLS handshake; from then on, sending the request and reading the headers and body, the read is cut
    at DEADLINE seconds by a watchdog that shuts the socket. TIMEOUT alone only bounds each wait for a byte,
    so a server sending a byte at a time could otherwise hold a read for hours. A connect that ends after
    the deadline is refused too."""
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise RegistryError(f"the registry URL {url!r} is not http(s)")
    too_slow = f"the registry took over {DEADLINE} s to answer"
    connection_class = HTTPSConnection if parts.scheme == "https" else HTTPConnection
    connection = connection_class(parts.hostname, parts.port, timeout=TIMEOUT)
    watchdog = _Watchdog(DEADLINE)
    watchdog.start()
    response = None
    try:
        connection.connect()
        watchdog.watch(connection.sock)    # for https, the wrapped socket the answer is read from
        if watchdog.expired.is_set():      # the deadline passed while connecting: the cut missed this socket
            raise RegistryError(too_slow)
        connection.request("GET", parts.path.rstrip("/") + "/legos", headers={"Accept": "application/json"})
        response = connection.getresponse()
        if response.status != 200:
            raise RegistryError(f"the registry answered {response.status} {response.reason}")
        if response.length is not None and response.length > MAX_BYTES:
            raise RegistryError(f"the registry's answer is over {MAX_BYTES} bytes")
        chunks, size = [], 0
        while chunk := response.read1(64 * 1024):
            size += len(chunk)
            if size > MAX_BYTES:
                raise RegistryError(f"the registry's answer is over {MAX_BYTES} bytes")
            chunks.append(chunk)
        if response.length:                # bytes still owed: the server closed early, or the watchdog cut
            raise RegistryError("the registry's answer was cut short")
    except Exception as err:               # OSError, HTTPException, and SSL's ValueError/AttributeError on a cut
        if watchdog.expired.is_set():
            raise RegistryError(too_slow) from err
        raise
    finally:
        watchdog.finish()                  # first: no cut can touch the sockets closed below
        if response is not None:
            response.close()               # when the server closes, the response holds the socket
        connection.close()
    if watchdog.expired.is_set():          # cut mid-answer: what arrived is a fragment, not the answer
        raise RegistryError(too_slow)
    return json.loads(b"".join(chunks))


class RemoteRegistry:
    def __init__(self, url: str, ttl: float = 60, fetch: Callable[[str], object] = fetch_public_legos,
                 clock: Callable[[], float] = time.monotonic):
        self.url, self.ttl, self._fetch, self._clock = url, ttl, fetch, clock
        self._snapshot: Snapshot | None = None
        self._tried_at: float | None = None        # the last refresh, successful or not
        self._last_error: Exception | None = None
        self._refreshing = threading.Lock()        # requests run in worker threads: one refresh at a time

    def _due(self) -> bool:
        if self._tried_at is None:
            return True
        wait = self.ttl if self._snapshot is not None else min(self.ttl, RETRY_AFTER)
        return self._clock() - self._tried_at >= wait

    def _refresh(self) -> None:
        """Read the registry once (the caller holds _refreshing). A failure keeps the snapshot we had."""
        try:
            self._snapshot = snapshot_of(self._fetch(self.url))
            self._last_error = None
        except Exception as err:           # unreachable, too slow, a bad answer
            self._last_error = err
            LOGGER.error("the lego registry at %s can't be read%s: %s", self.url,
                         "; serving the last snapshot" if self._snapshot else "", err)
        finally:
            self._tried_at = self._clock()

    def snapshot(self) -> Snapshot:
        if self._due():
            if self._snapshot is not None:
                if self._refreshing.acquire(blocking=False):   # one thread refreshes; the rest don't wait
                    try:
                        if self._due():
                            self._refresh()
                    finally:
                        self._refreshing.release()
            else:
                with self._refreshing:                          # nothing to serve yet: wait for the read
                    if self._snapshot is None and self._due():
                        self._refresh()
        if self._snapshot is None:
            raise RegistryError(f"the lego registry at {self.url} can't be read: {self._last_error}")
        return self._snapshot

    def legos(self) -> list[Lego]:
        return self.snapshot().legos

    def processes(self) -> dict[str, Pipeline]:
        return self.snapshot().processes


class RegistryProcesses(Mapping):
    """ArgoManager.processes when the processes come from the registry: what pygeoapi reads (ids, `in`,
    len, items) answered from the current snapshot. Values are the pipelines; pygeoapi only passes the
    ids on to get_processor."""

    def __init__(self, registry: RemoteRegistry):
        self.registry = registry

    def _current(self) -> dict[str, Pipeline]:
        try:
            return self.registry.processes()
        except RegistryError as err:       # never read yet and unreachable now: no processes, not a crash
            LOGGER.error("%s", err)
            return {}

    def __getitem__(self, process_id: str) -> Pipeline:
        return self._current()[process_id]

    def __iter__(self) -> Iterator[str]:
        return iter(self._current())

    def __len__(self) -> int:
        return len(self._current())

    # Views over one snapshot: a refresh between reading the ids and their pipelines can't mix two.
    def keys(self) -> KeysView:
        return self._current().keys()

    def items(self) -> ItemsView:
        return self._current().items()

    def values(self) -> ValuesView:
        return self._current().values()
