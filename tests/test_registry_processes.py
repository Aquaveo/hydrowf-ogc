"""Processes from hydrowf-registry (RemoteRegistry, ArgoManager in registry mode), with a fake registry.

The fake serves what the registry's GET /legos returns: the public legos' JSON. Here, the shipped
registry/legos.yaml, published public.
"""
import copy
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
import yaml
from pygeoapi.config import get_config
from pygeoapi.openapi import get_oas
from starlette.testclient import TestClient
from test_manager import CNG, INPUTS, MANIFEST, MANIFEST_KEY, ME, SIMULATED, FakeRuntime, presigner_reading
from test_app import AUDIENCE, ISSUER, URL, StubKeys, token

from hydrowf_core.auth import BearerAuth
from hydrowf_core.models import LEGOS
from hydrowf_ogc import app as service, caller
from hydrowf_ogc.manager import ArgoManager, registry_from
from hydrowf_ogc import registry as registry_module
from hydrowf_ogc.registry import RegistryError, RemoteRegistry, fetch_public_legos

REPO = Path(__file__).parents[1]
CONFIG = REPO / "pygeoapi-config.yml"
SHIPPED = [{**lego.model_dump(mode="json"), "visibility": "public"}
           for lego in LEGOS.validate_python(yaml.safe_load((REPO / "registry" / "legos.yaml").read_text()))]
SIM = "ngiab-simulation"


def body(id, version=None):
    found = next(b for b in SHIPPED if b["id"] == id)
    return {**copy.deepcopy(found), **({"version": version} if version else {})}


class FakeRegistry:
    """GET /legos as the registry answers an anonymous caller; `down` makes it unreachable."""

    def __init__(self, bodies=None):
        self.bodies = list(SHIPPED if bodies is None else bodies)
        self.down = False
        self.calls = 0

    def __call__(self, url):
        self.calls += 1
        if self.down:
            raise OSError("connection refused")
        return copy.deepcopy(self.bodies)


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def remote(fake, ttl=60, clock=None):
    return RemoteRegistry("http://registry.test", ttl, fetch=fake, clock=clock or Clock())


# ---- RemoteRegistry: process ids, refreshes, failures


def test_every_public_pipeline_version_is_a_process_and_its_id_the_latest():
    fake = FakeRegistry(SHIPPED + [body(SIM, "0.2.0"), body(SIM, "0.10.0")])
    processes = remote(fake).processes()
    assert {f"{SIM}@0.1.0", f"{SIM}@0.2.0", f"{SIM}@0.10.0", SIM} <= set(processes)
    assert processes[SIM].version == "0.10.0"                  # SemVer order, not text order


def test_a_snapshot_is_kept_for_ttl_seconds():
    fake, clock = FakeRegistry(), Clock()
    registry = remote(fake, ttl=60, clock=clock)
    registry.processes()
    fake.bodies.append(body(SIM, "0.2.0"))
    clock.now = 59
    assert f"{SIM}@0.2.0" not in registry.processes() and fake.calls == 1
    clock.now = 60
    assert f"{SIM}@0.2.0" in registry.processes() and fake.calls == 2


def test_an_unreachable_registry_keeps_serving_the_last_snapshot():
    fake, clock = FakeRegistry(), Clock()
    registry = remote(fake, ttl=60, clock=clock)
    before = registry.processes()
    fake.down, clock.now = True, 60
    assert registry.processes() == before
    clock.now = 61                                             # the failure counts as a refresh: no retry storm
    registry.processes()
    assert fake.calls == 2


def test_never_read_and_unreachable_is_an_error():
    fake = FakeRegistry()
    fake.down = True
    with pytest.raises(RegistryError, match="can't be read"):
        remote(fake).processes()


def test_a_lego_that_does_not_validate_here_is_left_out_alone():
    broken = {**body(SIM, "0.2.0"), "unknown_field": 1}       # e.g. written by a newer hydrowf-core
    processes = remote(FakeRegistry(SHIPPED + [broken])).processes()
    assert f"{SIM}@0.2.0" not in processes and f"{SIM}@0.1.0" in processes


def test_a_pipeline_whose_tool_is_not_public_is_left_out():
    orphan = body(SIM, "0.3.0")
    orphan["steps"] = [{**step, "tool": {"id": "private-tool", "version": "1.0.0"}} for step in orphan["steps"]]
    processes = remote(FakeRegistry(SHIPPED + [orphan])).processes()
    assert f"{SIM}@0.3.0" not in processes and processes[SIM].version == "0.1.0"


@pytest.mark.parametrize("section, message", [
    ("http://x", "is not"), ({"url": "http://x", "other": 1}, "is not"), ({"url": "http://x", "ttl": "1m"}, "ttl"),
    ({"url": "http://x", "ttl": float("nan")}, "ttl"), ({"url": "http://x", "ttl": float("inf")}, "ttl"),
    ({"url": "http://x", "ttl": 0}, "ttl"), ({"url": "file:///etc"}, "http"), ({"url": "ftp://x"}, "http"),
])
def test_a_bad_registry_section_fails_at_startup(section, message):
    with pytest.raises(ValueError, match=message):
        registry_from(section)


def test_no_registry_url_means_the_configs_processes(monkeypatch):
    monkeypatch.delenv("HYDROWF_REGISTRY_URL", raising=False)
    assert registry_from(None) is None and registry_from({"ttl": 5}) is None
    monkeypatch.setenv("HYDROWF_REGISTRY_URL", "http://registry.env")
    assert registry_from({"url": "http://registry.config"}).url == "http://registry.env"


# ---- /ogc in registry mode


@pytest.fixture
def fake():
    return FakeRegistry()


@pytest.fixture
def runtime():
    return FakeRuntime()


@pytest.fixture
def client(fake, runtime, monkeypatch):
    monkeypatch.setenv("PYGEOAPI_CONFIG", str(CONFIG))
    monkeypatch.setenv("PYGEOAPI_SERVER_URL", URL)
    monkeypatch.setenv("HYDROWF_REGISTRY", str(REPO / "registry" / "legos.yaml"))
    monkeypatch.delenv("HYDROWF_REGISTRY_URL", raising=False)
    config = get_config()
    manager = config["server"]["manager"]
    manager["runtime"], manager["registry_fetch"] = runtime, fake
    manager["connection"]["registry"] = {"url": "http://registry.test", "ttl": 1}
    config["resources"] = {}                                   # nothing from the config: all from the registry
    app = service.create_app(config, get_oas(copy.deepcopy(config)), BearerAuth(ISSUER, AUDIENCE, keys=StubKeys()))
    return TestClient(app, base_url="http://testserver")


def test_ogc_lists_the_registrys_pipelines(client):
    ids = {p["id"] for p in client.get("/ogc/processes", params={"f": "json"}).json()["processes"]}
    assert {SIM, f"{SIM}@0.1.0"} <= ids


def test_a_versioned_process_describes_itself_by_its_own_id(client):
    description = client.get(f"/ogc/processes/{SIM}@0.1.0", params={"f": "json"}).json()
    assert description["id"] == f"{SIM}@0.1.0" and description["version"] == "0.1.0"
    assert "feature" in description["inputs"]


def test_a_pipeline_published_to_the_registry_becomes_runnable(client, fake, runtime):
    fake.bodies.append(body(SIM, "0.2.0"))
    response = client.post(f"/ogc/processes/{SIM}/execution", json={"inputs": INPUTS},
                           headers={"Authorization": f"Bearer {token()}", "Prefer": "respond-async"})
    assert response.status_code == 201, response.text
    [workflow] = runtime.submitted
    assert workflow["metadata"]["labels"]["hydrowf/version"] == "0.2.0"    # the id ran its latest version
    assert workflow["metadata"]["labels"]["hydrowf/owner"] == ME


def test_an_unknown_process_is_404(client):
    assert client.get("/ogc/processes/no-such-pipeline", params={"f": "json"}).status_code == 404


def test_an_unreachable_registry_lists_nothing_instead_of_failing(client, fake):
    fake.down = True                                            # never read before: no snapshot to fall back on
    response = client.get("/ogc/processes", params={"f": "json"})
    assert response.status_code == 200 and response.json()["processes"] == []


# ---- what the review asked for


def test_only_public_legos_are_used_whatever_the_registry_answers():
    private_tool = {**body("ngiab-run", "9.0.0"), "visibility": "owner"}
    uses_it = body(SIM, "0.4.0")
    uses_it["steps"] = [{**st, "tool": {"id": "ngiab-run", "version": "9.0.0"}} if st["tool"]["id"] == "ngiab-run"
                        else st for st in uses_it["steps"]]
    private_pipeline = {**body(SIM, "0.5.0"), "visibility": "group"}
    snapshot = remote(FakeRegistry(SHIPPED + [private_tool, uses_it, private_pipeline])).snapshot()
    assert ("ngiab-run", "9.0.0") not in {(x.id, x.version) for x in snapshot.legos}
    assert f"{SIM}@0.4.0" not in snapshot.processes and f"{SIM}@0.5.0" not in snapshot.processes


@pytest.mark.parametrize("answer", [{"detail": "oops"}, "text", None])
def test_an_answer_that_is_not_a_list_is_an_error(answer):
    with pytest.raises(RegistryError, match="can't be read"):
        remote(lambda url: answer).processes()


def test_elements_that_are_not_legos_are_left_out_alone():
    processes = remote(FakeRegistry(SHIPPED + [3, "x", None, []])).processes()
    assert f"{SIM}@0.1.0" in processes


def test_without_a_snapshot_failures_back_off():
    fake, clock = FakeRegistry(), Clock()
    fake.down = True
    registry = remote(fake, ttl=60, clock=clock)
    for _ in range(3):                                         # three requests in a row: one try
        with pytest.raises(RegistryError):
            registry.processes()
    assert fake.calls == 1
    clock.now = registry_module.RETRY_AFTER
    fake.down = False
    assert f"{SIM}@0.1.0" in registry.processes() and fake.calls == 2


def test_a_refresh_never_holds_other_requests_up():
    fake, clock = FakeRegistry(), Clock()
    registry = remote(fake, ttl=60, clock=clock)
    before = registry.snapshot()
    entered, release = threading.Event(), threading.Event()

    def slow(url):
        entered.set()
        release.wait(5)
        return copy.deepcopy(SHIPPED)

    registry._fetch, clock.now = slow, 60
    refresher = threading.Thread(target=registry.snapshot)
    refresher.start()
    assert entered.wait(5)                                     # one thread is inside the slow read
    started = time.monotonic()
    assert registry.snapshot() is before                       # another gets the current snapshot at once
    assert time.monotonic() - started < 1
    release.set()
    refresher.join(5)
    assert not refresher.is_alive() and registry.snapshot() is not before   # the refresh landed


def test_before_any_snapshot_concurrent_requests_read_the_registry_once():
    fake, entered, release = FakeRegistry(), threading.Event(), threading.Event()

    def slow(url):
        entered.set()
        release.wait(5)
        return fake(url)

    registry = remote(slow, ttl=60)
    results = []
    threads = [threading.Thread(target=lambda: results.append(registry.snapshot())) for _ in range(4)]
    for thread in threads:
        thread.start()
    assert entered.wait(5)
    release.set()
    for thread in threads:
        thread.join(5)
    assert fake.calls == 1 and len(results) == 4 and all(r is results[0] for r in results)


def test_a_short_ttl_retries_sooner_than_retry_after():
    fake, clock = FakeRegistry(), Clock()
    fake.down = True
    registry = remote(fake, ttl=2, clock=clock)
    with pytest.raises(RegistryError):
        registry.processes()
    clock.now, fake.down = 2, False                            # min(ttl, RETRY_AFTER) = 2 s
    assert registry.processes() and fake.calls == 2


def test_the_mapping_reads_ids_and_pipelines_from_one_snapshot(client):
    processes = client.app.state.api.manager.processes
    assert dict(processes.items()).keys() == set(processes.keys())


def test_a_versioned_process_runs_that_version(client, fake, runtime):
    fake.bodies.append(body(SIM, "0.2.0"))
    response = client.post(f"/ogc/processes/{SIM}@0.1.0/execution", json={"inputs": INPUTS},
                           headers={"Authorization": f"Bearer {token()}", "Prefer": "respond-async"})
    assert response.status_code == 201, response.text
    assert runtime.submitted[0]["metadata"]["labels"]["hydrowf/version"] == "0.1.0"


def registry_manager(fake):
    presigner = presigner_reading({MANIFEST_KEY: json.dumps(MANIFEST).encode()})
    return ArgoManager({
        "name": "hydrowf_ogc.manager.ArgoManager",
        "runtime": FakeRuntime([SIMULATED], outputs={"job-done": {"cng": CNG}}),
        "presigner": presigner,
        "registry_fetch": fake,
        "connection": {"bucket": "my-bucket", "registry": {"url": "http://registry.test"}},
    })


def test_results_find_an_outputs_data_lego_in_the_registry():
    with caller.acting_as(ME):
        results = registry_manager(FakeRegistry()).get_job_result("job-done")[1]
    assert results["cng"]["item"]["properties"]["hydrowf:data"] == "ngen-cng@1.0.0"


def test_results_answer_without_items_while_the_registry_was_never_read():
    fake = FakeRegistry()
    fake.down = True
    with caller.acting_as(ME):
        results = registry_manager(fake).get_job_result("job-done")[1]
    assert "item" not in results["cng"] and len(results["cng"]["files"]) == 2


def test_each_thread_has_its_own_processor(client):
    """pygeoapi's execute_process sets manager.processor and reads it back: per thread, two concurrent
    executions can't swap processors."""
    manager = client.app.state.api.manager
    barrier, seen = threading.Barrier(2), {}

    def run(name):
        manager.processor = name
        barrier.wait(5)                                        # both have set theirs before either reads
        seen[name] = manager.processor

    threads = [threading.Thread(target=run, args=(name,)) for name in ("a", "b")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(5)
    assert set(seen) == {"a", "b"}, "a thread didn't finish"
    assert seen == {"a": "a", "b": "b"}


# ---- the real HTTP read: http(s) only, no redirects, a size cap


@pytest.fixture
def server():
    """A local registry stand-in: `routes` maps a path to (status, headers, body bytes)."""
    routes = {}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            route = routes.get(self.path, (404, {}, b"{}"))
            if callable(route):
                try:
                    return route(self)
                except OSError:                                # the client hung up: fine
                    return
            status, headers, payload = route
            self.send_response(status)
            for key, value in headers.items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *args):
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}", routes
    httpd.shutdown()
    httpd.server_close()


def test_fetch_reads_the_public_legos(server):
    url, routes = server
    routes["/legos"] = (200, {"Content-Type": "application/json"}, json.dumps(SHIPPED).encode())
    assert f"{SIM}@0.1.0" in RemoteRegistry(url + "/").processes()        # a trailing slash is fine


def test_fetch_does_not_follow_redirects(server):
    url, routes = server
    routes["/legos"] = (302, {"Location": url + "/elsewhere"}, b"")
    routes["/elsewhere"] = (200, {}, json.dumps(SHIPPED).encode())
    with pytest.raises(RegistryError, match="answered 302"):
        fetch_public_legos(url)


def drip(payload, every=0.2):
    """A route that sends its answer a byte at a time: each wait is short, the whole is not."""
    def handle(request):
        request.send_response(200)
        request.send_header("Content-Length", str(len(payload)))
        request.end_headers()
        for byte in payload:
            request.wfile.write(bytes([byte]))
            request.wfile.flush()
            time.sleep(every)
    return handle


def drip_chunked(request):
    """Transfer-Encoding: chunked, one 1-byte chunk at a time."""
    request.protocol_version = "HTTP/1.1"
    request.send_response(200)
    request.send_header("Transfer-Encoding", "chunked")
    request.end_headers()
    for byte in b"[]" + b" " * 100:
        request.wfile.write(b"1\r\n" + bytes([byte]) + b"\r\n")
        request.wfile.flush()
        time.sleep(0.2)


def slow_headers(request):
    request.wfile.write(b"HTTP/1.1 200 OK\r\n")
    for byte in b"X-Slow: " + b"a" * 100:
        request.wfile.write(bytes([byte]))
        request.wfile.flush()
        time.sleep(0.2)


@pytest.mark.parametrize("route", [drip(b"[]" + b" " * 100), drip_chunked, slow_headers],
                         ids=["slow-body", "slow-chunked-body", "slow-headers"])
def test_a_slow_registry_is_cut_at_the_deadline(server, monkeypatch, route):
    url, routes = server
    routes["/legos"] = route
    monkeypatch.setattr(registry_module, "DEADLINE", 0.5)
    started = time.monotonic()
    with pytest.raises(RegistryError, match="took over 0.5 s"):
        fetch_public_legos(url)
    assert time.monotonic() - started < 2                     # the whole answer would take 20 s


def test_fetch_refuses_a_huge_answer(server, monkeypatch):
    url, routes = server
    routes["/legos"] = (200, {}, json.dumps(SHIPPED).encode())
    monkeypatch.setattr(registry_module, "MAX_BYTES", 1000)
    with pytest.raises(RegistryError, match="over 1000 bytes"):
        fetch_public_legos(url)


def test_fetch_refuses_urls_that_are_not_http():
    with pytest.raises(RegistryError, match="not http"):
        fetch_public_legos("file:///etc")


def test_an_answer_cut_short_is_refused(server):
    url, routes = server

    def short(request):                                        # declares 1000 bytes, sends "[]", hangs up
        request.send_response(200)
        request.send_header("Content-Length", "1000")
        request.end_headers()
        request.wfile.write(b"[]")
    routes["/legos"] = short
    with pytest.raises(RegistryError, match="cut short"):
        fetch_public_legos(url)


def test_a_declared_answer_over_max_bytes_is_refused_before_reading(server, monkeypatch):
    url, routes = server
    routes["/legos"] = (200, {"Content-Length": "5000"}, b"[]" + b" " * 4998)
    monkeypatch.setattr(registry_module, "MAX_BYTES", 1000)
    with pytest.raises(RegistryError, match="over 1000 bytes"):
        fetch_public_legos(url)
