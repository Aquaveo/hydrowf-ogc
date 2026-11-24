"""A caller's own pipelines: /ogc reads the registry with the caller's token, so the pipelines they
published privately are their processes, and nobody else's."""
import copy
import dataclasses
import json
import threading
import time

import jwt
import pytest
from pygeoapi.config import get_config
from pygeoapi.openapi import get_oas
from starlette.testclient import TestClient
from test_app import AUDIENCE, ISSUER, URL, StubKeys, token
from test_manager import INPUTS, ME, SOMEONE, FakeRuntime
from test_registry_processes import CONFIG, REPO, SHIPPED, SIM, Clock, body, server  # noqa: F401

from hydrowf_core.auth import BearerAuth
from hydrowf_ogc import app as service, caller
from hydrowf_ogc import registry as registry_module
from hydrowf_ogc.registry import RETRY_AFTER, RegistryError, RemoteRegistry, fetch_legos

MINE = "my-flow"


def private(owner=ME, id=MINE, version="0.1.0"):
    """A pipeline published with owner visibility: the simulation's steps under the caller's own id."""
    return {**body(SIM), "id": id, "version": version, "visibility": "owner", "owner": owner}


class TokenRegistry:
    """GET /legos as the registry answers: the public legos, plus the token holder's own (`own` maps a
    sub to the bodies the registry would add for them). `leak` adds bodies to every token's answer: a
    registry answering wrongly."""

    def __init__(self, own=None, leak=()):
        self.own, self.leak = dict(own or {}), list(leak)
        self.public = list(SHIPPED)            # what everyone sees: a test may publish more
        self.down = False
        self.tokens = []                       # the token of each read, None for anonymous ones

    def __call__(self, url, token=None):
        self.tokens.append(token)
        if self.down:
            raise OSError("connection refused")
        answer = copy.deepcopy(self.public)
        if token is not None:
            sub = jwt.decode(token, options={"verify_signature": False})["sub"]
            answer += copy.deepcopy(self.own.get(sub, [])) + copy.deepcopy(self.leak)
        return answer


def remote(fake, ttl=60, clock=None):
    return RemoteRegistry("http://registry.test", ttl, fetch=fake, clock=clock or Clock())


# ---- RemoteRegistry


def test_a_callers_own_pipeline_is_in_their_view_only():
    registry = remote(TokenRegistry(own={ME: [private()]}))
    assert MINE in registry.processes(ME, token())
    assert MINE not in registry.processes()                              # anonymous: public only
    assert MINE not in registry.processes(SOMEONE, token(sub=SOMEONE))


def test_an_ids_latest_version_is_its_latest_of_public_and_own():
    fake = TokenRegistry(own={ME: [private(version="0.1.0")]})
    fake.public.append({**private(version="0.2.0"), "visibility": "public"})
    registry = remote(fake)
    mine = registry.processes(ME, token())
    assert mine[MINE].version == "0.2.0"                                 # not the caller's older private one
    assert {f"{MINE}@0.1.0", f"{MINE}@0.2.0"} <= set(mine)
    assert registry.processes()[MINE].version == "0.2.0"                 # the same for everyone
    fake.own[ME].append(private(version="0.3.0"))
    assert remote(fake).processes(ME, token())[MINE].version == "0.3.0"  # a newer private one is the latest


def test_a_callers_view_follows_the_public_snapshot_without_waiting_for_their_own_read():
    fake, clock = TokenRegistry(own={ME: [private()]}), Clock()
    registry = remote(fake, ttl=60, clock=clock)
    registry.processes()                                                 # public read at 0
    clock.now = 30
    registry.processes(ME, token())                                      # own legos read at 30, due at 90
    fake.public.append(body(SIM, "9.0.0"))
    clock.now = 60                                                       # the public snapshot is due
    seen = registry.processes(ME, token())
    assert seen[SIM].version == "9.0.0" and MINE in seen
    assert len([t for t in fake.tokens if t is not None]) == 1           # their own legos weren't read again


def test_someone_elses_private_lego_is_left_out_whatever_the_registry_answers():
    registry = remote(TokenRegistry(leak=[private(owner=SOMEONE)]))
    assert MINE not in registry.processes(ME, token())


def test_the_callers_token_is_what_the_registry_is_read_with():
    fake, mine = TokenRegistry(), token()
    remote(fake).processes(ME, mine)
    assert fake.tokens == [None, mine]                                   # the public read, then the caller's


def test_a_callers_view_is_kept_for_ttl_seconds():
    fake, clock = TokenRegistry(), Clock()
    registry = remote(fake, ttl=60, clock=clock)
    registry.processes(ME, token())
    fake.own[ME] = [private()]
    clock.now = 59
    assert MINE not in registry.processes(ME, token())
    clock.now = 60
    assert MINE in registry.processes(ME, token())


def test_an_unreachable_registry_serves_the_callers_last_view_then_tries_again_soon():
    fake, clock = TokenRegistry(own={ME: [private()]}), Clock()
    registry = remote(fake, ttl=60, clock=clock)
    assert MINE in registry.processes(ME, token())
    clock.now, fake.down = 60, True
    assert MINE in registry.processes(ME, token())                       # the last view, not the public one
    reads = len(fake.tokens)
    clock.now = 60 + RETRY_AFTER - 1
    registry.processes(ME, token())
    assert len(fake.tokens) == reads                                     # no read on every request
    fake.down, fake.own[ME] = False, [private(), private(version="0.2.0")]
    clock.now = 60 + RETRY_AFTER
    assert f"{MINE}@0.2.0" in registry.processes(ME, token())


def test_before_any_view_of_theirs_an_unreachable_registry_serves_the_public_one():
    fake = TokenRegistry()
    registry = remote(fake)
    registry.processes()                                                 # the public snapshot exists
    fake.down = True
    assert SIM in registry.processes(ME, token())


def test_only_the_most_recent_callers_views_are_kept(monkeypatch):
    monkeypatch.setattr(registry_module, "MAX_CALLERS", 2)
    fake = TokenRegistry()
    registry = remote(fake)
    registry.processes()                                                 # the public snapshot first

    def reads_for(sub):
        before = len(fake.tokens)
        registry.processes(sub, token(sub=sub))
        return len(fake.tokens) - before

    for sub in ("a", "b", ME):
        assert reads_for(sub) == 1
    assert reads_for(ME) == 0                                            # kept
    assert reads_for("a") == 1                                           # the least recently used went


def test_a_registry_refusing_the_callers_token_leaves_them_the_public_view_and_says_why(caplog):
    def refusing(url, token=None):
        if token:
            raise RegistryError("the registry answered 401 Unauthorized")
        return copy.deepcopy(SHIPPED)

    registry = remote(refusing)
    processes = registry.processes(ME, token())
    assert SIM in processes and MINE not in processes
    assert "401 Unauthorized" in caplog.text


def test_the_token_never_reaches_the_log(caplog):
    secret = token()

    def failing(url, token=None):
        if token:
            raise OSError(f"connection to {url} with {token} refused")       # a message that holds it
        return copy.deepcopy(SHIPPED)

    remote(failing).processes(ME, secret)
    assert caplog.records and secret not in caplog.text


@pytest.mark.parametrize("bad", [{"id": ["a"]}, {"version": {"x": 1}}, {"id": None, "version": None}])
def test_a_malformed_lego_is_left_out_alone_in_both_views(bad):
    broken = {**private(), **bad}
    public = remote(lambda url, token=None: copy.deepcopy(SHIPPED) + [{**broken, "visibility": "public"}])
    assert SIM in public.processes()
    registry = remote(TokenRegistry(own={ME: [broken, private(id="other-flow")]}))
    assert "other-flow" in registry.processes(ME, token())


def test_one_read_refreshes_a_callers_view_while_their_other_requests_get_the_current_one():
    clock, entered, release = Clock(), threading.Event(), threading.Event()
    fake = TokenRegistry(own={ME: [private()]})
    registry = remote(fake, ttl=60, clock=clock)
    registry.processes(ME, token())
    clock.now = 60

    def slow(url, token=None):
        if token is not None:                                            # only the caller's read is slow
            entered.set()
            release.wait(5)
        return fake(url, token)

    registry._fetch = slow
    refresher = threading.Thread(target=registry.processes, args=(ME, token()))
    refresher.start()
    assert entered.wait(5)
    started = time.monotonic()
    assert MINE in registry.processes(ME, token())                       # served now, not after the read
    assert time.monotonic() - started < 1
    release.set()
    refresher.join(5)
    caller_reads = [t for t in fake.tokens if t is not None]
    assert len(caller_reads) == 2                                        # the first view and one refresh: no third


# ---- the service: list, describe and run the caller's own pipeline


@pytest.fixture
def fake():
    return TokenRegistry(own={ME: [private()]})


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
    config["resources"] = {}
    app = service.create_app(config, get_oas(copy.deepcopy(config)), BearerAuth(ISSUER, AUDIENCE, keys=StubKeys()))
    return TestClient(app, base_url="http://testserver")


def auth(sub=ME):
    return {"Authorization": f"Bearer {token(sub=sub)}"}


def listed(client, headers=None):
    answer = client.get("/ogc/processes", params={"f": "json"}, headers=headers or {})
    assert answer.status_code == 200, answer.text
    return {p["id"] for p in answer.json()["processes"]}


def test_the_caller_lists_and_describes_their_own_pipeline(client):
    assert {MINE, f"{MINE}@0.1.0", SIM} <= listed(client, auth())
    described = client.get(f"/ogc/processes/{MINE}", params={"f": "json"}, headers=auth())
    assert described.status_code == 200 and "feature" in described.json()["inputs"]


def test_nobody_else_lists_or_describes_it(client):
    assert MINE not in listed(client)
    assert MINE not in listed(client, auth(SOMEONE))
    assert client.get(f"/ogc/processes/{MINE}", params={"f": "json"}, headers=auth(SOMEONE)).status_code == 404


def test_the_caller_runs_their_own_pipeline(client, runtime):
    response = client.post(f"/ogc/processes/{MINE}/execution", json={"inputs": INPUTS},
                           headers={**auth(), "Prefer": "respond-async"})
    assert response.status_code == 201, response.text
    [workflow] = runtime.submitted
    labels = workflow["metadata"]["labels"]
    assert (labels["hydrowf/pipeline"], labels["hydrowf/version"], labels["hydrowf/owner"]) == (MINE, "0.1.0", ME)


def test_nobody_else_runs_it(client, runtime):
    response = client.post(f"/ogc/processes/{MINE}/execution", json={"inputs": INPUTS},
                           headers={**auth(SOMEONE), "Prefer": "respond-async"})
    assert response.status_code == 404 and runtime.submitted == []


def test_the_token_is_not_kept_past_the_request():
    with caller.acting_as(ME, "a-token"):
        assert caller.identity() == (ME, "a-token")
    assert caller.identity() == (None, None)


# ---- the real read sends the token, and only when there is one


def test_fetch_sends_the_token_as_a_bearer_credential(server):  # noqa: F811  (the imported fixture)
    url, routes = server
    seen = []

    def legos(request):
        seen.append(request.headers.get("Authorization"))
        payload = json.dumps([]).encode()
        request.send_response(200)
        request.send_header("Content-Length", str(len(payload)))
        request.end_headers()
        request.wfile.write(payload)

    routes["/legos"] = legos
    fetch_legos(url, "t0ken")
    fetch_legos(url)
    assert seen == ["Bearer t0ken", None]


# ---- each step's status


def started(client, runtime, sub=ME):
    response = client.post(f"/ogc/processes/{MINE}/execution", json={"inputs": INPUTS},
                           headers={**auth(sub), "Prefer": "respond-async"})
    assert response.status_code == 201, response.text
    return runtime.submitted[-1]["metadata"]["name"]


def steps_of(client, job_id, sub=ME):
    return client.get(f"/ogc/jobs/{job_id}/steps", headers=auth(sub))


def test_every_planned_step_is_listed_with_its_status(client, runtime):
    job = started(client, runtime)
    first, second, *rest = [step["name"] for step in private()["steps"]]
    runtime.runs[job] = dataclasses.replace(runtime.runs[job], phase="Running",
                                            steps={first: "Succeeded", second: "Running"})
    answer = steps_of(client, job)
    assert answer.status_code == 200, answer.text
    assert answer.json() == {"jobID": job, "steps": {first: "successful", second: "running",
                                                     **{name: "pending" for name in rest}}}


def test_steps_that_never_ran_in_a_finished_job_are_skipped(client, runtime):
    job = started(client, runtime)
    first, second, *rest = [step["name"] for step in private()["steps"]]
    runtime.runs[job] = dataclasses.replace(runtime.runs[job], phase="Failed",
                                            steps={first: "Error", second: "Omitted"})
    assert steps_of(client, job).json()["steps"] == {first: "failed", second: "skipped",
                                                     **{name: "skipped" for name in rest}}


def test_someone_elses_job_steps_are_404_and_a_token_is_needed(client, runtime):
    job = started(client, runtime)
    assert steps_of(client, job, SOMEONE).status_code == 404
    assert client.get(f"/ogc/jobs/{job}/steps").status_code == 401
    assert steps_of(client, "no-such-job").status_code == 404


def test_steps_come_in_the_pipelines_order(client, runtime):
    job = started(client, runtime)
    names = [step["name"] for step in private()["steps"]]
    runtime.runs[job] = dataclasses.replace(runtime.runs[job], phase="Running",
                                            steps={names[-1]: "Running", names[0]: "Succeeded"})
    assert list(steps_of(client, job).json()["steps"]) == names
