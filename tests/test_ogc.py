"""hydrowf_platform.ogc, the service: the shipped pygeoapi config on Starlette, with a fake Argo and stub keys."""
import asyncio
import json
import time
from pathlib import Path

import jwt
import pytest
import yaml
from cryptography.hazmat.primitives.asymmetric import rsa
from jwt.algorithms import RSAAlgorithm
from jwt.exceptions import PyJWKClientConnectionError, PyJWKClientError
from pygeoapi.config import get_config
from pygeoapi.openapi import generate_openapi_document
from starlette.testclient import TestClient
from test_manager import DONE, INPUTS, ME, SOMEONE, THEIRS, FakeRuntime

from hydrowf_core import auth
from hydrowf_platform import caller, ogc

REPO = Path(__file__).parents[1]
CONFIG = REPO / "pygeoapi-config.yml"
URL = "http://testserver/ogc"

ISSUER = "https://keycloak.test/realms/hydrowf"
AUDIENCE = "hydrowf-api"
KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)       # the issuer's signing key
OTHER_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
SIGNING_KEY = jwt.PyJWK({**RSAAlgorithm.to_jwk(KEY.public_key(), as_dict=True), "alg": "RS256", "kid": "test"})


def token(key=KEY, **claims):
    """A token minted now (inside the test, so a slow suite can't expire it)."""
    now = int(time.time())
    body = {"iss": ISSUER, "aud": AUDIENCE, "sub": ME, "iat": now, "exp": now + 300, **claims}
    return jwt.encode({k: v for k, v in body.items() if v is not None}, key, algorithm="RS256",
                      headers={"kid": "test"})


class StubKeys:
    """The issuer's JWKS as PyJWKClient serves it: the signing key, or the error a broken issuer gives."""

    def __init__(self, error=None):
        self.error = error

    def get_signing_key_from_jwt(self, token):
        if self.error:
            raise self.error
        return SIGNING_KEY


@pytest.fixture
def runtime():
    return FakeRuntime([DONE, THEIRS])


@pytest.fixture
def client(runtime, monkeypatch):
    """The service as app_from_env builds it from the shipped config, with bearer tokens on."""
    monkeypatch.setenv("PYGEOAPI_CONFIG", str(CONFIG))
    monkeypatch.setenv("PYGEOAPI_SERVER_URL", URL)
    monkeypatch.setenv("HYDROWF_REGISTRY", str(REPO / "registry" / "legos.yaml"))
    config = get_config()
    config["server"]["manager"]["runtime"] = runtime              # ArgoManager takes a runtime from its config
    openapi = yaml.safe_load(generate_openapi_document(CONFIG, "yaml"))
    app = ogc.create_app(config, openapi, auth.BearerAuth(ISSUER, AUDIENCE, keys=StubKeys()))
    return TestClient(app, base_url="http://testserver")


def bearer(sub=ME, **claims):
    return {"Authorization": f"Bearer {token(sub=sub, **claims)}"}


# ---- routes: under server.url's path, open catalogue, jobs need a token


@pytest.mark.parametrize("path", ["/ogc", "/ogc/conformance", "/ogc/openapi", "/ogc/processes",
                                  "/ogc/processes/ngiab-simulation"])
def test_the_catalogue_is_open(client, path):
    assert client.get(path).status_code == 200


def test_links_are_under_the_server_url(client):
    links = client.get("/ogc/processes", params={"f": "json"}).json()["links"]
    assert links and all(link["href"].startswith(URL) for link in links)


@pytest.mark.parametrize("method, path", [
    ("GET", "/ogc/jobs"),
    ("GET", "/ogc/jobs/job-done"),
    ("GET", "/ogc/jobs/job-done/results"),
    ("DELETE", "/ogc/jobs/job-done"),
    ("POST", "/ogc/processes/ngiab-simulation/execution"),
])
def test_jobs_need_a_token(client, runtime, method, path):
    response = client.request(method, path, json={"inputs": INPUTS})
    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == "Bearer"
    assert runtime.submitted == [] and "job-done" in runtime.runs


BAD = {
    "wrong key": lambda: {"Authorization": f"Bearer {token(key=OTHER_KEY)}"},
    "expired": lambda: bearer(exp=int(time.time()) - 120),
    "another audience": lambda: bearer(aud="someone-else"),
    "empty": lambda: {"Authorization": "Bearer"},
    "not a jwt": lambda: {"Authorization": "Bearer abc"},
}


@pytest.mark.parametrize("bad", BAD)
@pytest.mark.parametrize("path", ["/ogc/processes", "/ogc/jobs"])
def test_a_bad_token_is_refused_even_on_open_reads(client, bad, path):
    assert client.get(path, headers=BAD[bad]()).status_code == 401


def test_other_schemes_are_no_credential(client):
    basic = {"Authorization": "Basic dXNlcjpwYXNz"}
    assert client.get("/ogc/processes", headers=basic).status_code == 200
    assert client.get("/ogc/jobs", headers=basic).status_code == 401


def test_without_an_issuer_jobs_are_closed(client):
    app = ogc.create_app(client.app.state.api.config, client.app.state.api.openapi, bearer=None)
    closed = TestClient(app, base_url="http://testserver")
    assert closed.get("/ogc/processes").status_code == 200
    assert closed.get("/ogc/jobs", headers=bearer()).status_code == 401


@pytest.mark.parametrize("method, path, allowed", [
    ("POST", "/ogc/processes", "GET"),
    ("GET", "/ogc/processes/ngiab-simulation/execution", "POST"),
    ("PUT", "/ogc/jobs/job-done", "DELETE"),
])
def test_routes_refuse_methods_they_do_not_take(client, method, path, allowed):
    response = client.request(method, path, headers=bearer())
    assert response.status_code == 405 and allowed in response.headers["Allow"]


# ---- the token's owner reaches ArgoManager, in the worker thread


def test_jobs_are_the_tokens_owners(client):
    mine = client.get("/ogc/jobs", params={"f": "json"}, headers=bearer()).json()["jobs"]
    theirs = client.get("/ogc/jobs", params={"f": "json"}, headers=bearer(SOMEONE)).json()["jobs"]
    assert [job["jobID"] for job in mine] == ["job-done"]
    assert [job["jobID"] for job in theirs] == ["job-theirs"]


def test_someone_elses_job_is_not_found(client, runtime):
    assert client.get("/ogc/jobs/job-theirs", headers=bearer()).status_code == 404
    assert client.delete("/ogc/jobs/job-theirs", headers=bearer()).status_code == 404
    assert "job-theirs" in runtime.runs


def test_a_job_runs_as_the_tokens_owner(client, runtime):
    response = client.post("/ogc/processes/ngiab-simulation/execution", json={"inputs": INPUTS},
                           headers={**bearer(), "Prefer": "respond-async"})
    assert response.status_code == 201, response.text
    [workflow] = runtime.submitted
    assert workflow["metadata"]["labels"]["hydrowf/owner"] == ME


def test_the_owner_does_not_outlive_its_request(client):
    client.get("/ogc/jobs", headers=bearer())
    with pytest.raises(caller.NoCaller):            # neither here nor in the pool's threads
        caller.owner()
    assert client.get("/ogc/processes").status_code == 200


def test_a_failing_issuer_is_a_server_error(client):
    failing = auth.BearerAuth(ISSUER, AUDIENCE, keys=StubKeys(PyJWKClientConnectionError("down")))
    app = ogc.create_app(client.app.state.api.config, client.app.state.api.openapi, failing)
    with pytest.raises(PyJWKClientConnectionError):         # TestClient re-raises what would be a 500
        TestClient(app).get("/ogc/jobs", headers=bearer())


# ---- startup: settings checked once, before the first request


def test_a_bad_manager_config_fails_at_startup(client):
    config = client.app.state.api.config
    config["server"]["manager"]["connection"]["retention"] = {"days": 1.5}
    with pytest.raises(ValueError):
        ogc.create_app(config, client.app.state.api.openapi, None)


def test_a_token_whose_key_the_issuer_lacks_is_refused(client):
    unknown_kid = auth.BearerAuth(ISSUER, AUDIENCE, keys=StubKeys(PyJWKClientError("no matching kid")))
    app = ogc.create_app(client.app.state.api.config, client.app.state.api.openapi, unknown_kid)
    assert TestClient(app).get("/ogc/jobs", headers=bearer()).status_code == 401


# ---- what the review asked for: auth before the body, a bounded body, the OpenAPI document of this run


def asgi(app, method, path, headers, receive):
    """One request straight through the ASGI app, with our own `receive`: (status, sent messages)."""
    sent = []

    async def send(message):
        sent.append(message)

    scope = {"type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1", "method": method,
             "scheme": "http", "path": path, "raw_path": path.encode(), "query_string": b"", "root_path": "",
             "server": ("testserver", 80), "client": ("127.0.0.1", 1),
             "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()]}
    asyncio.run(app(scope, receive, send))
    return next(m["status"] for m in sent if m["type"] == "http.response.start")


EXECUTE = "/ogc/processes/ngiab-simulation/execution"


def test_an_anonymous_caller_is_refused_before_its_body_is_read(client):
    asked = []

    async def receive():                                   # a spy: records each ask for the body
        asked.append(True)
        return {"type": "http.request", "body": b"x" * (ogc.MAX_BODY + 1), "more_body": False}

    assert asgi(client.app, "POST", EXECUTE, {}, receive) == 401 and asked == []


def test_reading_stops_once_the_body_passes_max_body(client):
    asked = []

    async def receive():                                   # a body that never ends
        asked.append(True)
        return {"type": "http.request", "body": b"x" * 65536, "more_body": True}

    assert asgi(client.app, "POST", EXECUTE, bearer(), receive) == 413
    assert len(asked) <= ogc.MAX_BODY // 65536 + 2         # not read to the end, then measured


def test_a_body_over_max_body_is_refused(client, runtime):
    response = client.post("/ogc/processes/ngiab-simulation/execution", content=b"x" * (ogc.MAX_BODY + 1),
                           headers={**bearer(), "Content-Type": "application/json"})
    assert response.status_code == 413 and runtime.submitted == []


def test_a_chunked_body_is_read(client, runtime):           # no Content-Length: counted as it streams
    body = json.dumps({"inputs": INPUTS}).encode()
    response = client.post("/ogc/processes/ngiab-simulation/execution", content=iter([body[:10], body[10:]]),
                           headers={**bearer(), "Prefer": "respond-async", "Content-Type": "application/json"})
    assert response.status_code == 201 and len(runtime.submitted) == 1


def test_a_chunked_body_over_max_body_is_refused(client, runtime):
    chunks = (b"x" * 65536 for _ in range(ogc.MAX_BODY // 65536 + 1))
    response = client.post("/ogc/processes/ngiab-simulation/execution", content=chunks, headers=bearer())
    assert response.status_code == 413 and runtime.submitted == []


def test_a_lying_content_length_is_not_trusted(client, runtime):
    chunks = (b"x" * 65536 for _ in range(ogc.MAX_BODY // 65536 + 1))      # declares 10 bytes, sends more
    response = client.post("/ogc/processes/ngiab-simulation/execution", content=chunks,
                           headers={**bearer(), "Content-Length": "10"})
    assert response.status_code == 413 and runtime.submitted == []


@pytest.mark.parametrize("method, path, headers", [
    ("GET", "/ogc/processes", {}),                                   # anonymous, on an open route
    ("GET", "/ogc/jobs", None),                                      # with a token: bearer() at run time
    ("DELETE", "/ogc/jobs/job-done", None),
], ids=["anonymous-get", "token-get", "token-delete"])
def test_only_a_post_body_is_read(client, method, path, headers):
    """A spy `receive`: the app never asks for the body of a request that takes none."""
    asked = []
    headers = bearer() if headers is None else headers

    async def receive():
        asked.append(True)
        return {"type": "http.request", "body": b"x" * (ogc.MAX_BODY + 1), "more_body": False}

    assert asgi(client.app, method, path, headers, receive) in (200, 204) and asked == []


def test_authentication_comes_before_the_format_check(client):
    assert client.get("/ogc/jobs", params={"f": "no-such-format"}).status_code == 401


def test_a_bad_token_gets_the_invalid_token_challenge(client):
    response = client.get("/ogc/jobs", headers={"Authorization": "Bearer abc"})
    assert response.headers["WWW-Authenticate"] == 'Bearer error="invalid_token"'


def test_the_openapi_document_names_the_running_url(monkeypatch):
    monkeypatch.setenv("PYGEOAPI_CONFIG", str(CONFIG))
    monkeypatch.setenv("PYGEOAPI_SERVER_URL", "https://hydrowf.example/ogc")
    monkeypatch.setenv("HYDROWF_REGISTRY", str(REPO / "registry" / "legos.yaml"))
    monkeypatch.delenv("HYDROWF_OIDC_ISSUER", raising=False)
    app = ogc.app_from_env()
    assert app.state.api.openapi["servers"][0]["url"] == "https://hydrowf.example/ogc"
    assert TestClient(app).get("/ogc/processes").status_code == 200
