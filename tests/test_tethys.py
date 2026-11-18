"""hydrowf_platform.tethys with Django configured the way Tethys does it, without Tethys.

Tethys copies hydrowf_platform.tethys.settings' names into its settings (ADDITIONAL_SETTINGS_FILES) and
skips, with only a log line, an ADDITIONAL_URLPATTERNS module it cannot import: /ogc would answer 404.
Importing the module here makes a broken one fail the build instead.
"""
import importlib
import io
import json
import os
import time
from pathlib import Path
from types import SimpleNamespace

import django
import jwt
import pytest
import yaml
from cryptography.hazmat.primitives.asymmetric import rsa
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.http import HttpResponse
from django.test import Client, RequestFactory
from django.urls import resolve
from jwt.algorithms import RSAAlgorithm
from jwt.exceptions import PyJWKClientConnectionError, PyJWKClientError, PyJWKSetError
from pygeoapi.openapi import generate_openapi_document

REPO = Path(__file__).parents[1]
CONFIG = REPO / "pygeoapi-config.yml"

ISSUER = "https://keycloak.test/realms/hydrowf"
AUDIENCE = "hydrowf-api"
KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)       # the issuer's signing key
OTHER_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture(scope="module")
def urls(tmp_path_factory):
    """hydrowf_platform.tethys.urls, imported after Django is configured (once: Django can't be reconfigured)."""
    openapi = tmp_path_factory.mktemp("openapi") / "pygeoapi-openapi.yml"
    with pytest.MonkeyPatch.context() as env:
        env.setenv("PYGEOAPI_CONFIG", str(CONFIG))
        env.setenv("PYGEOAPI_OPENAPI", str(openapi))
        env.setenv("HYDROWF_REGISTRY", str(REPO / "registry" / "legos.yaml"))
        env.delenv("HYDROWF_OIDC_ISSUER", raising=False)             # bearer tokens off unless a test sets them
        env.delenv("HYDROWF_OIDC_AUDIENCE", raising=False)
        openapi.write_text(generate_openapi_document(CONFIG, "yaml"))

        tethys_settings = importlib.import_module("hydrowf_platform.tethys.settings")
        settings.configure(
            ROOT_URLCONF="hydrowf_platform.tethys.urls",
            ALLOWED_HOSTS=["testserver"],
            **{name: getattr(tethys_settings, name) for name in tethys_settings.__all__},
        )
        django.setup()
        yield importlib.import_module("hydrowf_platform.tethys.urls")


# ---- routes: /ogc/... resolves to pygeoapi's own views, each behind guard()


@pytest.mark.parametrize("url, view", [
    ("/ogc", "landing_page"),
    ("/ogc/processes", "processes"),
    ("/ogc/processes/ngiab-simulation", "processes"),
    ("/ogc/processes/ngiab-simulation/execution", "process_execution"),
    ("/ogc/jobs", "jobs"),
    ("/ogc/jobs/job-1", "jobs"),
    ("/ogc/jobs/job-1/results", "job_results"),
])
def test_ogc_routes_are_pygeoapi_views(urls, url, view):
    from pygeoapi.django_ import views
    assert resolve(url).func.__wrapped__ is getattr(views, view)       # functools.wraps keeps the original


def test_process_list_through_django(urls):
    response = Client().get("/ogc/processes", {"f": "json"})
    assert response.status_code == 200
    assert [p["id"] for p in json.loads(response.content)["processes"]] == ["ngiab-simulation", "ngiab-calibration"]


# ---- bearer tokens: an RS256 JWT from ISSUER for AUDIENCE, signed with KEY


def token(key=KEY, **claims):
    """A token minted now (inside the test, so a slow suite can't expire it)."""
    now = int(time.time())
    body = {"iss": ISSUER, "aud": AUDIENCE, "sub": "user-1", "iat": now, "exp": now + 300, **claims}
    return jwt.encode({k: v for k, v in body.items() if v is not None}, key, algorithm="RS256",
                      headers={"kid": "test"})


SIGNING_KEY = jwt.PyJWK({**RSAAlgorithm.to_jwk(KEY.public_key(), as_dict=True), "alg": "RS256", "kid": "test"})


class StubKeys:
    """The issuer's JWKS as PyJWKClient serves it: the signing key, or the error a broken issuer gives."""

    def __init__(self, error=None):
        self.error = error

    def get_signing_key_from_jwt(self, token):
        if self.error:
            raise self.error
        return SIGNING_KEY


@pytest.fixture
def bearer(urls, monkeypatch):
    """guard() configured for ISSUER and AUDIENCE, with the stub keys (no Keycloak, no network)."""
    from hydrowf_platform.tethys import auth
    bearer = auth.BearerAuth(ISSUER, AUDIENCE, keys=StubKeys())
    monkeypatch.setattr(auth, "bearer_auth", lambda: bearer)
    return bearer


@pytest.mark.parametrize("claims", [
    {},
    {"aud": ["account", AUDIENCE]},                       # Keycloak's aud: a list with ours in it
    {"iat": int(time.time()) + 20},                       # the issuer's clock 20 s ahead: within LEEWAY
], ids=["plain", "keycloak-aud-list", "clock-skew"])
def test_valid_tokens_give_their_claims(bearer, claims):
    assert bearer.claims(token(**claims))["sub"] == "user-1"


BAD_TOKENS = {
    "expired": lambda: token(exp=int(time.time()) - 60),
    "audience": lambda: token(aud="account"),             # Keycloak's default audience, not ours
    "issuer": lambda: token(iss="https://elsewhere.test/realms/hydrowf"),
    "key": lambda: token(key=OTHER_KEY),                  # not signed by the issuer
    "no-sub": lambda: token(sub=None),                    # whose job would it be?
    "future": lambda: token(iat=int(time.time()) + 600, nbf=int(time.time()) + 600),
    "hs256": lambda: jwt.encode({"iss": ISSUER, "aud": AUDIENCE, "sub": "u", "exp": int(time.time()) + 300},
                                "secret", algorithm="HS256"),
    "none": lambda: jwt.encode({"iss": ISSUER, "aud": AUDIENCE, "sub": "u", "exp": int(time.time()) + 300},
                               None, algorithm="none"),
    "garbage": lambda: "not-a-jwt",
}


@pytest.mark.parametrize("bad", BAD_TOKENS)
def test_tokens_not_for_us_have_no_claims(bearer, bad):
    assert bearer.claims(BAD_TOKENS[bad]()) is None


def test_a_token_whose_key_the_issuer_lacks_has_no_claims(urls):
    from hydrowf_platform.tethys import auth
    unknown_kid = auth.BearerAuth(ISSUER, AUDIENCE, keys=StubKeys(PyJWKClientError("no matching kid")))
    assert unknown_kid.claims(token()) is None


@pytest.mark.parametrize("error", [PyJWKClientConnectionError("down"), PyJWKSetError("no keys")],
                         ids=["unreachable", "broken-key-set"])
def test_a_failing_issuer_is_an_error_not_a_bad_token(urls, error):
    from hydrowf_platform.tethys import auth
    from hydrowf_platform.tethys.urls import guard
    failing = auth.BearerAuth(ISSUER, AUDIENCE, keys=StubKeys(error))
    with pytest.raises(type(error)):
        failing.claims(token())
    with pytest.MonkeyPatch.context() as patch, pytest.raises(type(error)):   # through guard: a 500
        patch.setattr(auth, "bearer_auth", lambda: failing)
        guard(ok, ALL)(request_with("POST", "bearer"))


def discovery(document):
    return lambda url, timeout: io.BytesIO(json.dumps(document).encode())


def test_discovery_gives_the_jwks_uri(urls, monkeypatch):
    from hydrowf_platform.tethys import auth
    monkeypatch.setattr(auth, "urlopen", discovery({"issuer": ISSUER, "jwks_uri": f"{ISSUER}/certs"}))
    assert auth.discover_jwks_uri(ISSUER) == f"{ISSUER}/certs"


def test_discovery_must_name_our_issuer(urls, monkeypatch):
    from hydrowf_platform.tethys import auth
    monkeypatch.setattr(auth, "urlopen", discovery({"issuer": "https://evil.test", "jwks_uri": "x"}))
    with pytest.raises(ImproperlyConfigured):
        auth.discover_jwks_uri(ISSUER)


def test_bearer_tokens_are_off_without_an_issuer(urls):
    from hydrowf_platform.tethys import auth
    auth.bearer_auth.cache_clear()
    assert settings.OIDC_ISSUER is None and auth.bearer_auth() is None
    assert urls.guard(ok)(request_with("GET", "bearer")).status_code == 401


def test_an_issuer_without_an_audience_fails_at_startup(urls, monkeypatch):
    monkeypatch.setenv("HYDROWF_OIDC_ISSUER", ISSUER)
    module = importlib.import_module("hydrowf_platform.tethys.settings")
    with pytest.raises(ImproperlyConfigured):
        importlib.reload(module)
    monkeypatch.delenv("HYDROWF_OIDC_ISSUER")
    importlib.reload(module)


def test_a_half_configured_downloads_section_fails_at_startup(urls, monkeypatch, tmp_path):
    config = yaml.safe_load(Path(os.environ["PYGEOAPI_CONFIG"]).read_text())
    config["server"]["manager"]["connection"]["downloads"] = {"endpoint": "http://localhost:9000"}
    broken = tmp_path / "pygeoapi-config.yml"
    broken.write_text(yaml.safe_dump(config))
    monkeypatch.setenv("PYGEOAPI_CONFIG", str(broken))
    monkeypatch.delenv("HYDROWF_S3_ACCESS_KEY", raising=False)       # no keys: half configured
    module = importlib.import_module("hydrowf_platform.tethys.settings")
    with pytest.raises(ImproperlyConfigured, match="downloads"):
        importlib.reload(module)
    monkeypatch.undo()
    importlib.reload(module)


def test_a_shared_input_that_is_not_a_prefix_fails_at_startup(urls, monkeypatch, tmp_path):
    config = yaml.safe_load(Path(os.environ["PYGEOAPI_CONFIG"]).read_text())
    config["server"]["manager"]["connection"]["shared_inputs"] = ["hydrofabric"]   # no slash: hydrofabric-x/ too
    broken = tmp_path / "pygeoapi-config.yml"
    broken.write_text(yaml.safe_dump(config))
    monkeypatch.setenv("PYGEOAPI_CONFIG", str(broken))
    module = importlib.import_module("hydrowf_platform.tethys.settings")
    with pytest.raises(ImproperlyConfigured, match="shared_inputs"):
        importlib.reload(module)
    monkeypatch.undo()
    importlib.reload(module)


# ---- guard(): allowed methods, bearer token, opt-in open reads; no sessions


def ok(request, *args, **kwargs):
    return HttpResponse("ok")


ALL = ("GET", "HEAD", "OPTIONS", "POST", "DELETE", "PATCH")

CREDENTIALS = {          # credential -> (request headers, made when the test runs; Tethys session logged in?)
    None: (dict, False),
    "session": (dict, True),                                         # a logged-in Tethys user, no token
    "bearer": (lambda: {"HTTP_AUTHORIZATION": f"Bearer {token()}"}, False),
    "bearer+session": (lambda: {"HTTP_AUTHORIZATION": f"Bearer {token()}"}, True),
    "bearer-expired": (lambda: {"HTTP_AUTHORIZATION": f"Bearer {BAD_TOKENS['expired']()}"}, False),
    "bearer-expired+session": (lambda: {"HTTP_AUTHORIZATION": f"Bearer {BAD_TOKENS['expired']()}"}, True),
    "bearer-lowercase": (lambda: {"HTTP_AUTHORIZATION": f"bearer  {token()} "}, False),   # RFC 6750: any case
    "bearer-tab": (lambda: {"HTTP_AUTHORIZATION": f" Bearer\t{token()}"}, False),         # any whitespace
    "bearer-empty+session": (lambda: {"HTTP_AUTHORIZATION": "Bearer "}, True),
    "bearer-bad-sub": (lambda: {"HTTP_AUTHORIZATION": f"Bearer {token(sub='a,b')}"}, False),
    "basic": (lambda: {"HTTP_AUTHORIZATION": "Basic dXNlcjpwYXNz"}, False),
    "basic+session": (lambda: {"HTTP_AUTHORIZATION": "Basic dXNlcjpwYXNz"}, True),
}


def request_with(method, credential, **extra):
    make_headers, logged_in = CREDENTIALS[credential]
    request = RequestFactory().generic(method, "/ogc/jobs/job-1", **{**make_headers(), **extra})
    request.user = SimpleNamespace(is_authenticated=logged_in)
    return request


@pytest.mark.parametrize("method, credential, status", [       # a route without open_reads (the default)
    ("GET", None, 401),                         # reads need a caller too: job results are download links
    ("OPTIONS", None, 401),
    ("HEAD", None, 401),
    ("POST", None, 401),
    ("DELETE", None, 401),
    ("PATCH", None, 401),                       # deny by default
    ("GET", "session", 401),                    # a session has no Keycloak identity: no jobs
    ("POST", "session", 401),
    ("DELETE", "session", 401),
    ("POST", "bearer", 200),
    ("DELETE", "bearer", 200),
    ("GET", "bearer", 200),
    ("POST", "bearer+session", 200),            # the token decides; no CSRF, since no cookie is read
    ("POST", "bearer-expired", 401),
    ("GET", "bearer-expired", 401),             # a bad token is refused, not read as anonymous
    ("POST", "bearer-expired+session", 401),    # nor read as the session
    ("POST", "bearer-lowercase", 200),
    ("POST", "bearer-tab", 200),
    ("GET", "bearer-empty+session", 401),       # a malformed credential is refused too
    ("GET", "bearer-bad-sub", 401),             # a sub that can't be an owner label owns nothing
    ("GET", "basic", 401),                      # not ours
    ("GET", "basic+session", 401),
])
def test_guard(bearer, method, credential, status):
    response = urls_guard(ALL)(request_with(method, credential), job_id="job-1")
    assert response.status_code == status
    if status == 401:
        assert "error" in json.loads(response.content)


def urls_guard(methods, view=ok, **options):
    from hydrowf_platform.tethys.urls import guard
    return guard(view, methods, **options)


@pytest.mark.parametrize("method, credential, status", [
    ("GET", None, 200),
    ("HEAD", None, 200),
    ("OPTIONS", None, 200),
    ("POST", None, 401),                        # open reads open nothing else
    ("DELETE", None, 401),
    ("GET", "bearer-expired", 401),             # and a bad token is still refused
])
def test_open_reads_open_only_the_reads(bearer, method, credential, status):
    response = urls_guard(ALL, open_reads=True)(request_with(method, credential), job_id="job-1")
    assert response.status_code == status


@pytest.mark.parametrize("url", ["/ogc/jobs", "/ogc/jobs/job-1", "/ogc/jobs/job-1/results"])
@pytest.mark.parametrize("logged_in", [False, True], ids=["anonymous", "session"])
def test_jobs_need_a_token(urls, url, logged_in):
    request = RequestFactory().get(url)
    request.user = SimpleNamespace(is_authenticated=logged_in)
    match = resolve(url)
    assert match.func(request, **match.kwargs).status_code == 401


def test_the_view_runs_as_the_tokens_owner(bearer):
    from hydrowf_platform import caller
    seen = []

    def view(request, **kwargs):
        seen.append(caller.owner())
        return HttpResponse("ok")

    urls_guard(ALL, view)(request_with("GET", "bearer"))
    assert seen == ["user-1"]
    with pytest.raises(caller.NoCaller):                    # and only for the length of that request
        caller.owner()


@pytest.mark.parametrize("url, method", [
    ("/ogc/processes/ngiab-simulation/execution", "GET"),     # pygeoapi would execute the body
    ("/ogc/processes/ngiab-simulation/execution", "OPTIONS"),
    ("/ogc/processes/ngiab-simulation/execution", "HEAD"),
    ("/ogc/jobs", "DELETE"),
    ("/ogc/processes", "POST"),
])
def test_routes_refuse_methods_they_do_not_take(urls, url, method):
    request = RequestFactory().generic(method, url, data=json.dumps({"inputs": {"feature": "x"}}),
                                       content_type="application/json")
    request.user = SimpleNamespace(is_authenticated=False)
    match = resolve(url)
    assert match.func(request, **match.kwargs).status_code == 405


def test_guard_hands_the_claims_to_the_view(bearer):
    seen = {}

    def view(request, **kwargs):
        seen.update(request.oidc_claims)
        return HttpResponse("ok")

    urls_guard(ALL, view)(request_with("POST", "bearer"))
    assert seen["sub"] == "user-1"


def test_every_route_is_csrf_exempt(urls):            # no route reads the session cookie
    assert all(pattern.callback.csrf_exempt for pattern in urls.urlpatterns)
