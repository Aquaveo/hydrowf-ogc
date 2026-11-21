"""The step-key vendor's Kubernetes client (hydrowf_ogc.kube) and the two-port launcher (hydrowf_ogc.serve)."""
import io
import json
import urllib.error

import pytest
from test_stepkeys import StubKube

from hydrowf_ogc import kube as kube_module
from hydrowf_ogc import serve
from hydrowf_ogc.kube import MAX_BYTES, Kube, KubeError, kube_from_env


class FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeOpener:
    """The opener Kube uses: an answer (bytes) or an exception, and the requests it was asked."""

    def __init__(self, answer=b"{}"):
        self.answer, self.requests = answer, []

    def open(self, request, timeout):
        self.requests.append((request, timeout))
        if isinstance(self.answer, Exception):
            raise self.answer
        return FakeResponse(self.answer)


@pytest.fixture
def token_file(tmp_path):
    path = tmp_path / "token"
    path.write_text("sa-token\n")
    return path


def kube_with(token_file, answer=b"{}"):
    k = Kube("https://10.0.0.1:443/", token_file, None)
    k.opener = FakeOpener(answer)
    return k


def test_a_token_review_asks_for_the_given_audiences_with_the_service_accounts_token(token_file):
    k = kube_with(token_file, json.dumps({"status": {"authenticated": True}}).encode())
    assert k.token_review("pod-token", ["hydrowf-vendor"]) == {"authenticated": True}
    request, timeout = k.opener.requests[0]
    assert request.full_url == "https://10.0.0.1:443/apis/authentication.k8s.io/v1/tokenreviews"
    assert request.get_header("Authorization") == "Bearer sa-token" and timeout == kube_module.TIMEOUT
    assert json.loads(request.data)["spec"] == {"token": "pod-token", "audiences": ["hydrowf-vendor"]}


def test_a_pod_name_is_quoted_into_its_path(token_file):
    k = kube_with(token_file)
    k.pod("argo", "a/../b")
    assert k.opener.requests[0][0].full_url == "https://10.0.0.1:443/api/v1/namespaces/argo/pods/a%2F..%2Fb"


@pytest.mark.parametrize("answer, transient", [
    (urllib.error.HTTPError("u", 503, "x", {}, None), True),
    (urllib.error.HTTPError("u", 429, "x", {}, None), True),
    (urllib.error.HTTPError("u", 403, "x", {}, None), False),
    (urllib.error.HTTPError("u", 404, "x", {}, None), False),
    (urllib.error.URLError("refused"), True),
    (TimeoutError(), True),
    (b"not json", False),
    (b" " * (MAX_BYTES + 1), False),
], ids=["5xx", "429", "403", "404", "unreachable", "timeout", "not-json", "too-big"])
def test_every_failure_is_a_kube_error_that_says_whether_to_retry(token_file, answer, transient):
    with pytest.raises(KubeError) as err:
        kube_with(token_file, answer).pod("argo", "p")
    assert err.value.transient is transient and "sa-token" not in str(err.value)


def test_a_token_file_gone_after_startup_is_a_kube_error(token_file):
    k = kube_with(token_file)
    token_file.unlink()
    with pytest.raises(KubeError):
        k.pod("argo", "p")


def test_the_opener_follows_no_redirects_and_uses_no_proxy(token_file, monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.example.org:3128")
    handlers = {type(h).__name__ for h in Kube("https://10.0.0.1", token_file, None).opener.handlers}
    assert "NoRedirect" in handlers and "HTTPRedirectHandler" not in handlers
    proxies = [h.proxies for h in Kube("https://10.0.0.1", token_file, None).opener.handlers
               if type(h).__name__ == "ProxyHandler"]
    assert all(p == {} for p in proxies)            # none at all, or an empty one: HTTPS_PROXY is never used


def test_missing_files_fail_at_startup(tmp_path, token_file):
    with pytest.raises(ValueError, match="missing"):
        Kube("https://x", tmp_path / "nope", None)
    with pytest.raises(ValueError, match="missing"):
        Kube("https://x", token_file, tmp_path / "no-ca.crt")


def test_from_env_outside_the_cluster(monkeypatch, token_file):
    monkeypatch.setenv("HYDROWF_K8S_API", "https://127.0.0.1:6443")
    monkeypatch.setenv("HYDROWF_K8S_TOKEN_FILE", str(token_file))
    monkeypatch.delenv("HYDROWF_K8S_CA_FILE", raising=False)
    assert kube_from_env().api == "https://127.0.0.1:6443"
    monkeypatch.delenv("HYDROWF_K8S_TOKEN_FILE")
    with pytest.raises(ValueError, match="needs HYDROWF_K8S_TOKEN_FILE"):
        kube_from_env()


@pytest.mark.parametrize("host, url", [("10.96.0.1", "https://10.96.0.1:443"), ("fd00::1", "https://[fd00::1]:443")])
def test_from_env_in_the_cluster(monkeypatch, tmp_path, host, url):
    (tmp_path / "token").write_text("t")
    (tmp_path / "ca.crt").write_text("")
    monkeypatch.delenv("HYDROWF_K8S_API", raising=False)
    monkeypatch.setenv("KUBERNETES_SERVICE_HOST", host)
    monkeypatch.setenv("KUBERNETES_SERVICE_PORT", "443")
    monkeypatch.setattr(kube_module, "IN_CLUSTER", tmp_path)
    monkeypatch.setattr(kube_module.ssl, "create_default_context", lambda cafile=None: kube_module.ssl.SSLContext())
    assert kube_from_env().api == url


def test_not_in_a_cluster_and_not_configured_fails_at_startup(monkeypatch):
    monkeypatch.delenv("HYDROWF_K8S_API", raising=False)
    monkeypatch.delenv("KUBERNETES_SERVICE_HOST", raising=False)
    with pytest.raises(ValueError, match="not in a cluster"):
        kube_from_env()


# ---- the launcher


class FakeApp:
    def __init__(self, reader):
        class Manager:
            pass
        manager = Manager()
        manager.reader, manager.runtime, manager.vendor = reader, None, None
        manager.bucket, manager.namespace = "b", "argo"

        class Api:
            pass

        class State:
            pass
        self.state = State()
        self.state.api = Api()
        self.state.api.manager = manager


def test_without_steps_only_the_public_server_runs():
    [public] = serve.servers(app=FakeApp(reader=None))
    assert public.config.port == 8000 and public.config.proxy_headers is True


def test_with_steps_the_vendor_runs_on_its_own_port_without_proxy_headers():
    public, vendor = serve.servers(app=FakeApp(reader=object()), kube=StubKube())
    assert (public.config.port, vendor.config.port) == (8000, 8001) and vendor.config.proxy_headers is False


def test_the_servers_leave_signals_to_the_launcher():
    [public] = serve.servers(app=FakeApp(reader=None))
    with public.capture_signals():                     # a no-op: main() stops both servers together
        pass


def test_the_api_server_is_verified_against_the_given_ca_only(token_file, tmp_path, monkeypatch):
    import ssl
    seen = {}
    real = ssl.create_default_context

    def spy(cafile=None):
        seen["cafile"] = cafile
        return real()                                # the system's, to build a context without a real CA file
    monkeypatch.setattr(kube_module.ssl, "create_default_context", spy)
    ca = tmp_path / "ca.crt"
    ca.write_text("")
    k = Kube("https://10.0.0.1", token_file, ca)
    https = next(h for h in k.opener.handlers if type(h).__name__ == "HTTPSHandler")
    assert seen["cafile"] == str(ca)
    assert https._context.check_hostname is True and https._context.verify_mode == ssl.CERT_REQUIRED


def test_a_redirect_is_a_refusal_and_the_token_does_not_follow(token_file):
    import http.server
    import threading

    class Redirect(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(302)
            self.send_header("Location", "http://127.0.0.1:1/elsewhere")
            self.end_headers()

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Redirect)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        k = Kube(f"http://127.0.0.1:{server.server_port}", token_file, None)
        with pytest.raises(KubeError) as err:
            k.pod("argo", "p")
        assert err.value.transient is False and "302" in str(err.value)
    finally:
        server.shutdown()
