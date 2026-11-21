"""The two Kubernetes API calls the step-key vendor makes: a TokenReview and reading one pod.

In the cluster it uses the pod's own service account (the token and CA Kubernetes mounts, and the API
server's address from the environment). Outside it (the service in Docker beside kind), the same three
settings come from:

    HYDROWF_K8S_API         https://127.0.0.1:<port>          the API server
    HYDROWF_K8S_TOKEN_FILE  a file with a token of the hydrowf-ogc service account
    HYDROWF_K8S_CA_FILE     the cluster's CA certificate

The service account needs: create tokenreviews (cluster-wide) and get pods in the workflows' namespace
(hydrowf-deploy). Answers are capped at MAX_BYTES and calls at TIMEOUT seconds.
"""
from __future__ import annotations

import http.client
import json
import os
import ssl
import urllib.error
import urllib.request
from pathlib import Path
from urllib.parse import quote

IN_CLUSTER = Path("/var/run/secrets/kubernetes.io/serviceaccount")
TIMEOUT = 10
MAX_BYTES = 1024 * 1024


class KubeError(Exception):
    """The API server can't be reached, refuses, or answers something that isn't JSON. `transient`: worth
    retrying (unreachable, a timeout, a 5xx) rather than a refusal (401, 403, 404)."""

    def __init__(self, message: str, transient: bool):
        super().__init__(message)
        self.transient = transient


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):        # the bearer token never follows a redirect
        return None


class Kube:
    def __init__(self, api: str, token_file: str | Path, ca_file: str | Path | None):
        self.api = api.rstrip("/")
        self.token_file = Path(token_file)                 # read per call: Kubernetes rotates mounted tokens
        if not self.token_file.is_file() or (ca_file and not Path(ca_file).is_file()):
            raise ValueError(f"step keys: the Kubernetes token file {self.token_file} or CA {ca_file} is missing")
        context = ssl.create_default_context(cafile=str(ca_file) if ca_file else None)
        # no proxy (the API server is reached directly) and no redirects (the token stays with `api`)
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect(),
                                                  urllib.request.HTTPSHandler(context=context))

    def _call(self, method: str, path: str, body: dict | None = None) -> dict:
        try:
            request = urllib.request.Request(
                self.api + path, method=method, data=json.dumps(body).encode() if body is not None else None,
                headers={"Authorization": f"Bearer {self.token_file.read_text().strip()}",
                         "Content-Type": "application/json", "Accept": "application/json"})
            with self.opener.open(request, timeout=TIMEOUT) as response:
                raw = response.read(MAX_BYTES + 1)
        except urllib.error.HTTPError as err:              # a redirect lands here too (NoRedirect): refused
            err.close()
            raise KubeError(f"{method} {path}: {err.code}", transient=err.code >= 500 or err.code == 429) from None
        except (urllib.error.URLError, http.client.HTTPException, OSError) as err:
            raise KubeError(f"{method} {path}: {type(err).__name__}", transient=True) from None
        if len(raw) > MAX_BYTES:
            raise KubeError(f"{method} {path}: the answer is over {MAX_BYTES} bytes", transient=False)
        try:
            return json.loads(raw)
        except ValueError:
            raise KubeError(f"{method} {path}: not JSON", transient=False) from None

    def token_review(self, token: str, audiences: list[str]) -> dict:
        """The TokenReview's status for `token`, asked for `audiences` only."""
        body = {"apiVersion": "authentication.k8s.io/v1", "kind": "TokenReview",
                "spec": {"token": token, "audiences": audiences}}
        return self._call("POST", "/apis/authentication.k8s.io/v1/tokenreviews", body).get("status") or {}

    def pod(self, namespace: str, name: str) -> dict:
        return self._call("GET", f"/api/v1/namespaces/{quote(namespace, safe='')}/pods/{quote(name, safe='')}")


def kube_from_env() -> Kube:
    """The client for this service: HYDROWF_K8S_* when set, else the pod's own service account. ValueError at
    startup when they don't add up."""
    api = os.environ.get("HYDROWF_K8S_API")
    if api:
        token = os.environ.get("HYDROWF_K8S_TOKEN_FILE")
        if not token:
            raise ValueError("step keys: HYDROWF_K8S_API needs HYDROWF_K8S_TOKEN_FILE")
        return Kube(api, token, os.environ.get("HYDROWF_K8S_CA_FILE"))
    host, port = os.environ.get("KUBERNETES_SERVICE_HOST"), os.environ.get("KUBERNETES_SERVICE_PORT", "443")
    if not host:
        raise ValueError("step keys: not in a cluster and no HYDROWF_K8S_API")
    host = f"[{host}]" if ":" in host else host          # an IPv6 address
    return Kube(f"https://{host}:{port}", IN_CLUSTER / "token", IN_CLUSTER / "ca.crt")
