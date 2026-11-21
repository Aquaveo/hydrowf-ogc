"""The step-key vendor: read keys for a running step that reads the bucket in place (hydrowf-core Part D.4).

A step whose dataset ports read our bucket (render's Reader) starts with an init container that POSTs its
pod's projected service-account token (audience `hydrowf-vendor`) here and writes the answer, an AWS
credentials file, for the tool. The token says which pod is asking; what the pod may read comes only from
what render recorded for its step (the workflow's hydrowf/reads), never from the request:

1. TokenReview, asked for the vendor's audience only: the pod's default service-account token, which the
   tool can see, doesn't pass. The token is a service account's in the workflows' namespace.
2. The review names the pod (name and uid, bound to the token); the pod must exist with that uid, and its
   controlling owner must be the Workflow its label names, by that workflow's uid.
3. The workflow is one hydrowf-ogc made (its owner and pipeline labels) and hasn't finished, and the pod is
   the very pod Argo made for the step (the name Argo derives for the step's node, which is Pending or
   Running). A pod made to look like a step can't take that name while the real one exists, and the real
   one's node is no longer Pending or Running once it is gone.
4. The keys are that step's hydrowf/reads, already checked as plain keys (hydrowf_core RunStatus.reads),
   issued for an hour (credentials.Vendor; the most role chaining allows on EKS).

Every refusal is the same 403, the reason only in the log: a caller learns nothing from which check failed.
It is served on its own port (hydrowf_ogc.serve), which no ingress routes.
"""
from __future__ import annotations

import logging
import re

from hera.exceptions import NotFound
from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import PlainTextResponse, Response
from starlette.routing import Route

from hydrowf_core.adapters.argo_runtime import RunStatus
from hydrowf_ogc import caller
from hydrowf_ogc.credentials import IssuerUnavailable, Vendor
from hydrowf_ogc.kube import Kube, KubeError

LOGGER = logging.getLogger(__name__)
AUDIENCE = "hydrowf-vendor"
STEP_SECONDS = 3600
MAX_TOKEN = 16 * 1024                       # bytes: a projected token is about 1 KB
POD_NAME = "authentication.kubernetes.io/pod-name"
POD_UID = "authentication.kubernetes.io/pod-uid"
WORKFLOW_LABEL = "workflows.argoproj.io/workflow"
NODE_NAME = "workflows.argoproj.io/node-name"
NO_STORE = {"Cache-Control": "no-store"}
JWT = re.compile(r"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+")
LIVE = ("Pending", "Running")             # a step's node while its pod (and its init container) runs


class Unavailable(Exception):
    """A lookup failed for a reason worth retrying (the API server or Argo unreachable): a 503."""


class Refused(Exception):
    """No keys for this request; the message is for the log only."""


def one(extra: dict, name: str) -> str:
    values = extra.get(name) or []
    if len(values) != 1 or not isinstance(values[0], str) or not values[0]:
        raise Refused(f"the review names no single {name}")
    return values[0]


class StepKeys:
    def __init__(self, kube: Kube, runtime, vendor: Vendor, bucket: str, namespace: str,
                 audience: str = AUDIENCE, seconds: int = STEP_SECONDS):
        self.kube, self.runtime, self.vendor = kube, runtime, vendor
        self.bucket, self.namespace, self.audience, self.seconds = bucket, namespace, audience, seconds

    def step_of(self, token: str) -> tuple[RunStatus, str]:
        """(the workflow's run, step) of the pod whose token this is, after checks 1 to 3; Refused otherwise,
        Unavailable when a lookup failed for a reason worth retrying."""
        if not JWT.fullmatch(token):
            raise Refused("the body is not a token")
        review = self.kube.token_review(token, [self.audience])
        if review.get("authenticated") is not True or self.audience not in (review.get("audiences") or []):
            raise Refused(f"the token isn't valid for {self.audience}")
        user = review.get("user") or {}
        if not str(user.get("username", "")).startswith(f"system:serviceaccount:{self.namespace}:"):
            raise Refused(f"not a service account of {self.namespace}: {user.get('username')!r}")
        extra = user.get("extra") or {}
        name, uid = one(extra, POD_NAME), one(extra, POD_UID)
        meta = self.kube.pod(self.namespace, name).get("metadata") or {}
        if meta.get("uid") != uid:
            raise Refused(f"pod {name} isn't the one the token was made for")
        workflow = (meta.get("labels") or {}).get(WORKFLOW_LABEL)
        owners = [o for o in meta.get("ownerReferences") or [] if o.get("kind") == "Workflow"]
        if not workflow or [o.get("name") for o in owners] != [workflow] or owners[0].get("controller") is not True:
            raise Refused(f"pod {name} isn't controlled by one workflow")
        node = (meta.get("annotations") or {}).get(NODE_NAME, "")
        # one step of it. Argo names a retry's or loop's pod step(0): it passes here, and gets nothing, since
        # render records no reads for "step(0)"
        if not node.startswith(f"{workflow}.") or "." in node[len(workflow) + 1:]:
            raise Refused(f"pod {name} isn't a step of {workflow} ({node!r})")
        step = node[len(workflow) + 1:]
        try:
            run = self.runtime.status(workflow)
        except NotFound:
            raise Refused(f"no workflow {workflow}") from None
        except Exception as err:                            # Argo unreachable (or a bug): worth retrying, logged
            LOGGER.warning("step keys: reading workflow %s failed", workflow, exc_info=True)
            raise Unavailable(f"workflow {workflow}: {type(err).__name__}") from None
        if not run.uid or owners[0].get("uid") != run.uid:
            raise Refused(f"pod {name}'s owner isn't workflow {workflow} itself")
        pod_name, phase = run.step_pods.get(step, ("", ""))
        if pod_name != name or phase not in LIVE:
            raise Refused(f"pod {name} isn't the running pod Argo made for step {step} ({pod_name!r}, {phase!r})")
        return run, step

    def keys_for(self, token: str) -> str:
        """An AWS credentials file with read keys for the asking step; Refused or IssuerUnavailable."""
        run, step = self.step_of(token)
        workflow = run.name
        if not run.labels.get(caller.OWNER_LABEL) or not run.labels.get("hydrowf/pipeline"):
            raise Refused(f"{workflow} isn't a hydrowf-ogc job")
        if run.finished:
            raise Refused(f"{workflow} has finished")
        keys = run.reads.get(step)
        if not keys:
            raise Refused(f"step {step} of {workflow} reads nothing in place")
        try:
            issued = self.vendor.issue(self.bucket, keys, session=f"step-{workflow}-{step}", seconds=self.seconds)
        except ValueError as err:
            raise Refused(str(err)) from err
        LOGGER.info("read keys for %s step %s: %d keys, %d s", workflow, step, len(keys), self.seconds)
        return ("[default]\n"
                f"aws_access_key_id = {issued['access_key_id']}\n"
                f"aws_secret_access_key = {issued['secret_access_key']}\n"
                f"aws_session_token = {issued['session_token']}\n")


def step_keys_app(step_keys: StepKeys) -> Starlette:
    """POST /step-keys, the body a projected token: the credentials file, or 403 / 503."""
    async def handle(request: Request) -> Response:
        body = b""
        async for chunk in request.stream():                # counted as it streams, as the public app does
            body += chunk
            if len(body) > MAX_TOKEN:
                return PlainTextResponse("refused\n", status_code=403, headers=NO_STORE)
        token = body.decode("ascii", "replace").strip()
        try:
            text = await run_in_threadpool(step_keys.keys_for, token)
        except KubeError as err:
            LOGGER.warning("step keys: %s", err)
            if err.transient:                             # the init container's curl retries a 503
                return PlainTextResponse("try again\n", status_code=503, headers=NO_STORE)
            return PlainTextResponse("refused\n", status_code=403, headers=NO_STORE)
        except Refused as err:
            LOGGER.warning("step keys refused: %s", err)
            return PlainTextResponse("refused\n", status_code=403, headers=NO_STORE)
        except (IssuerUnavailable, Unavailable) as err:
            LOGGER.warning("step keys: %s", err)
            return PlainTextResponse("try again\n", status_code=503, headers=NO_STORE)
        except Exception:                                   # a lookup that broke: never a traceback out
            LOGGER.exception("step keys")
            return PlainTextResponse("refused\n", status_code=403, headers=NO_STORE)
        return PlainTextResponse(text, headers=NO_STORE)

    return Starlette(routes=[Route("/step-keys", handle, methods=["POST"])])
