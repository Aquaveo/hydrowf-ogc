"""The step-key vendor (hydrowf_ogc.stepkeys) and steps that read in place (manager: reader, dataset inputs)."""
import copy
import json

import pytest
from hera.exceptions import NotFound
from starlette.testclient import TestClient
from test_credentials import StubSTS
from test_manager import ME, MINE, FakeRuntime, manager

from hydrowf_core.adapters.argo_runtime import RunStatus
from hydrowf_ogc import caller
from hydrowf_ogc.credentials import Vendor
from hydrowf_ogc.manager import BadExecuteRequest, ForbiddenInput, reader_from
from hydrowf_ogc.kube import KubeError
from hydrowf_ogc.stepkeys import AUDIENCE, Refused, StepKeys, step_keys_app

WF, WF_UID = "job-1", "wf-uid-1"
POD, UID = "job-1-step-eval-123", "uid-1"
TOKEN = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ4In0.c2ln"                 # a JWT's shape
READS = ["job-0/job-0-step-to-cng-1/cng/", "managed/hf/conus.gpkg"]
LABELS = {**MINE, "hydrowf/pipeline": "ngiab-eval"}
RUNNING = RunStatus(WF, "Running", pipeline="ngiab-eval", labels=LABELS, reads={"eval": READS}, uid=WF_UID,
                    step_pods={"eval": (POD, "Pending")})             # Pending: its init container runs
REVIEW = {"authenticated": True, "audiences": [AUDIENCE],
          "user": {"username": "system:serviceaccount:argo:default",
                   "extra": {"authentication.kubernetes.io/pod-name": [POD],
                             "authentication.kubernetes.io/pod-uid": [UID]}}}
POD_META = {"metadata": {"name": POD, "uid": UID, "labels": {"workflows.argoproj.io/workflow": WF},
                         "annotations": {"workflows.argoproj.io/node-name": f"{WF}.eval"},
                         "ownerReferences": [{"kind": "Workflow", "name": WF, "uid": WF_UID, "controller": True}]}}


class StubKube:
    """The Kubernetes API: canned answers, and what was asked."""

    def __init__(self, review=REVIEW, pod=POD_META):
        self.review, self.pod_answer, self.asked = copy.deepcopy(review), copy.deepcopy(pod), []

    def token_review(self, token, audiences):
        self.asked.append(("review", token, audiences))
        return self.review

    def pod(self, namespace, name):
        self.asked.append(("pod", namespace, name))
        return self.pod_answer


def vendor_for(run=RUNNING, kube=None, sts=None):
    sts = sts or StubSTS()
    step_keys = StepKeys(kube or StubKube(), FakeRuntime([run]), Vendor(sts, "arn:aws:iam::0:role/reader"),
                         "my-bucket", "argo")
    return step_keys, sts


def test_a_running_step_gets_an_hour_of_keys_for_exactly_its_reads():
    step_keys, sts = vendor_for()
    text = step_keys.keys_for(TOKEN)
    assert text == ("[default]\naws_access_key_id = AK\naws_secret_access_key = SK\naws_session_token = ST\n")
    call = sts.calls[0]
    assert call["DurationSeconds"] == 3600 and call["RoleSessionName"] == "step-job-1-eval"
    statements = json.loads(call["Policy"])["Statement"]
    assert statements[0]["Resource"] == ["arn:aws:s3:::my-bucket/job-0/job-0-step-to-cng-1/cng/*",
                                         "arn:aws:s3:::my-bucket/managed/hf/conus.gpkg"]
    assert step_keys.kube.asked[0] == ("review", TOKEN, [AUDIENCE])                # asked for our audience only


def replace_run(**change):
    import dataclasses
    return dataclasses.replace(RUNNING, **change)


def review_with(**change):
    review = copy.deepcopy(REVIEW)
    review.update(change)
    return review


def pod_with(**change):
    pod = copy.deepcopy(POD_META)
    pod["metadata"].update(change)
    return pod


@pytest.mark.parametrize("kube", [
    StubKube(review=review_with(authenticated=False)),
    StubKube(review=review_with(audiences=["https://kubernetes.default.svc"])),    # the default SA token
    StubKube(review=review_with(user={**REVIEW["user"], "username": "system:serviceaccount:other:default"})),
    StubKube(review=review_with(user={**REVIEW["user"], "username": "alice"})),
    StubKube(review=review_with(user={**REVIEW["user"], "extra": {}})),             # not a pod's token
    StubKube(pod=pod_with(uid="another-uid")),                                     # a new pod with the old name
    StubKube(pod=pod_with(labels={})),
    StubKube(pod=pod_with(ownerReferences=[{"kind": "Workflow", "name": "job-2", "uid": WF_UID, "controller": True}])),
    StubKube(pod=pod_with(ownerReferences=[{"kind": "Workflow", "name": WF, "uid": WF_UID}])),          # not controller
    StubKube(pod=pod_with(ownerReferences=[{"kind": "Workflow", "name": WF, "uid": "x", "controller": True}])),
    StubKube(review=review_with(user={**REVIEW["user"], "extra": {
        "authentication.kubernetes.io/pod-name": ["job-1-step-eval-forged"],
        "authentication.kubernetes.io/pod-uid": [UID]}}), pod=pod_with(name="job-1-step-eval-forged")),   # a look-alike
    StubKube(pod=pod_with(ownerReferences=[])),
    StubKube(pod=pod_with(annotations={"workflows.argoproj.io/node-name": "job-2.eval"})),
    StubKube(pod=pod_with(annotations={"workflows.argoproj.io/node-name": "job-1.eval.inner"})),
], ids=["unauthenticated", "other-audience", "other-namespace", "a-user", "no-pod", "uid-differs", "no-workflow",
        "other-owner", "not-controller", "other-workflow-uid", "forged-pod", "no-owner", "other-workflows-step",
        "nested-node"])
def test_a_token_or_pod_that_doesnt_check_out_gets_nothing(kube):
    step_keys, sts = vendor_for(kube=kube)
    with pytest.raises(Refused):
        step_keys.keys_for(TOKEN)
    assert sts.calls == []


@pytest.mark.parametrize("run", [
    replace_run(labels={"hydrowf/pipeline": "x"}),                                      # no owner
    replace_run(labels=MINE),                                                            # no pipeline
    replace_run(phase="Succeeded"),
    replace_run(reads={"other": READS}),
    replace_run(reads={}),
    replace_run(step_pods={"eval": (POD, "Succeeded")}),           # its pod is gone: the name is free again
    replace_run(step_pods={}),                                     # Argo has no single pod for it (a retry)
    replace_run(uid=""),
], ids=["not-ours-owner", "not-ours-pipeline", "finished", "another-steps-reads", "no-reads", "step-done",
        "no-step-pod", "no-uid"])
def test_a_workflow_or_step_that_doesnt_check_out_gets_nothing(run):
    step_keys, sts = vendor_for(run=run)
    with pytest.raises(Refused):
        step_keys.keys_for(TOKEN)
    assert sts.calls == []


# ---- the vendor's HTTP face: the same 403 whatever failed


def test_the_app_answers_the_credentials_file_or_one_403():
    good = TestClient(step_keys_app(vendor_for()[0]))
    response = good.post("/step-keys", content=TOKEN.encode() + b"\n")
    assert response.status_code == 200 and response.headers["cache-control"] == "no-store"
    assert response.text.startswith("[default]\n")
    refused = TestClient(step_keys_app(vendor_for(kube=StubKube(pod=pod_with(uid="x")))[0]))
    other = TestClient(step_keys_app(vendor_for(run=replace_run(phase="Succeeded"))[0]))
    not_a_token = TestClient(step_keys_app(vendor_for()[0]))
    answers = {(r.status_code, r.text) for r in (refused.post("/step-keys", content=TOKEN.encode()),
                                                 other.post("/step-keys", content=TOKEN.encode()),
                                                 not_a_token.post("/step-keys", content=b""))}
    assert answers == {(403, "refused\n")}                               # nothing says which check failed


@pytest.mark.parametrize("body", [b"x" * 20000, b"", b"not a token", b"a.b"],
                         ids=["huge", "empty", "words", "two-parts"])
def test_what_isnt_a_token_never_reaches_the_api_server(body):
    step_keys = vendor_for()[0]
    response = TestClient(step_keys_app(step_keys)).post("/step-keys", content=body)
    assert response.status_code == 403 and step_keys.kube.asked == []


def test_an_sts_failure_is_a_503():
    from botocore.exceptions import EndpointConnectionError
    sts = StubSTS(error=EndpointConnectionError(endpoint_url="http://minio:9000"))
    response = TestClient(step_keys_app(vendor_for(sts=sts)[0])).post("/step-keys", content=TOKEN.encode())
    assert response.status_code == 503 and "AK" not in response.text


class FailingKube(StubKube):
    def __init__(self, error):
        super().__init__()
        self.error = error

    def token_review(self, token, audiences):
        raise self.error


@pytest.mark.parametrize("error, status", [
    (KubeError("POST tokenreviews: URLError", transient=True), 503),      # worth the init container's retry
    (KubeError("POST tokenreviews: 503", transient=True), 503),
    (KubeError("POST tokenreviews: 403", transient=False), 403),
], ids=["unreachable", "api-5xx", "api-refuses"])
def test_kubernetes_trouble_is_retryable_unless_it_is_a_refusal(error, status):
    client = TestClient(step_keys_app(vendor_for(kube=FailingKube(error))[0]))
    response = client.post("/step-keys", content=TOKEN.encode())
    assert response.status_code == status


class FailingRuntime(FakeRuntime):
    def __init__(self, error):
        super().__init__([RUNNING])
        self.error = error

    def status(self, name):
        raise self.error


@pytest.mark.parametrize("error, status", [(NotFound("job-1"), 403), (ConnectionError("argo down"), 503)],
                         ids=["no-workflow", "argo-unreachable"])
def test_argo_trouble_is_retryable_unless_the_workflow_is_gone(error, status):
    step_keys = StepKeys(StubKube(), FailingRuntime(error), Vendor(StubSTS(), "arn:aws:iam::0:role/r"), "my-bucket",
                         "argo")
    assert TestClient(step_keys_app(step_keys)).post("/step-keys", content=TOKEN.encode()).status_code == status


def test_only_post():
    assert TestClient(step_keys_app(vendor_for()[0])).get("/step-keys").status_code == 405


# ---- the manager: the Reader it gives render, dataset inputs checked like files


def test_credentials_steps_make_the_reader():
    vendor = Vendor(StubSTS(), "arn:aws:iam::0:role/r", region="us-west-2")
    reader = reader_from({"vendor_url": "http://hydrowf-ogc.hydrowf.svc:8001/step-keys",
                          "init_image": "curlimages/curl@sha256:" + "a" * 64,
                          "s3_endpoint": "http://minio.argo.svc:9000"}, "my-bucket", vendor)
    assert (reader.bucket, reader.region, reader.audience) == ("my-bucket", "us-west-2", AUDIENCE)
    assert reader_from(None, "my-bucket", vendor) is None


@pytest.mark.parametrize("section, vendor", [
    ({"vendor_url": "http://x:8001/step-keys"}, None),                                    # no STS vendor
    ({"vendor_url": "http://x:8001/step-keys", "init_image": "curl:8", "s3_endpoint": "http://m:9000"}, "v"),
    ({"other": 1}, "v"),
], ids=["no-vendor", "unpinned-image", "unknown-field"])
def test_a_bad_steps_section_fails_at_startup(section, vendor):
    with pytest.raises(ValueError):
        reader_from(section, "my-bucket", Vendor(StubSTS(), "arn:aws:iam::0:role/r") if vendor else None)


class DatasetProcessor:
    """What check_inputs reads of a processor: the pipeline's inputs."""

    def __init__(self):
        from hydrowf_core.models import Port

        class P:
            inputs = [Port(name="ds", type="dataset", data="ngen-cng")]
        self.pipeline = P()


def test_a_dataset_input_is_checked_like_a_file_input():
    theirs = RunStatus("job-theirs", "Succeeded", labels={caller.OWNER_LABEL: "someone-else"})
    m = manager(FakeRuntime([theirs]))
    with caller.acting_as(ME), pytest.raises(ForbiddenInput):
        m.check_inputs(DatasetProcessor(), {"ds": "s3://my-bucket/job-theirs/job-theirs-step-x-1/cng/"})


@pytest.mark.parametrize("url, error", [
    ("s3://my-bucket/job-theirs/job-theirs-step-x-1/cng/", ForbiddenInput),         # not the caller's
    ("s3://other-bucket/job-1/job-1-step-x-1/cng/", BadExecuteRequest),             # not our bucket
    ("s3://my-bucket/job-1/../job-theirs/x/", BadExecuteRequest),                   # not a plain key
], ids=["not-theirs", "other-bucket", "unclean"])
def test_a_dataset_input_is_refused_as_a_file_input_would_be(url, error):
    theirs = RunStatus("job-theirs", "Succeeded", labels={caller.OWNER_LABEL: "someone-else"})
    mine = RunStatus("job-1", "Succeeded", labels=MINE)
    m = manager(FakeRuntime([theirs, mine]))
    with caller.acting_as(ME), pytest.raises(error):
        m.check_inputs(DatasetProcessor(), {"ds": url})


def test_a_dataset_input_of_another_recorded_type_is_refused_and_ones_own_passes():
    out = "s3://my-bucket/job-1/job-1-step-x-1/cng/"
    wrong = RunStatus("job-1", "Succeeded", labels=MINE, formats={"cng": "teehr-timeseries@1.0.0"})
    right = RunStatus("job-1", "Succeeded", labels=MINE, formats={"cng": "ngen-cng@2.0.0"})
    with caller.acting_as(ME):
        with pytest.raises(BadExecuteRequest, match="is teehr-timeseries data"):
            manager(FakeRuntime([wrong], outputs={"job-1": {"cng": out}})).check_inputs(DatasetProcessor(), {"ds": out})
        manager(FakeRuntime([right], outputs={"job-1": {"cng": out}})).check_inputs(DatasetProcessor(), {"ds": out})
