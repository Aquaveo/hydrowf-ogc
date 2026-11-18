"""ArgoManager with a fake ArgoRuntime (no cluster), driven through pygeoapi's own execute_process."""

import contextvars
from datetime import datetime, timezone
from pathlib import Path

import pytest
from hera.exceptions import NotFound
from pygeoapi.process.base import JobNotFoundError, JobResultNotFoundError
from pygeoapi.util import JobStatus

from hydrowf_core.adapters.argo_runtime import RunStatus
from hydrowf_platform import caller
from hydrowf_platform.downloads import Presigner
from hydrowf_platform.manager import ArgoManager, BadExecuteRequest, ForbiddenInput, job_dict, shared_prefixes

REGISTRY = Path(__file__).parent / "fixtures" / "legos.yaml"
PROCESSOR = {
    "name": "hydrowf_platform.processor.PipelineProcessor",
    "registry": str(REGISTRY),
    "pipeline": {"id": "ngiab-simulation", "version": "0.1.0"},
}
INPUTS = {
    "feature": "gage-01031510",
    "start_date": "2017-10-01",
    "end_date": "2019-09-30",
    "hydrofabric": "s3://my-bucket/hydrofabric/v2.2/conus_nextgen.gpkg",
}
ME, SOMEONE = "8d3a6e2c-1f0b-4c4e-9a51-0e6d2b7c9f10", "5b1e0c7a-2d4f-4e8b-a3c6-9f0d1e2b3c4a"   # Keycloak subs
MINE = {caller.OWNER_LABEL: ME}
NGEN = "awiciroh/ciroh-ngen-image@sha256:d5ee1a6c15c025f9be2f9ce140dd9cdc2e933bc30f531a41f180d45b15c8ab17"


def utc(*args):
    return datetime(*args, tzinfo=timezone.utc)


class FakeRuntime:
    """An in-memory ArgoRuntime: runs live in a dict."""

    def __init__(self, runs=(), outputs=None):
        self.runs = {run.name: run for run in runs}
        self.out = outputs or {}
        self.submitted = []

    def submit(self, workflow):
        self.submitted.append(workflow)
        name = workflow["metadata"]["name"]
        labels = workflow["metadata"]["labels"]
        self.runs[name] = RunStatus(name, "Pending", pipeline=labels["hydrowf/pipeline"], labels=labels)
        return name

    def status(self, name):
        if name not in self.runs:
            raise NotFound(name)
        return self.runs[name]

    def list(self, pipeline=None, labels=None):
        wanted = (labels or {}).items()
        return sorted(
            (run for run in self.runs.values() if wanted <= run.labels.items()),
            key=lambda r: (r.created_at is not None, r.created_at),
            reverse=True,
        )

    def outputs(self, name):
        return self.out.get(name, {})

    def delete(self, name):
        if self.runs.pop(name, None) is None:
            raise NotFound(name)


def manager(runtime=None, presigner=None):
    return ArgoManager(
        {
            "name": "hydrowf_platform.manager.ArgoManager",
            "runtime": runtime or FakeRuntime(),
            "presigner": presigner,
            "connection": {
                "bucket": "my-bucket",
                "entrypoints": {NGEN: ["/ngen/HelloNGEN.sh"]},
                "shared_inputs": ["hydrofabric/"],
            },
            "processes": {
                "ngiab-simulation": {"type": "process", "processor": PROCESSOR}
            },
        }
    )


DONE = RunStatus(
    "job-done",
    "Succeeded",
    "",
    {"preprocess": "Succeeded", "run": "Succeeded"},
    pipeline="ngiab-simulation",
    created_at=utc(2026, 11, 15, 0, 29, 58),
    started_at=utc(2026, 11, 15, 0, 29, 58),
    finished_at=utc(2026, 11, 15, 0, 40, 15),
    progress="2/2",
    labels=MINE,
)
RUNNING = RunStatus(
    "job-running",
    "Running",
    "",
    {"preprocess": "Succeeded", "run": "Running"},
    pipeline="ngiab-simulation",
    created_at=utc(2026, 11, 16, 9, 0, 0),
    started_at=utc(2026, 11, 16, 9, 0, 1),
    progress="1/3",
    labels=MINE,
)
QUEUED = RunStatus(
    "job-queued",
    "Pending",
    pipeline="ngiab-simulation",
    created_at=utc(2026, 11, 16, 10, 0, 0),
    labels=MINE,
)
THEIRS = RunStatus("job-theirs", "Succeeded", pipeline="ngiab-simulation", created_at=utc(2026, 11, 16, 11),
                   labels={caller.OWNER_LABEL: SOMEONE})
NOBODYS = RunStatus("job-nobodys", "Succeeded", pipeline="ngiab-simulation",   # submitted before owners existed
                    created_at=utc(2026, 11, 16, 12))


@pytest.fixture(autouse=True)
def as_me():
    """Every test runs as ME, the way guard() runs a request with a verified token."""
    with caller.acting_as(ME):
        yield


# ---- job_dict: one run -> the job record pygeoapi reads


def test_job_dict_of_a_finished_run():
    assert job_dict(DONE) == {
        "type": "process",
        "identifier": "job-done",
        "process_id": "ngiab-simulation",
        "status": "successful",
        "message": "",
        "progress": 100,
        "mimetype": "application/json",
        "location": None,
        "created": "2026-11-15T00:29:58.000000Z",
        "started": "2026-11-15T00:29:58.000000Z",
        "finished": "2026-11-15T00:40:15.000000Z",
        "updated": "2026-11-15T00:40:15.000000Z",
    }


def test_job_dict_of_a_running_run():
    job = job_dict(RUNNING)
    assert (job["status"], job["progress"], job["finished"], job["updated"]) == (
        "running",
        33,
        None,
        "2026-11-16T09:00:01.000000Z",
    )


def test_a_queued_run_starts_when_it_was_created():
    job = job_dict(QUEUED)  # pygeoapi sorts the job list by `started`: never None
    assert (job["status"], job["progress"], job["started"]) == (
        "accepted",
        0,
        "2026-11-16T10:00:00.000000Z",
    )


@pytest.mark.parametrize(
    "phase, status",
    [
        ("Pending", "accepted"),
        ("Running", "running"),
        ("Succeeded", "successful"),
        ("Failed", "failed"),
        ("Error", "failed"),
    ],
)
def test_argo_phases_become_ogc_statuses(phase, status):
    assert (
        job_dict(RunStatus("j", phase, created_at=utc(2026, 1, 1)))["status"] == status
    )


# ---- execute: through pygeoapi's own execute_process


def test_execute_submits_the_rendered_pipeline_named_after_the_job():
    m = manager()
    job_id, _, _, status, _ = m.execute_process("ngiab-simulation", INPUTS)
    [workflow] = m.runtime.submitted
    assert status == JobStatus.accepted
    assert (
        workflow["metadata"]["name"] == job_id
        and "generateName" not in workflow["metadata"]
    )
    assert m.get_job(job_id)["status"] == "accepted"
    assert workflow["metadata"]["labels"][caller.OWNER_LABEL] == ME          # the run is the caller's


def test_execute_with_bad_inputs_is_a_400():
    with pytest.raises(BadExecuteRequest, match="start_date") as err:
        manager().execute_process(
            "ngiab-simulation", {k: v for k, v in INPUTS.items() if k != "start_date"}
        )
    assert err.value.http_status_code == 400


# ---- execute: file and directory inputs are the caller's own job outputs or shared

OUTPUT_OF = "s3://my-bucket/{}/{}-step-run-1/ngen-run.tgz".format


def run_with(hydrofabric):
    return {**INPUTS, "hydrofabric": hydrofabric}


def test_an_output_of_the_callers_own_job_is_an_input():
    m = manager(FakeRuntime([DONE]))
    m.execute_process("ngiab-simulation", run_with(OUTPUT_OF("job-done", "job-done")))
    assert len(m.runtime.submitted) == 1


@pytest.mark.parametrize("url", [
    OUTPUT_OF("job-theirs", "job-theirs"),               # another caller's job
    OUTPUT_OF("job-nobodys", "job-nobodys"),             # a job from before owners
    OUTPUT_OF("job-gone", "job-gone"),                   # no such job (or a deleted one)
    "s3://my-bucket/hydrofabric-v3/conus.gpkg",          # a name that only starts like the prefix
    "s3://my-bucket/Not_A_Job/x.tgz",                    # can't be a workflow name: Argo isn't asked
    "s3://my-bucket/job-done",                           # a bare job id is not one of its outputs
])
def test_any_other_key_is_forbidden_and_nothing_runs(url):
    m = manager(FakeRuntime([DONE, THEIRS, NOBODYS]))
    with pytest.raises(ForbiddenInput, match="neither an output of one of your jobs nor a shared input") as err:
        m.execute_process("ngiab-simulation", run_with(url))
    assert err.value.http_status_code == 403              # the same answer whether the job exists or not
    assert m.runtime.submitted == []


@pytest.mark.parametrize("url", [
    "s3://my-bucket/hydrofabric/../job-theirs/x.tgz",    # out of the shared prefix by `..`
    "s3://my-bucket/hydrofabric/{{=sprig.repeat(2, '.')}}/job-theirs/x.tgz",   # Argo would expand it to ..
    "s3://my-bucket/hydrofabric/..\\job-theirs/x.tgz",   # a backslash
    "s3://my-bucket/hydrofabric//x.gpkg",                # an empty part
    "s3://my-bucket/hydrofabric/./x.gpkg",               # a . part
    "s3://my-bucket//hydrofabric/x.gpkg",                # a leading /
])
def test_a_key_that_argo_or_the_store_could_read_differently_is_a_400(url):
    m = manager(FakeRuntime([DONE, THEIRS]))
    with pytest.raises(BadExecuteRequest, match="A-Z a-z 0-9 . _ / - only"):
        m.execute_process("ngiab-simulation", run_with(url))
    assert m.runtime.submitted == []


def test_when_argo_cant_say_whose_job_it_is_nothing_runs():
    class Down(FakeRuntime):
        def status(self, name):
            raise ConnectionError("argo-server unreachable")
    m = manager(Down())
    with pytest.raises(ConnectionError):                   # a 500, never "allowed"
        m.execute_process("ngiab-simulation", run_with(OUTPUT_OF("job-done", "job-done")))
    assert m.runtime.submitted == []


def test_an_input_from_another_bucket_is_a_400():
    m = manager()
    with pytest.raises(BadExecuteRequest, match="read from s3://my-bucket/"):
        m.execute_process("ngiab-simulation", run_with("s3://other-bucket/hydrofabric/v2.2/conus_nextgen.gpkg"))
    assert m.runtime.submitted == []


@pytest.mark.parametrize("prefixes", ["hydrofabric/", ["hydrofabric"], [""], ["/"], ["a/../b/"], [7]])
def test_shared_inputs_are_key_prefixes_ending_in_a_slash(prefixes):
    with pytest.raises(ValueError, match="shared_inputs"):
        shared_prefixes(prefixes)


# ---- reads


def test_get_jobs_is_newest_first_and_filters_by_status():
    m = manager(FakeRuntime([DONE, RUNNING, QUEUED]))
    assert [j["identifier"] for j in m.get_jobs()["jobs"]] == [
        "job-queued",
        "job-running",
        "job-done",
    ]
    assert [j["identifier"] for j in m.get_jobs(status=JobStatus.running)["jobs"]] == [
        "job-running"
    ]
    page = m.get_jobs(limit=1, offset=1)
    assert ([j["identifier"] for j in page["jobs"]], page["numberMatched"]) == (
        ["job-running"],
        3,
    )


def test_get_jobs_lists_only_the_callers_runs():
    m = manager(FakeRuntime([DONE, THEIRS, NOBODYS]))
    jobs = m.get_jobs()
    assert ([j["identifier"] for j in jobs["jobs"]], jobs["numberMatched"]) == (["job-done"], 1)


@pytest.mark.parametrize("job_id", ["job-theirs", "job-nobodys"])
@pytest.mark.parametrize("operation", ["get_job", "get_job_result", "delete_job"])
def test_another_callers_job_is_not_found(job_id, operation):
    m = manager(FakeRuntime([THEIRS, NOBODYS], outputs={job_id: {"run": "s3://my-bucket/k.tgz"}}))
    with pytest.raises(JobNotFoundError):                 # 404 like a missing job: no hint that it exists
        getattr(m, operation)(job_id)
    assert job_id in m.runtime.runs                        # and a refused delete deletes nothing


def test_without_a_caller_nothing_is_read_or_run():
    m = manager(FakeRuntime([DONE]))
    for call in (lambda: m.get_job("job-done"), m.get_jobs,
                 lambda: m.execute_process("ngiab-simulation", INPUTS)):
        with pytest.raises(caller.NoCaller):
            contextvars.Context().run(call)                # an empty context: what the manager sees outside guard()
    assert m.runtime.submitted == []


def test_an_unknown_job_is_not_found():
    with pytest.raises(JobNotFoundError):
        manager().get_job("nope")
    with pytest.raises(JobNotFoundError):
        manager().delete_job("nope")


def test_results_are_links_once_the_run_succeeded():
    url = "s3://my-bucket/job-done/job-done-step-run-1/ngen-run.tgz"
    m = manager(FakeRuntime([DONE, RUNNING], outputs={"job-done": {"run": url}}))
    assert m.get_job_result("job-done") == ("application/json", {"run": {"href": url}})
    with pytest.raises(JobResultNotFoundError):
        m.get_job_result("job-running")


def test_with_downloads_results_are_presigned_and_keep_the_s3_address():
    url = "s3://my-bucket/job-done/job-done-step-run-1/ngen-run.tgz"
    presigner = Presigner("https://s3.example.org", "reader", "secret", expires=600)
    m = manager(FakeRuntime([DONE], outputs={"job-done": {"run": url}}), presigner=presigner)
    _, results = m.get_job_result("job-done")
    link = results["run"]
    assert link["href"].startswith("https://s3.example.org/my-bucket/job-done/job-done-step-run-1/ngen-run.tgz?")
    assert (link["type"], link["s3"]) == ("application/gzip", url)


def test_the_downloads_section_turns_presigning_on(monkeypatch):
    monkeypatch.delenv("HYDROWF_S3_PUBLIC_ENDPOINT", raising=False)
    monkeypatch.setenv("HYDROWF_S3_ACCESS_KEY", "reader")
    monkeypatch.setenv("HYDROWF_S3_SECRET_KEY", "secret")
    url = "s3://my-bucket/job-done/job-done-step-run-1/ngen-run.tgz"
    m = ArgoManager({                       # the path a pygeoapi config takes: connection.downloads
        "name": "hydrowf_platform.manager.ArgoManager",
        "runtime": FakeRuntime([DONE], outputs={"job-done": {"run": url}}),
        "connection": {"bucket": "my-bucket", "downloads": {"endpoint": "https://s3.example.org"}},
    })
    assert m.get_job_result("job-done")[1]["run"]["href"].startswith("https://s3.example.org/my-bucket/")


def test_delete_removes_the_run():
    m = manager(FakeRuntime([DONE]))
    assert m.delete_job("job-done") is True
    assert m.runtime.runs == {}
