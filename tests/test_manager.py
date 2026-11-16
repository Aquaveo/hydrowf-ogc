"""ArgoManager with a fake ArgoRuntime (no cluster), driven through pygeoapi's own execute_process."""

from datetime import datetime, timezone
from pathlib import Path

import pytest
from hera.exceptions import NotFound
from pygeoapi.process.base import JobNotFoundError, JobResultNotFoundError
from pygeoapi.util import JobStatus

from hydrowf_core.adapters.argo_runtime import RunStatus
from hydrowf_platform.manager import ArgoManager, BadExecuteRequest, job_dict

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
        self.runs[name] = RunStatus(
            name, "Pending", pipeline=workflow["metadata"]["labels"]["hydrowf/pipeline"]
        )
        return name

    def status(self, name):
        if name not in self.runs:
            raise NotFound(name)
        return self.runs[name]

    def list(self, pipeline=None):
        return sorted(
            self.runs.values(),
            key=lambda r: (r.created_at is not None, r.created_at),
            reverse=True,
        )

    def outputs(self, name):
        return self.out.get(name, {})

    def delete(self, name):
        if self.runs.pop(name, None) is None:
            raise NotFound(name)


def manager(runtime=None):
    return ArgoManager(
        {
            "name": "hydrowf_platform.manager.ArgoManager",
            "runtime": runtime or FakeRuntime(),
            "argo": {
                "bucket": "my-bucket",
                "entrypoints": {NGEN: ["/ngen/HelloNGEN.sh"]},
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
)
QUEUED = RunStatus(
    "job-queued",
    "Pending",
    pipeline="ngiab-simulation",
    created_at=utc(2026, 11, 16, 10, 0, 0),
)


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


def test_execute_with_bad_inputs_is_a_400():
    with pytest.raises(BadExecuteRequest, match="start_date") as err:
        manager().execute_process(
            "ngiab-simulation", {k: v for k, v in INPUTS.items() if k != "start_date"}
        )
    assert err.value.http_status_code == 400


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


def test_delete_removes_the_run():
    m = manager(FakeRuntime([DONE]))
    assert m.delete_job("job-done") is True
    assert m.runtime.runs == {}
