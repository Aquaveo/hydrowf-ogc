"""ArgoManager with a fake ArgoRuntime (no cluster), driven through pygeoapi's own execute_process."""

import contextvars
import io
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml
from botocore.exceptions import ClientError, EndpointConnectionError
from hera.exceptions import NotFound
from pygeoapi.process.base import JobNotFoundError, JobResultNotFoundError
from pygeoapi.util import JobStatus

from hydrowf_core.adapters.argo_runtime import RunStatus
from hydrowf_ogc import caller
from hydrowf_ogc.downloads import Presigner
from hydrowf_ogc.manager import (ArgoManager, BadExecuteRequest, ForbiddenInput, job_dict, keep_days,
                                 result_link, shared_prefixes)

REGISTRY = Path(__file__).parent / "fixtures" / "legos.yaml"
PROCESSOR = {
    "name": "hydrowf_ogc.processor.PipelineProcessor",
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


def manager(runtime=None, presigner=None, **connection):
    return ArgoManager(
        {
            "name": "hydrowf_ogc.manager.ArgoManager",
            "runtime": runtime or FakeRuntime(),
            "presigner": presigner,
            "connection": {
                "bucket": "my-bucket",
                "entrypoints": {NGEN: ["/ngen/HelloNGEN.sh"]},
                "shared_inputs": ["hydrofabric/"],
                **connection,
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
    """Every test runs as ME, the way the service runs a request with a verified token."""
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


def test_a_job_and_its_outputs_are_kept_30_days_by_default():
    m = manager()
    m.execute_process("ngiab-simulation", INPUTS)
    [workflow] = m.runtime.submitted
    assert workflow["spec"]["ttlStrategy"] == {"secondsAfterCompletion": 30 * 86400}
    assert workflow["spec"]["artifactGC"] == {"strategy": "OnWorkflowDeletion"}   # deleting it deletes them


def test_retention_days_come_from_the_config():
    m = manager(retention={"days": 7})
    m.execute_process("ngiab-simulation", INPUTS)
    assert m.runtime.submitted[0]["spec"]["ttlStrategy"] == {"secondsAfterCompletion": 7 * 86400}
    assert keep_days(None) == keep_days({}) == 30


@pytest.mark.parametrize("retention", [{"days": 0}, {"days": 1.5}, {"days": "30"}, {"days": True}, "30", [30],
                                       {"days": 3651}, {"day": 7}])     # too long for Argo; a misspelt key
def test_retention_is_a_whole_number_of_days(retention):
    with pytest.raises(ValueError, match="retention"):
        keep_days(retention)


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


@pytest.mark.parametrize("url", [
    OUTPUT_OF("job-done", "job-done"),                                   # a .tgz output
    "s3://my-bucket/job-done/job-done-step-to-cng-1/cng/",               # a directory output (files)
    "s3://my-bucket/job-done/job-done-step-x-1/region/region.gpkg",      # a file output
    "s3://my-bucket/hydrofabric/v2.2/",                                  # a folder below a shared prefix
], ids=["tgz", "directory", "file", "shared-subfolder"])
def test_an_output_of_the_callers_own_job_is_an_input(url):
    m = manager(FakeRuntime([DONE]))
    m.execute_process("ngiab-simulation", run_with(url))
    assert len(m.runtime.submitted) == 1


@pytest.mark.parametrize("url", [
    OUTPUT_OF("job-theirs", "job-theirs"),               # another caller's job
    OUTPUT_OF("job-nobodys", "job-nobodys"),             # a job from before owners
    OUTPUT_OF("job-gone", "job-gone"),                   # no such job (or a deleted one)
    "s3://my-bucket/hydrofabric-v3/conus.gpkg",          # a name that only starts like the prefix
    "s3://my-bucket/Not_A_Job/x.tgz",                    # can't be a workflow name: Argo isn't asked
    "s3://my-bucket/job-done",                           # a bare job id is not one of its outputs
    "s3://my-bucket/job-done/",                          # nor is the job as a directory: every output, logs too
    "s3://my-bucket/job-done/job-done-step-run-1/",      # nor a whole step
    "s3://my-bucket/hydrofabric/",                       # the whole shared tree, not something in it
    "s3://my-bucket/job-theirs/job-theirs-step-to-cng-1/cng/",   # another caller's directory output
])
def test_any_other_key_is_forbidden_and_nothing_runs(url):
    m = manager(FakeRuntime([DONE, THEIRS, NOBODYS]))
    with pytest.raises(ForbiddenInput, match="neither an output of one of your jobs nor a shared input") as err:
        m.execute_process("ngiab-simulation", run_with(url))
    assert err.value.http_status_code == 403              # the same answer whether the job exists or not
    assert m.runtime.submitted == []


@pytest.mark.parametrize("url", [
    "s3://my-bucket/hydrofabric/../job-theirs/x.tgz",    # out of the shared prefix by `..`
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


def test_a_key_with_an_argo_template_is_refused_before_the_manager_looks_at_it():
    m = manager(FakeRuntime([THEIRS]))
    # refused by core's render(), before the manager checks the key
    with pytest.raises(BadExecuteRequest, match=r"\{, \}, \$ and control characters are not allowed"):
        m.execute_process("ngiab-simulation", run_with("s3://my-bucket/hydrofabric/{{=sprig.repeat(2, '.')}}/x.tgz"))
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
            contextvars.Context().run(call)                # an empty context: what the manager sees outside a request
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
        "name": "hydrowf_ogc.manager.ArgoManager",
        "runtime": FakeRuntime([DONE], outputs={"job-done": {"run": url}}),
        "connection": {"bucket": "my-bucket", "downloads": {"endpoint": "https://s3.example.org"}},
    })
    assert m.get_job_result("job-done")[1]["run"]["href"].startswith("https://s3.example.org/my-bucket/")


def test_delete_removes_the_run():
    m = manager(FakeRuntime([DONE]))
    assert m.delete_job("job-done") is True
    assert m.runtime.runs == {}


# ---- results for outputs stored as files: a .tgz, a file, or a directory (a key ending in "/")


class StubLister:
    """An S3 client's list_objects_v2 paginator: `keys` under the prefix, or `error` when listing."""

    def __init__(self, keys=(), error=None):
        self.keys, self.error = list(keys), error

    def get_paginator(self, operation):
        stub = self

        class Paginator:
            def paginate(self, Bucket, Prefix, PaginationConfig):
                if stub.error:
                    raise stub.error
                matching = [{"Key": Prefix + key, "Size": 10} for key in stub.keys][:PaginationConfig["MaxItems"]]
                size = 3                                  # small pages: the listing must read every one
                for start in range(0, len(matching), size):
                    yield {"Contents": matching[start:start + size]}
        return Paginator()


def presigner_listing(keys=(), error=None):
    presigner = Presigner("https://s3.example.org", "reader", "secret", expires=600)
    presigner.lister = StubLister(keys, error)
    return presigner


DIR = "s3://my-bucket/job-done/job-done-step-to-cng-1/cng/"


@pytest.mark.parametrize("url, kind", [
    ("s3://my-bucket/job-done/job-done-step-run-1/ngen-run.tgz", "application/gzip"),
    ("s3://my-bucket/job-done/job-done-step-x-1/region/region.gpkg", "application/geopackage+sqlite3"),
    ("s3://my-bucket/job-done/job-done-step-x-1/out/data.unknownext", "application/octet-stream"),
], ids=["tgz", "file", "file-of-unknown-type"])
def test_a_tgz_or_a_file_is_one_link(url, kind):
    link = result_link(url, presigner_listing())
    assert link["s3"] == url and link["href"].startswith("https://s3.example.org/my-bucket/job-done/")
    if kind != "application/geopackage+sqlite3":       # mimetypes' answer for .gpkg depends on the system
        assert link["type"] == kind


def test_a_directory_lists_its_files_each_with_a_link():
    link = result_link(DIR, presigner_listing(["manifest.json", "gridded/cog/TMP.tif"]))
    assert (link["type"], link["s3"], link["truncated"]) == ("inode/directory", DIR, False)
    paths = {f["path"]: f for f in link["files"]}
    assert set(paths) == {"manifest.json", "gridded/cog/TMP.tif"}
    assert paths["gridded/cog/TMP.tif"]["href"].startswith(
        "https://s3.example.org/my-bucket/job-done/job-done-step-to-cng-1/cng/gridded/cog/TMP.tif?")
    assert paths["manifest.json"]["size"] == 10


def test_a_long_directory_listing_is_truncated():
    from hydrowf_ogc import downloads
    link = result_link(DIR, presigner_listing([f"f{n}.csv" for n in range(downloads.MAX_LISTED + 5)]))
    assert len(link["files"]) == downloads.MAX_LISTED and link["truncated"] is True


def test_a_directory_that_cannot_be_listed_still_answers():
    link = result_link(DIR, presigner_listing(error=EndpointConnectionError(endpoint_url="http://minio:9000")))
    assert link["s3"] == DIR and link["files"] == [] and "can't be listed" in link["error"]


def test_without_downloads_every_output_is_its_s3_address():
    assert result_link(DIR, None) == {"href": DIR}


def test_listing_uses_the_internal_endpoint(monkeypatch):
    from hydrowf_ogc.downloads import presigner, presigner_from
    presigner.cache_clear()
    monkeypatch.setenv("HYDROWF_S3_ACCESS_KEY", "reader")
    monkeypatch.setenv("HYDROWF_S3_SECRET_KEY", "secret")
    monkeypatch.delenv("HYDROWF_S3_PUBLIC_ENDPOINT", raising=False)
    monkeypatch.setenv("HYDROWF_S3_INTERNAL_ENDPOINT", "http://minio:9000")
    p = presigner_from({"endpoint": "https://s3.example.org"})
    assert p.client.meta.endpoint_url == "https://s3.example.org"         # links: what clients reach
    assert p.lister.meta.endpoint_url == "http://minio:9000"              # listing: what we reach


def test_listed_paths_with_odd_characters_stay_paths():
    odd = ["a b.csv", "q?x=1.csv", "frag#1.csv", "café.csv", "folder marker/"]
    link = result_link(DIR, presigner_listing(odd))
    paths = {f["path"]: f["href"] for f in link["files"]}
    assert set(paths) == {"a b.csv", "q?x=1.csv", "frag#1.csv", "café.csv"}     # the marker isn't a file
    for path, href in paths.items():
        before_query = href.split("?X-Amz-", 1)[0]                     # the signature's own query string
        assert before_query.startswith("https://s3.example.org/my-bucket/job-done/job-done-step-to-cng-1/cng/")
        assert "?" not in before_query and "#" not in before_query and " " not in before_query


def test_every_page_of_a_listing_is_read():
    link = result_link(DIR, presigner_listing([f"f{n}.csv" for n in range(10)]))
    assert len(link["files"]) == 10 and link["truncated"] is False


def test_a_compressed_file_is_typed_by_its_compression():
    link = result_link("s3://my-bucket/job-done/job-done-step-x-1/out/data.tar.gz", presigner_listing())
    assert link["type"] == "application/gzip"


# ---- STAC Items: a directory output whose data lego is driver: stac carries the Item of its manifest.json


class StubS3(StubLister):
    """A lister that also answers get_object: `objects` maps key -> bytes, or -> an exception to raise."""

    def __init__(self, objects=None, keys=()):
        super().__init__(keys)
        self.objects, self.reads = objects or {}, []

    def get_object(self, Bucket, Key, Range):
        self.reads.append((Bucket, Key, Range))
        found = self.objects.get(Key)
        if isinstance(found, Exception):
            raise found
        if found is None:
            raise ClientError({"Error": {"Code": "NoSuchKey", "Message": "missing"}}, "GetObject")
        if found == b"":                                  # S3 refuses any range of an empty object
            raise ClientError({"Error": {"Code": "InvalidRange", "Message": "empty"}}, "GetObject")
        start, end = (int(n) for n in Range.removeprefix("bytes=").split("-"))
        return {"Body": io.BytesIO(found[start:end + 1])}


def presigner_reading(objects=None):
    presigner = Presigner("https://s3.example.org", "reader", "secret", expires=600)
    presigner.lister = StubS3(objects, keys=["manifest.json", "timeseries/catchment.parquet"])
    return presigner


CNG = "s3://my-bucket/job-done/job-done-step-to-cng-1/cng/"
MANIFEST_KEY = "job-done/job-done-step-to-cng-1/cng/manifest.json"
MANIFEST = {"manifest_version": "0.1", "tool": {"name": "ngiab-to-cng", "version": "0.1.0"},
            "outputs": [{"path": "timeseries/catchment.parquet", "format": "parquet"}]}
SIMULATED = RunStatus("job-done", "Succeeded", pipeline="ngiab-simulation", finished_at=utc(2026, 11, 15, 0, 40, 15),
                      labels={**MINE, "hydrowf/pipeline": "ngiab-simulation", "hydrowf/version": "0.1.0"})
RUN_TGZ = "s3://my-bucket/job-done/job-done-step-run-1/ngen-run.tgz"


def results_with(objects, run=SIMULATED, outputs=None, registry=None):
    raw = {k: json.dumps(v).encode() if isinstance(v, dict) else v for k, v in objects.items()}
    presigner = presigner_reading(raw)
    m = manager(FakeRuntime([run], outputs={"job-done": outputs or {"run": RUN_TGZ, "cng": CNG}}), presigner=presigner)
    if registry:
        m.processes["ngiab-simulation"]["processor"] = {**PROCESSOR, "registry": str(registry)}
    return m.get_job_result("job-done")[1], presigner.lister


def test_a_stac_directory_output_carries_the_item_of_its_manifest():
    results, s3 = results_with({MANIFEST_KEY: MANIFEST})
    item = results["cng"]["item"]
    assert item["id"] == "job-done-cng" and item["properties"]["hydrowf:data"] == "ngen-cng@1.0.0"
    assert set(item["assets"]) == {"timeseries/catchment.parquet", "manifest.json"}
    assert [f["path"] for f in results["cng"]["files"]] == ["manifest.json", "timeseries/catchment.parquet"]
    assert "item" not in results["run"] and "item_error" not in results["run"]      # a .tgz has no Item
    assert s3.reads == [("my-bucket", MANIFEST_KEY, "bytes=0-1048576")]             # one bounded read


def test_an_output_without_its_manifest_says_so_and_keeps_its_links():
    results, _ = results_with({})
    assert results["cng"]["item_error"] == "the output has no manifest.json"
    assert "item" not in results["cng"] and len(results["cng"]["files"]) == 2


def test_a_manifest_that_breaks_the_contract_says_where():
    bad = {**MANIFEST, "outputs": [{"path": "../../job-theirs/x.parquet", "format": "parquet"}]}
    results, _ = results_with({MANIFEST_KEY: bad})
    assert results["cng"]["item_error"].startswith("manifest.json is not a manifest: outputs.0.path:")


@pytest.mark.parametrize("content, error", [
    (b"{not json", "manifest.json is not a manifest"),
    (b"[]", "manifest.json is not a manifest"),
    (b" " * (1024 * 1024 + 1), "manifest.json is over 1048576 bytes"),
], ids=["not-json", "not-an-object", "too-big"])
def test_a_manifest_that_is_not_one_is_an_item_error(content, error):
    results, _ = results_with({MANIFEST_KEY: content})
    assert results["cng"]["item_error"].startswith(error)


def test_a_manifest_that_cannot_be_read_right_now_is_an_item_error():
    results, _ = results_with({MANIFEST_KEY: EndpointConnectionError(endpoint_url="http://minio:9000")})
    assert results["cng"]["item_error"] == "manifest.json can't be read right now"


def test_a_run_of_a_pipeline_no_longer_offered_has_links_and_no_item():
    gone = RunStatus("job-done", "Succeeded", pipeline="ngiab-simulation",
                     labels={**MINE, "hydrowf/pipeline": "ngiab-simulation", "hydrowf/version": "9.9.9"})
    results, s3 = results_with({MANIFEST_KEY: MANIFEST}, run=gone)
    assert "item" not in results["cng"] and "item_error" not in results["cng"] and s3.reads == []


def test_without_downloads_no_manifest_is_read():
    m = manager(FakeRuntime([SIMULATED], outputs={"job-done": {"cng": CNG}}))
    assert m.get_job_result("job-done")[1] == {"cng": {"href": CNG}}


def registry_where_cng(tmp_path, *storages):
    """The fixture registry with ngen-cng's storage replaced: one version per storage, 1.0.0, 2.0.0, ..."""
    legos = yaml.safe_load(REGISTRY.read_text())
    cng = next(lego for lego in legos if lego["id"] == "ngen-cng")
    legos.remove(cng)
    legos += [{**cng, "version": f"{n + 1}.0.0", "storage": storage} for n, storage in enumerate(storages)]
    path = tmp_path / "legos.yaml"
    path.write_text(yaml.safe_dump(legos))
    return path


STAC = {"driver": "stac", "format": "other"}


@pytest.mark.parametrize("item_from", ["../../job-theirs/x/manifest.json", "/etc/manifest.json", "a//manifest.json",
                                       "meta/", "%2e%2e/manifest.json"])
def test_an_item_from_outside_the_output_is_never_read(tmp_path, item_from):
    registry = registry_where_cng(tmp_path, {**STAC, "item_from": item_from})
    results, s3 = results_with({MANIFEST_KEY: MANIFEST}, registry=registry)
    assert results["cng"]["item_error"] == f"ngen-cng@1.0.0: item_from {item_from!r} is not a path"
    assert s3.reads == []


@pytest.mark.parametrize("storage", [
    {**STAC, "item_from": None},
    {"driver": "stac", "format": "other", "archive": "tgz"},     # not a directory: no lookup at all
    {"driver": "iceberg", "tables": {}},
], ids=["item-from-null", "tgz", "not-stac"])
def test_an_output_whose_data_has_no_manifest_gets_neither_item_nor_error(tmp_path, storage):
    outputs = {"cng": CNG.rstrip("/") + ".tgz" if storage.get("archive") == "tgz" else CNG}
    registry = registry_where_cng(tmp_path, storage)
    results, s3 = results_with({MANIFEST_KEY: MANIFEST}, outputs=outputs, registry=registry)
    assert "item" not in results["cng"] and "item_error" not in results["cng"] and s3.reads == []


def test_the_latest_version_of_the_data_lego_decides(tmp_path):
    other = "job-done/job-done-step-to-cng-1/cng/meta/stac.json"
    registry = registry_where_cng(tmp_path, STAC, {**STAC, "item_from": "meta/stac.json"})
    results, s3 = results_with({other: MANIFEST}, registry=registry)
    assert results["cng"]["item"]["properties"]["hydrowf:data"] == "ngen-cng@2.0.0"
    assert [key for _, key, _ in s3.reads] == [other]


def test_a_manifest_s3_refuses_to_give_is_an_item_error():
    denied = ClientError({"Error": {"Code": "AccessDenied", "Message": "no"}}, "GetObject")
    results, _ = results_with({MANIFEST_KEY: denied})
    assert results["cng"]["item_error"] == "manifest.json can't be read right now"


def test_an_empty_manifest_is_not_a_manifest():
    results, _ = results_with({MANIFEST_KEY: b""})
    assert results["cng"]["item_error"] == "manifest.json is not a manifest: it is empty"


def test_a_manifest_with_no_time_at_all_is_an_item_error():
    unfinished = RunStatus("job-done", "Succeeded", pipeline="ngiab-simulation", labels=SIMULATED.labels)
    results, _ = results_with({MANIFEST_KEY: MANIFEST}, run=unfinished)
    assert results["cng"]["item_error"].startswith("no datetime")


@pytest.mark.parametrize("content", ["- kind: data\n  id: nope\n", "a: [unclosed\n", None],
                         ids=["not-legos", "not-yaml", "missing"])
def test_a_broken_registry_file_gives_links_without_items(tmp_path, content):
    broken = tmp_path / "legos.yaml"
    if content is not None:
        broken.write_text(content)
    results, s3 = results_with({MANIFEST_KEY: MANIFEST}, registry=broken)
    assert "item" not in results["cng"] and len(results["cng"]["files"]) == 2 and s3.reads == []
