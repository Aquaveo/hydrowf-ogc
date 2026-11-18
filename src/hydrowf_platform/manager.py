"""ArgoManager: pygeoapi jobs are Argo Workflows.

The manager keeps no job state. Executing a process renders the pipeline lego and submits it,
named after pygeoapi's job id (so job id = workflow name = the run's {run}). Every other call reads
Argo through hydrowf_core's ArgoRuntime: a job's status is the workflow's phase, the job list is the
workflows render() labelled, results are links to the pipeline's outputs (presigned when configured).
Every run belongs to the caller who submitted it (the hydrowf/owner label, hydrowf_platform.caller):
the caller sees, reads and dismisses only their own, and passes as file or directory inputs only
their own jobs' outputs or keys under a shared prefix (check_inputs). Any pygeoapi worker can answer
for any job, and a restart loses nothing.

pygeoapi config:

    server:
      manager:
        name: hydrowf_platform.manager.ArgoManager
        connection:                                # pygeoapi's "where jobs are stored": Argo
          host: https://argo-server.argo:2746     # or HYDROWF_ARGO_HOST
          namespace: argo
          bucket: my-bucket                        # the artifact repository bucket
          verify_ssl: true
          entrypoints: {}                          # image -> ENTRYPOINT, for tools without `command`
          downloads: {endpoint: https://s3.example.org}   # presigned result links: see downloads.py
          shared_inputs: [hydrofabric/]            # key prefixes every caller may pass as an input
        output_dir: /tmp                           # required by pygeoapi's schema; unused

The Argo token comes from HYDROWF_ARGO_TOKEN, never from the config file.
"""
from __future__ import annotations

import logging
import os
import re
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

from hera.exceptions import NotFound
from pygeoapi.process.base import (BaseProcessor, JobNotFoundError, JobResultNotFoundError,
                                   ProcessorExecuteError)
from pygeoapi.process.manager.base import BaseManager
from pygeoapi.util import DATETIME_FORMAT, JobStatus

from hydrowf_core.adapters.argo import FILE_TYPES, BadInputs, UnsupportedForArgo, render
from hydrowf_core.adapters.argo_runtime import ArgoRuntime, RunStatus
from hydrowf_platform import caller
from hydrowf_platform.downloads import Presigner, presigner_from, split_s3_url
from hydrowf_platform.processor import PipelineProcessor

LOGGER = logging.getLogger(__name__)


class BadExecuteRequest(ProcessorExecuteError):
    """The inputs don't fit the pipeline (HTTP 400 instead of pygeoapi's default 500)."""
    http_status_code = 400
    ogc_exception_code = "InvalidParameterValue"


class ForbiddenInput(ProcessorExecuteError):
    """An input the caller may not read (HTTP 403). Missing and someone else's look the same."""
    http_status_code = 403
    ogc_exception_code = "Forbidden"


MIMETYPE = "application/json"

JOB_NAME = re.compile(r"[a-z0-9](?:[-a-z0-9]{0,251}[a-z0-9])?")   # a workflow name, as job ids are

STATUS = {                                    # Argo phase -> OGC job status
    "Pending": JobStatus.accepted,
    "Running": JobStatus.running,
    "Succeeded": JobStatus.successful,
    "Failed": JobStatus.failed,
    "Error": JobStatus.failed,
}


def get_progress(value: str | None, succeeded: bool = False) -> int:
    """Percent done from Argo's "done/total"; 0 when it can't be read."""
    if succeeded:
        return 100
    try:
        done, total = map(int, value.split("/"))
    except (ValueError, AttributeError, TypeError):
        return 0
    return max(0, min(100, done * 100 // total)) if total > 0 else 0


def format_datetime(value: datetime | None) -> str | None:
    return value.astimezone(timezone.utc).strftime(DATETIME_FORMAT) if value else None


def job_dict(run: RunStatus) -> dict:
    """A pygeoapi job record (the keys pygeoapi.api.processes reads) for one run."""
    times = [t for t in (run.created_at, run.started_at, run.finished_at) if t is not None]
    return {
        "type": "process",
        "identifier": run.name,
        "process_id": run.pipeline,
        "status": STATUS.get(run.phase, JobStatus.accepted).value,   # "" = not picked up yet
        "message": run.message,
        "progress": get_progress(run.progress, succeeded=run.phase == "Succeeded"),
        "mimetype": MIMETYPE,
        "location": None,
        "created": format_datetime(run.created_at),
        "started": format_datetime(run.started_at or run.created_at),  # pygeoapi sorts jobs by it
        "finished": format_datetime(run.finished_at),
        "updated": format_datetime(max(times, default=None)),
    }


def result_link(s3_url: str, presigner: Presigner | None) -> dict:
    """One output in a job's results: an OGC link object for the s3://<bucket>/<key> .tgz.

    href downloads it (presigned, it expires); s3 is the lasting address another pipeline takes as
    an input. presigner is None when the manager config has no `downloads` section: the link is then
    the s3:// URL alone, as before downloads existed.
    """
    if presigner is None:
        return {"href": s3_url}
    # every output is a file or directory Argo archives with its default (.tgz): render() sets no other
    return {"href": presigner.url(s3_url), "type": "application/gzip", "s3": s3_url}


KEY_CHARS = re.compile(r"[A-Za-z0-9._/-]+")   # no {{ }}: Argo expands them in the artifact key it reads


def clean_key(key: str) -> bool:
    """A prefix match on the text means the same key to Argo and the store: plain characters only, and
    no empty, `.` or `..` part."""
    return bool(KEY_CHARS.fullmatch(key)) and all(
        part not in ("", ".", "..") for part in key.removesuffix("/").split("/"))


def shared_prefixes(value: object) -> tuple[str, ...]:
    """The config's shared_inputs: key prefixes like `hydrofabric/`. ValueError when one isn't."""
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ValueError(f"shared_inputs: {value!r} is not a list of key prefixes")
    for prefix in value:
        if not (isinstance(prefix, str) and prefix.endswith("/") and clean_key(prefix)):
            raise ValueError(f"shared_inputs: {prefix!r} is not a key prefix ending in /, like hydrofabric/")
    return tuple(value)


@contextmanager
def job_lookup(job_id: str) -> Iterator[None]:
    """Argo's NotFound is pygeoapi's JobNotFoundError (HTTP 404)."""
    try:
        yield
    except NotFound as err:
        raise JobNotFoundError(f"no job {job_id}") from err


class ArgoManager(BaseManager):
    def __init__(self, manager_def: dict):
        super().__init__(manager_def)
        self.is_async = True
        self.supports_subscribing = False
        argo = manager_def.get("connection") or {}
        self.entrypoints: dict[str, list[str]] = argo.get("entrypoints", {})
        self.bucket: str = argo["bucket"]
        self.shared_inputs = shared_prefixes(argo.get("shared_inputs"))
        self.runtime: Any = manager_def.get("runtime") or ArgoRuntime(   # `runtime`: tests pass a fake
            host=os.environ.get("HYDROWF_ARGO_HOST", argo.get("host", "")),
            bucket=self.bucket,
            namespace=argo.get("namespace", "argo"),
            token=os.environ.get("HYDROWF_ARGO_TOKEN"),
            verify_ssl=argo.get("verify_ssl", True),
        )
        self.presigner: Presigner | None = manager_def.get("presigner") or presigner_from(argo.get("downloads"))

    # ---- execute: render and submit, nothing stored here

    def _execute_handler_async(self, p: BaseProcessor, job_id: str, data_dict: dict,
                               requested_outputs: dict | None = None, *args, **kwargs):
        if not isinstance(p, PipelineProcessor):
            raise ProcessorExecuteError(user_msg=f"{p!r} is not a PipelineProcessor")
        try:
            workflow = render(p.pipeline, p.legos, data_dict, self.entrypoints.get, name=job_id,
                              labels={caller.OWNER_LABEL: caller.owner()})
        except (BadInputs, UnsupportedForArgo) as err:
            raise BadExecuteRequest(user_msg=str(err)) from err
        self.check_inputs(p, data_dict)
        self.runtime.submit(workflow)
        return MIMETYPE, None, JobStatus.accepted

    def _execute_handler_sync(self, p, job_id, data_dict, *args, **kwargs):
        # A run takes minutes: never hold an HTTP request open for it. The process description
        # advertises async-execute only, so pygeoapi picks the async handler.
        return self._execute_handler_async(p, job_id, data_dict, *args, **kwargs)

    def check_inputs(self, p: PipelineProcessor, data_dict: dict) -> None:
        """Every file or directory input the caller passed is theirs to read.

        Argo reads inputs with the cluster's own credentials, so this is the only thing between a
        caller and any key in the bucket. A key may be the output of one of the caller's jobs (Argo
        stores outputs under <job id>/, and job id = workflow name) or under a shared prefix.
        Port defaults come from the registry, not the caller, and aren't checked.
        """
        me = caller.owner()
        for port in p.pipeline.inputs:
            url = data_dict.get(port.name)
            if url is None or port.type not in FILE_TYPES:
                continue
            try:
                bucket, key = split_s3_url(url)
            except ValueError as err:
                raise BadExecuteRequest(user_msg=f"input {port.name}: {err}") from err
            if bucket != self.bucket:              # render() drops the bucket: Argo would read ours
                raise BadExecuteRequest(user_msg=f"input {port.name}: inputs are read from s3://{self.bucket}/")
            if not clean_key(key):
                raise BadExecuteRequest(user_msg=f"input {port.name}: a key uses A-Z a-z 0-9 . _ / - only, "
                                                 "with no empty, . or .. part")
            if not self._may_read(key, me):
                raise ForbiddenInput(user_msg=f"input {port.name}: {url} is neither an output of one of "
                                              "your jobs nor a shared input")

    def _may_read(self, key: str, me: str) -> bool:
        if key.startswith(self.shared_inputs):
            return True
        job, slash, _ = key.partition("/")
        if not (slash and JOB_NAME.fullmatch(job)):  # never ask Argo about a name it can't have
            return False
        try:
            run = self.runtime.status(job)
        except NotFound:                           # a deleted job's outputs are nobody's
            return False
        return run.labels.get(caller.OWNER_LABEL) == me

    def add_job(self, job_metadata: dict) -> str:
        return job_metadata["identifier"]          # Argo holds the job: nothing to store

    def update_job(self, job_id: str, update_dict: dict) -> bool:
        return True

    # ---- reads: every answer comes from Argo

    def _run(self, job_id: str) -> RunStatus:
        """The caller's run `job_id`. Someone else's run is a JobNotFoundError (404), like a missing one."""
        me = caller.owner()                        # before Argo: no caller, no lookup
        with job_lookup(job_id):
            run = self.runtime.status(job_id)
        if run.labels.get(caller.OWNER_LABEL) != me:   # an unlabelled run (older than owners) is nobody's
            raise JobNotFoundError(f"no job {job_id}")
        return run

    def get_job(self, job_id: str) -> dict:
        return job_dict(self._run(job_id))

    def get_jobs(self, status: JobStatus | None = None, limit: int | None = None,
                 offset: int | None = None) -> dict:
        mine = {caller.OWNER_LABEL: caller.owner()}
        jobs = [job_dict(run) for run in self.runtime.list(labels=mine)]   # the caller's, newest first
        if status is not None:
            jobs = [job for job in jobs if job["status"] == status.value]
        start = offset or 0
        return {"jobs": jobs[start:start + limit] if limit else jobs[start:], "numberMatched": len(jobs)}

    def get_job_result(self, job_id: str) -> tuple[str, Any]:
        run = self._run(job_id)
        if run.phase != "Succeeded":
            raise JobResultNotFoundError(f"job {job_id} is {run.phase}")
        return MIMETYPE, {name: result_link(url, self.presigner)
                          for name, url in self.runtime.outputs(job_id).items()}

    def delete_job(self, job_id: str) -> bool:
        self._run(job_id)                          # only the caller's own
        with job_lookup(job_id):
            self.runtime.delete(job_id)
        return True
