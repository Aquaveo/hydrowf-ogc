"""ArgoManager: pygeoapi jobs are Argo Workflows.

The manager keeps no job state. Executing a process renders the pipeline lego and submits it,
named after pygeoapi's job id (so job id = workflow name = the run's {run}). Every other call reads
Argo through hydrowf_core's ArgoRuntime: a job's status is the workflow's phase, the job list is the
workflows render() labelled, results are the s3:// links of the pipeline's outputs. Any pygeoapi
worker can answer for any job, and a restart loses nothing.

pygeoapi config:

    server:
      manager:
        name: hydrowf_platform.manager.ArgoManager
        argo:
          host: https://argo-server.argo:2746     # or HYDROWF_ARGO_HOST
          namespace: argo
          bucket: my-bucket                        # the artifact repository bucket
          verify_ssl: true
          entrypoints: {}                          # image -> ENTRYPOINT, for tools without `command`

The Argo token comes from HYDROWF_ARGO_TOKEN, never from the config file.
"""
from __future__ import annotations

import logging
import os
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

from hera.exceptions import NotFound
from pygeoapi.process.base import (BaseProcessor, JobNotFoundError, JobResultNotFoundError,
                                   ProcessorExecuteError)
from pygeoapi.process.manager.base import BaseManager
from pygeoapi.util import DATETIME_FORMAT, JobStatus

from hydrowf_core.adapters.argo import BadInputs, UnsupportedForArgo, render
from hydrowf_core.adapters.argo_runtime import ArgoRuntime, RunStatus
from hydrowf_platform.processor import PipelineProcessor

LOGGER = logging.getLogger(__name__)


class BadExecuteRequest(ProcessorExecuteError):
    """The inputs don't fit the pipeline (HTTP 400 instead of pygeoapi's default 500)."""
    http_status_code = 400
    ogc_exception_code = "InvalidParameterValue"


MIMETYPE = "application/json"

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
        argo = manager_def.get("argo", {})
        self.entrypoints: dict[str, list[str]] = argo.get("entrypoints", {})
        self.runtime: Any = manager_def.get("runtime") or ArgoRuntime(   # `runtime`: tests pass a fake
            host=os.environ.get("HYDROWF_ARGO_HOST", argo.get("host", "")),
            bucket=argo["bucket"],
            namespace=argo.get("namespace", "argo"),
            token=os.environ.get("HYDROWF_ARGO_TOKEN"),
            verify_ssl=argo.get("verify_ssl", True),
        )

    # ---- execute: render and submit, nothing stored here

    def _execute_handler_async(self, p: BaseProcessor, job_id: str, data_dict: dict,
                               requested_outputs: dict | None = None, *args, **kwargs):
        if not isinstance(p, PipelineProcessor):
            raise ProcessorExecuteError(user_msg=f"{p!r} is not a PipelineProcessor")
        try:
            workflow = render(p.pipeline, p.legos, data_dict, self.entrypoints.get, name=job_id)
        except (BadInputs, UnsupportedForArgo) as err:
            raise BadExecuteRequest(user_msg=str(err)) from err
        self.runtime.submit(workflow)
        return MIMETYPE, None, JobStatus.accepted

    def _execute_handler_sync(self, p, job_id, data_dict, *args, **kwargs):
        # A run takes minutes: never hold an HTTP request open for it. The process description
        # advertises async-execute only, so pygeoapi picks the async handler.
        return self._execute_handler_async(p, job_id, data_dict, *args, **kwargs)

    def add_job(self, job_metadata: dict) -> str:
        return job_metadata["identifier"]          # Argo holds the job: nothing to store

    def update_job(self, job_id: str, update_dict: dict) -> bool:
        return True

    # ---- reads: every answer comes from Argo

    def _run(self, job_id: str) -> RunStatus:
        with job_lookup(job_id):
            return self.runtime.status(job_id)

    def get_job(self, job_id: str) -> dict:
        return job_dict(self._run(job_id))

    def get_jobs(self, status: JobStatus | None = None, limit: int | None = None,
                 offset: int | None = None) -> dict:
        jobs = [job_dict(run) for run in self.runtime.list()]          # newest first
        if status is not None:
            jobs = [job for job in jobs if job["status"] == status.value]
        start = offset or 0
        return {"jobs": jobs[start:start + limit] if limit else jobs[start:], "numberMatched": len(jobs)}

    def get_job_result(self, job_id: str) -> tuple[str, Any]:
        run = self._run(job_id)
        if run.phase != "Succeeded":
            raise JobResultNotFoundError(f"job {job_id} is {run.phase}")
        return MIMETYPE, {name: {"href": url} for name, url in self.runtime.outputs(job_id).items()}

    def delete_job(self, job_id: str) -> bool:
        with job_lookup(job_id):
            self.runtime.delete(job_id)
        return True
