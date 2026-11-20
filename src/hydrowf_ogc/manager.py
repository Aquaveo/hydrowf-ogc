"""ArgoManager: pygeoapi jobs are Argo Workflows.

The manager keeps no job state. Executing a process renders the pipeline lego and submits it,
named after pygeoapi's job id (so job id = workflow name = the run's {run}). Every other call reads
Argo through hydrowf_core's ArgoRuntime: a job's status is the workflow's phase, the job list is the
workflows render() labelled, results are links to the pipeline's outputs (presigned when configured).
Every run belongs to the caller who submitted it (the hydrowf/owner label, hydrowf_ogc.caller):
the caller sees, reads and dismisses only their own, and passes as file or directory inputs only
their own jobs' outputs or keys under a shared prefix (check_inputs). Any pygeoapi worker can answer
for any job, and a restart loses nothing.

pygeoapi config:

    server:
      manager:
        name: hydrowf_ogc.manager.ArgoManager
        connection:                                # pygeoapi's "where jobs are stored": Argo
          host: https://argo-server.argo:2746     # or HYDROWF_ARGO_HOST
          namespace: argo
          bucket: my-bucket                        # the artifact repository bucket
          verify_ssl: true
          entrypoints: {}                          # image -> ENTRYPOINT, for tools without `command`
          downloads: {endpoint: https://s3.example.org}   # presigned result links: see downloads.py
          shared_inputs: [hydrofabric/]            # key prefixes every caller may pass as an input
          retention: {days: 30}                    # a finished job and its outputs are deleted after (default 30)
          registry: {url: http://hydrowf-registry:8000, ttl: 60}   # processes from the registry: see registry.py
        output_dir: /tmp                           # required by pygeoapi's schema; unused

With `registry` (or HYDROWF_REGISTRY_URL), the processes are the registry's public pipelines, each version
as `<id>@<version>` and the latest as `<id>`; the config's process resources are then not used.

The Argo token comes from HYDROWF_ARGO_TOKEN, never from the config file.
"""
from __future__ import annotations

import logging
import math
import mimetypes
import os
import re
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit

import yaml
from botocore.exceptions import BotoCoreError, ClientError
from hera.exceptions import NotFound
from pydantic import ValidationError
from pygeoapi.process.base import (BaseProcessor, JobNotFoundError, JobResultNotFoundError,
                                   ProcessorExecuteError, UnknownProcessError)
from pygeoapi.process.manager.base import BaseManager
from pygeoapi.util import DATETIME_FORMAT, JobStatus

from hydrowf_core.adapters.argo import (FILE_TYPES, MAX_KEEP_DAYS, BadInputs, UnsupportedForArgo, latest_data,
                                        output_formats, render)
from hydrowf_core.adapters.argo_runtime import ArgoRuntime, RunStatus
from hydrowf_core.models import Pipeline
from hydrowf_core.models.lego import DataLego, Lego, Port, StacStorage
from hydrowf_ogc import caller, stac
from hydrowf_ogc.downloads import Presigner, TooLarge, clean_key, presigner_from, split_s3_url
from hydrowf_ogc.processor import PipelineProcessor
from hydrowf_ogc.registry import RegistryError, RegistryProcesses, RemoteRegistry

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
    """One output in a job's results, for its s3://<bucket>/<key>; s3 is the lasting address another
    pipeline takes as an input, and every href downloads (presigned: it expires).

    - <key>.tgz (data stored with archive: tgz, and every output of jobs from before files): one link.
    - <key>/ (a directory stored as files): its files, each with a link, at most MAX_LISTED of them;
      `truncated` says whether there were more. If they can't be listed, `error` says so.
    - any other key (a file stored as itself): one link, typed by its file name.

    presigner is None when the manager config has no `downloads` section: the link is then the s3://
    URL alone, as before downloads existed.
    """
    if presigner is None:
        return {"href": s3_url}
    if s3_url.endswith(".tgz"):
        return {"href": presigner.url(s3_url), "type": "application/gzip", "s3": s3_url}
    if s3_url.endswith("/"):
        link = {"s3": s3_url, "type": "inode/directory"}
        try:
            files, truncated = presigner.list(s3_url)
        except (BotoCoreError, ClientError) as err:      # S3 unreachable or refusing: the rest still answers
            LOGGER.warning("listing %s: %s", s3_url, err)
            return {**link, "files": [], "error": "the files can't be listed right now"}
        return {**link, "truncated": truncated,
                "files": [{"path": path, "size": size, "href": presigner.url(s3_url + path)} for path, size in files]}
    kind, encoding = mimetypes.guess_type(s3_url.rsplit("/", 1)[-1])
    kind = COMPRESSED.get(encoding) or kind or "application/octet-stream"   # x.tar.gz is gzip, not a tar
    return {"href": presigner.url(s3_url), "type": kind, "s3": s3_url}


COMPRESSED = {"gzip": "application/gzip", "bzip2": "application/x-bzip2", "xz": "application/x-xz"}


KEEP_DAYS = 30
REGISTRY_TTL = 60                          # seconds a registry snapshot is served before a refresh


def keep_days(retention: object) -> int:
    """The config's retention.days: how long a finished job and its outputs are kept. ValueError when
    the section isn't {days: <whole number of days, 1 to MAX_KEEP_DAYS>}."""
    if retention is None:
        return KEEP_DAYS
    days = retention.get("days", KEEP_DAYS) if isinstance(retention, dict) and set(retention) <= {"days"} else None
    if isinstance(days, bool) or not isinstance(days, int) or not 1 <= days <= MAX_KEEP_DAYS:
        raise ValueError(f"retention: {retention!r} is not {{days: <whole number of days, 1 to {MAX_KEEP_DAYS}>}}")
    return days


def registry_from(section: object, fetch: Any = None) -> RemoteRegistry | None:
    """The config's `registry: {url, ttl}` (HYDROWF_REGISTRY_URL overrides the url); None without a url,
    and the processes then come from the config's resources. ValueError for a section that isn't one."""
    section = section or {}
    if not isinstance(section, dict) or not set(section) <= {"url", "ttl"}:
        raise ValueError(f"registry: {section!r} is not {{url: <hydrowf-registry URL>, ttl: <seconds>}}")
    url = os.environ.get("HYDROWF_REGISTRY_URL") or section.get("url")
    if not url:
        return None
    if not isinstance(url, str) or urlsplit(url).scheme not in ("http", "https") or not urlsplit(url).hostname:
        raise ValueError(f"registry: url {url!r} is not an http(s) URL")
    ttl = section.get("ttl", REGISTRY_TTL)
    if isinstance(ttl, bool) or not isinstance(ttl, (int, float)) or not math.isfinite(ttl) or ttl < 1:
        raise ValueError(f"registry: ttl {ttl!r} is not a number of seconds, at least 1")
    return RemoteRegistry(url, ttl, fetch) if fetch else RemoteRegistry(url, ttl)


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
    # pygeoapi's execute_process keeps the processor it runs on the manager (self.processor), set and
    # read back within one call. One manager serves every request thread, so a shared attribute would let
    # two concurrent executions swap processors: here it is per thread.
    @property
    def processor(self) -> BaseProcessor | None:
        return getattr(self._per_thread, "processor", None)

    @processor.setter
    def processor(self, processor: BaseProcessor | None) -> None:
        self._per_thread.processor = processor

    def __init__(self, manager_def: dict):
        self._per_thread = threading.local()
        super().__init__(manager_def)
        self.is_async = True
        self.supports_subscribing = False
        argo = manager_def.get("connection") or {}
        self.entrypoints: dict[str, list[str]] = argo.get("entrypoints", {})
        self.bucket: str = argo["bucket"]
        self.shared_inputs = shared_prefixes(argo.get("shared_inputs"))
        self.keep_days = keep_days(argo.get("retention"))
        self.runtime: Any = manager_def.get("runtime") or ArgoRuntime(   # `runtime`: tests pass a fake
            host=os.environ.get("HYDROWF_ARGO_HOST", argo.get("host", "")),
            bucket=self.bucket,
            namespace=argo.get("namespace", "argo"),
            token=os.environ.get("HYDROWF_ARGO_TOKEN"),
            verify_ssl=argo.get("verify_ssl", True),
        )
        self.presigner: Presigner | None = manager_def.get("presigner") or presigner_from(argo.get("downloads"))
        self.registry = registry_from(argo.get("registry"), manager_def.get("registry_fetch"))
        if self.registry:                          # the processes come from hydrowf-registry, not resources
            self.processes = RegistryProcesses(self.registry)

    def get_processor(self, process_id: str) -> BaseProcessor:
        if not self.registry:
            return super().get_processor(process_id)
        snapshot = self.registry.snapshot()
        if process_id not in snapshot.processes:
            raise UnknownProcessError("Invalid process identifier")
        return PipelineProcessor.from_legos(process_id, snapshot.processes[process_id], snapshot.legos)

    # ---- execute: render and submit, nothing stored here

    def _execute_handler_async(self, p: BaseProcessor, job_id: str, data_dict: dict,
                               requested_outputs: dict | None = None, *args, **kwargs):
        if not isinstance(p, PipelineProcessor):
            raise ProcessorExecuteError(user_msg=f"{p!r} is not a PipelineProcessor")
        try:
            workflow = render(p.pipeline, p.legos, data_dict, self.entrypoints.get, name=job_id,
                              labels={caller.OWNER_LABEL: caller.owner()}, keep_days=self.keep_days)
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
            may_read, run = self._may_read(key, me)
            if not may_read:
                raise ForbiddenInput(user_msg=f"input {port.name}: {url} is neither an output of one of "
                                              "your jobs nor a shared input")
            if run is not None and (problem := self._type_problem(port, key, run)):
                raise BadExecuteRequest(user_msg=f"input {port.name}: {problem}")

    def _type_problem(self, port: Port, key: str, run: RunStatus) -> str | None:
        """Why the output at `key` of the caller's job `run` (_may_read's answer) can't feed `port`: its run
        recorded it as another data lego (hydrowf/formats). None when it fits, and when nothing is recorded
        for exactly that key: a run from before formats, or a file inside an output (no recorded type)."""
        try:
            outputs = self.runtime.outputs(run.name)
        except NotFound:                           # gone since _may_read: Argo will say so when it stages
            return None
        for name, url in outputs.items():
            if split_s3_url(url)[1].rstrip("/") != key.rstrip("/"):
                continue
            written = run.formats.get(name, "").partition("@")[0]
            if written and written != port.data:
                return f"{url} is {written} data; this input takes {port.data}"
        return None

    def _may_read(self, key: str, me: str) -> tuple[bool, RunStatus | None]:
        """Whether `me` may pass `key`, and the job run it belongs to (None for a shared key): something
        below a shared prefix (never the whole prefix), or one output of one of `me`'s jobs:
        <job>/<pod>/<output>[/...], never a whole job or a whole step, which would stage every output
        (and the logs) into one pod."""
        if any(key.startswith(prefix) and len(key.rstrip("/")) >= len(prefix) for prefix in self.shared_inputs):
            return True, None
        parts = key.rstrip("/").split("/")
        if len(parts) < 3 or not JOB_NAME.fullmatch(parts[0]):  # never ask Argo about a name it can't have
            return False, None
        try:
            run = self.runtime.status(parts[0])
        except NotFound:                           # a deleted job's outputs are nobody's
            return False, None
        return run.labels.get(caller.OWNER_LABEL) == me, run

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
        """A link per output (result_link); a directory output whose data lego is `driver: stac` with
        `item_from` also gets its STAC `item`, or an `item_error` saying why there is none."""
        run = self._run(job_id)
        if run.phase != "Succeeded":
            raise JobResultNotFoundError(f"job {job_id} is {run.phase}")
        outputs = self.runtime.outputs(job_id)
        data = self.output_data(run) if self.presigner and any(u.endswith("/") for u in outputs.values()) else {}
        results = {}
        for name, url in outputs.items():
            results[name] = result_link(url, self.presigner)
            if name in data and url.endswith("/"):
                results[name].update(self.item_for(run, name, url, data[name]))
        return MIMETYPE, results

    def pipeline_legos(self, pipeline_id: str, version: str) -> tuple[Pipeline, list[Lego]] | None:
        """The pipeline a run was rendered from (its hydrowf/pipeline and hydrowf/version labels) and the
        legos it was checked with; None when it is no longer offered."""
        if self.registry:
            try:
                snapshot = self.registry.snapshot()
            except RegistryError as err:            # never read yet: results still answer, without Items
                LOGGER.warning("no registry for %s@%s: %s", pipeline_id, version, err)
                return None
            pipeline = snapshot.processes.get(f"{pipeline_id}@{version}")
            return (pipeline, snapshot.legos) if pipeline else None
        for process_id, conf in self.processes.items():   # the config's resources: build only the match
            ref = (conf.get("processor") or {}).get("pipeline") or {}
            if (ref.get("id"), str(ref.get("version"))) != (pipeline_id, version):
                continue
            try:
                p = self.get_processor(process_id)
            except (OSError, yaml.YAMLError, RegistryError, ValidationError) as err:   # a broken registry file
                LOGGER.warning("process %s: %s", process_id, err)
                return None
            return (p.pipeline, p.legos) if isinstance(p, PipelineProcessor) else None
        return None

    def output_data(self, run: RunStatus) -> dict[str, DataLego]:
        """Pipeline output name -> its data lego, for the outputs whose data has STAC storage with
        `item_from`. The data lego is the one the run recorded (hydrowf/formats: id@version), so a newer
        version published since doesn't change an old job's Item. Runs from before formats, and recorded
        versions no longer offered, fall back to the pipeline's format at the data lego's latest version."""
        found = self.pipeline_legos(run.pipeline, run.labels.get("hydrowf/version", ""))
        if found is None:
            return {}
        pipeline, legos = found
        by_key = {(x.id, x.version): x for x in legos if isinstance(x, DataLego)}
        latest = latest_data(legos)
        written = {**output_formats(pipeline, legos), **run.formats}   # what the run recorded wins
        data = {}
        for name, ref in written.items():
            data_id, _, version = ref.partition("@")
            lego = by_key.get((data_id, version)) or latest.get(data_id)
            if lego and isinstance(lego.storage, StacStorage) and lego.storage.item_from:
                data[name] = lego
        return data

    def item_for(self, run: RunStatus, name: str, url: str, lego: DataLego) -> dict:
        """{"item": <STAC Item>} for the directory output `url`, or {"item_error": <why not>}."""
        manifest_path = lego.storage.item_from
        try:
            stac.relative_key(manifest_path)
        except ValueError:
            return {"item_error": f"{lego.id}@{lego.version}: item_from {manifest_path!r} is not a path"}
        try:
            raw = self.presigner.read(url + manifest_path, stac.MAX_MANIFEST)
        except ClientError as err:
            code = err.response.get("Error", {}).get("Code")
            if code in ("NoSuchKey", "404"):
                return {"item_error": f"the output has no {manifest_path}"}
            if code == "InvalidRange":             # bytes=0-N of an empty object
                return {"item_error": f"{manifest_path} is not a manifest: it is empty"}
            LOGGER.warning("reading %s%s: %s", url, manifest_path, err)
            return {"item_error": f"{manifest_path} can't be read right now"}
        except BotoCoreError as err:
            LOGGER.warning("reading %s%s: %s", url, manifest_path, err)
            return {"item_error": f"{manifest_path} can't be read right now"}
        except TooLarge:
            return {"item_error": f"{manifest_path} is over {stac.MAX_MANIFEST} bytes"}
        try:
            manifest = stac.Manifest.model_validate_json(raw)
            return {"item": stac.item(job_id=run.name, output=name, s3_url=url, data=f"{lego.id}@{lego.version}",
                                      manifest=manifest, manifest_path=manifest_path,
                                      finished_at=run.finished_at, presigner=self.presigner)}
        except ValidationError as err:
            first = err.errors()[0]
            where = ".".join(str(part) for part in first["loc"]) or "manifest"
            return {"item_error": f"{manifest_path} is not a manifest: {where}: {first['msg']}"}
        except ValueError as err:
            return {"item_error": str(err)}

    def delete_job(self, job_id: str) -> bool:
        self._run(job_id)                          # only the caller's own
        with job_lookup(job_id):
            self.runtime.delete(job_id)            # its outputs go too (render's artifactGC)
        return True
