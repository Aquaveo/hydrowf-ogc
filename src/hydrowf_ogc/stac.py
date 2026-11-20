"""STAC Items for job outputs, from the manifest the producing tool writes.

A tool whose output is a directory of cloud-native files (its data lego: `driver: stac`, `item_from`,
by default manifest.json) writes a manifest at the top of that directory. A job's results then carry one
STAC 1.1 Item per such output (hydrowf_ogc.manager), built here from the manifest alone: nothing here
knows a tool, a model or a domain, so any tool that writes the manifest gets Items.

The manifest, as far as Items read it (anything else in it is the tool's own and kept out of the Item):

    {
      "manifest_version": "0.2",
      "tool": {"name": "ngiab-to-cng", "version": "0.2.0"},          # optional
      "run_id": "...", "created_at": "2026-11-20T16:00:00+00:00",   # optional
      "bbox": [-71.3, 44.9, -70.9, 45.2],                           # optional: WGS84 lon/lat, west <= east
      "start_datetime": "...", "end_datetime": "...",               # optional, both or neither
      "inputs": [{"path": "config/realization.json", "sha256": "..."}],
      "outputs": [{"path": "timeseries/catchment.parquet", "format": "parquet",
                   "schema": "teehr.secondary_timeseries@0.8.0"}, ...]
    }

Each output is an asset, keyed by its path below the directory. A path is a plain relative key (the
characters an input key may use, no empty, `.` or `..` part), so an asset stays inside the directory
and its s3 address can be another pipeline's input. A manifest that breaks any of this gives no Item.

Without bbox the Item's geometry is null (valid STAC, but it can't be found by area); without a period
its datetime is the job's finish time.
"""
from __future__ import annotations

import mimetypes
from datetime import datetime, timezone
from typing import Annotated, Any, Optional

from pydantic import AfterValidator, AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from hydrowf_ogc.downloads import Presigner, clean_key

MAX_MANIFEST = 1024 * 1024           # bytes read from a manifest, at most
MAX_ASSETS = 1000                    # outputs a manifest may list (MAX_LISTED files are listed per directory)
STAC_VERSION = "1.1.0"
ALTERNATE = "https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json"

# The manifest's format -> the asset's media type (STAC best practices); others are typed by file name.
MEDIA_TYPES = {
    "cog": "image/tiff; application=geotiff; profile=cloud-optimized",
    "zarr": "application/vnd+zarr",
    "parquet": "application/vnd.apache.parquet",
    "geoparquet": "application/vnd.apache.parquet",
    "netcdf": "application/netcdf",
    "json": "application/json",
}
DIRECTORY_FORMATS = {"zarr"}         # stored as many objects: no single download link, the s3 address only


def relative_key(path: str) -> str:
    if path.startswith("/") or path.endswith("/") or not clean_key(path):
        raise ValueError("a path below the output directory: A-Z a-z 0-9 . _ / - only, "
                         "with no empty, . or .. part")
    return path


RelativeKey = Annotated[str, Field(max_length=1024), AfterValidator(relative_key)]


class Lenient(BaseModel):
    model_config = ConfigDict(extra="ignore")          # the tool's own fields stay in the manifest


class Tool(Lenient):
    name: Annotated[str, Field(max_length=200)]
    version: Annotated[str, Field(max_length=32)]


class Input(Lenient):
    path: Annotated[str, Field(max_length=1024)]
    sha256: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class Output(Lenient):
    path: RelativeKey
    format: Annotated[str, Field(max_length=32)]
    schema_: Annotated[Optional[str], Field(alias="schema", max_length=200)] = None


class Manifest(Lenient):
    manifest_version: Annotated[str, Field(max_length=16)]
    tool: Optional[Tool] = None
    run_id: Annotated[Optional[str], Field(max_length=253)] = None
    created_at: Optional[AwareDatetime] = None
    bbox: Optional[tuple[float, float, float, float]] = None
    start_datetime: Optional[AwareDatetime] = None
    end_datetime: Optional[AwareDatetime] = None
    inputs: Annotated[list[Input], Field(max_length=MAX_ASSETS)] = []
    outputs: Annotated[list[Output], Field(max_length=MAX_ASSETS)]

    @model_validator(mode="after")
    def check_space_and_time(self) -> Manifest:
        if self.bbox is not None:
            west, south, east, north = self.bbox
            if not (-180 <= west <= east <= 180 and -90 <= south <= north <= 90):
                raise ValueError("bbox is [west, south, east, north] in WGS84 degrees, west <= east")
        if (self.start_datetime is None) != (self.end_datetime is None):
            raise ValueError("start_datetime and end_datetime come together")
        if self.start_datetime and self.end_datetime and self.start_datetime > self.end_datetime:
            raise ValueError("start_datetime is after end_datetime")
        paths = [o.path for o in self.outputs]
        if len(set(paths)) != len(paths):
            raise ValueError("an output path is listed twice")
        return self


def utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def asset(output: Output, s3_url: str, presigner: Presigner) -> dict[str, Any]:
    """One output as a STAC asset. href downloads it (presigned: it expires) when it is a single object;
    a store of many objects (Zarr) has its s3 address only. The s3 address, which lasts, is the
    `s3` alternate either way."""
    s3 = s3_url + output.path
    kind = MEDIA_TYPES.get(output.format) or mimetypes.guess_type(output.path)[0] or "application/octet-stream"
    found: dict[str, Any] = {"type": kind, "roles": ["data"], "hydrowf:format": output.format}
    if output.schema_:
        found["hydrowf:schema"] = output.schema_
    if output.format in DIRECTORY_FORMATS:
        return {"href": s3, **found}
    return {"href": presigner.url(s3), **found, "alternate": {"s3": {"href": s3}}}


def item(*, job_id: str, output: str, s3_url: str, data: str, manifest: Manifest, manifest_path: str,
         finished_at: datetime | None, presigner: Presigner) -> dict[str, Any]:
    """The STAC Item of one job output: the directory s3_url (ending in "/"), described by `manifest`
    (read from s3_url + manifest_path). `data` is the output's data lego, id@version."""
    properties: dict[str, Any] = {}
    if manifest.start_datetime and manifest.end_datetime:
        properties.update(datetime=None, start_datetime=utc_text(manifest.start_datetime),
                          end_datetime=utc_text(manifest.end_datetime))
    else:
        when = finished_at or manifest.created_at
        if when is None:
            raise ValueError("no datetime: the manifest has no period and the job no finish time")
        properties["datetime"] = utc_text(when)
    if manifest.created_at:
        properties["created"] = utc_text(manifest.created_at)
    properties.update({"hydrowf:job": job_id, "hydrowf:output": output, "hydrowf:data": data})
    if manifest.tool:
        properties["hydrowf:tool"] = f"{manifest.tool.name}@{manifest.tool.version}"
    if manifest.run_id:
        properties["hydrowf:run_id"] = manifest.run_id
    if manifest.inputs:
        properties["hydrowf:inputs"] = [{"path": i.path, "sha256": i.sha256} for i in manifest.inputs]

    found: dict[str, Any] = {
        "type": "Feature",
        "stac_version": STAC_VERSION,
        "stac_extensions": [ALTERNATE],
        "id": f"{job_id}-{output}",
        "geometry": None,
        "properties": properties,
        "links": [],
        "assets": {o.path: asset(o, s3_url, presigner) for o in manifest.outputs},
    }
    if manifest.bbox:
        west, south, east, north = manifest.bbox
        found["bbox"] = list(manifest.bbox)
        found["geometry"] = {"type": "Polygon", "coordinates": [
            [[west, south], [east, south], [east, north], [west, north], [west, south]]]}
    manifest_s3 = s3_url + manifest_path
    found["assets"].setdefault(manifest_path, {
        "href": presigner.url(manifest_s3), "type": "application/json", "roles": ["metadata"],
        "alternate": {"s3": {"href": manifest_s3}}})
    return found
