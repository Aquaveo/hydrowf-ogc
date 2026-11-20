"""STAC Items from a tool's manifest (hydrowf_ogc.stac): pure functions, no S3."""
import copy
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from hydrowf_ogc import stac
from hydrowf_ogc.downloads import Presigner

DIR = "s3://my-bucket/job-done/job-done-step-to-cng-1/cng/"
FINISHED = datetime(2026, 11, 15, 0, 40, 15, tzinfo=timezone.utc)
SHA = "0" * 64

# What ngiab-to-cng 0.1.0 writes (hydrowf-tools nextgen/ngiab-to-cng), trimmed: manifest 0.1 has no bbox or period
V01 = {
    "manifest_version": "0.1",
    "tool": {"name": "ngiab-to-cng", "version": "0.1.0"},
    "run_id": "job-done",
    "configuration_name": "ngiab",
    "created_at": "2026-11-15T00:40:01+00:00",
    "run_metadata": {"ngen": "v0.3"},
    "inputs": [{"path": "config/realization.json", "sha256": SHA}],
    "outputs": [
        {"path": "timeseries/catchment.parquet", "format": "parquet",
         "schema": "teehr.secondary_timeseries@0.8.0", "rows": 10, "variables": {"q": {"count": 10}}},
        {"path": "timeseries/catchment.zarr", "format": "zarr", "zarr_format": 3},
        {"path": "gridded/cog/precip_rate_total.tif", "format": "cog", "units": "mm"},
        {"path": "hydrofabric/divides.parquet", "format": "geoparquet",
         "schema": "icefabric.hydrofabric_v2.2.Divides@9bcff38"},
    ],
    "not_converted": [],
}
V02 = {**copy.deepcopy(V01), "manifest_version": "0.2", "bbox": [-71.3, 44.9, -70.9, 45.2],
       "start_datetime": "2017-10-01T00:00:00+00:00", "end_datetime": "2019-09-30T23:00:00+00:00"}
PRESIGNER = Presigner("https://s3.example.org", "reader", "secret", expires=600)


def item_of(manifest: dict, finished_at=FINISHED) -> dict:
    return stac.item(job_id="job-done", output="cng", s3_url=DIR, data="ngen-cng@1.0.0",
                     manifest=stac.Manifest.model_validate(manifest), manifest_path="manifest.json",
                     finished_at=finished_at, presigner=PRESIGNER)


def test_each_output_is_an_asset_typed_by_its_format():
    assets = item_of(V01)["assets"]
    parquet = assets["timeseries/catchment.parquet"]
    assert parquet["type"] == "application/vnd.apache.parquet"
    assert parquet["hydrowf:schema"] == "teehr.secondary_timeseries@0.8.0"
    assert parquet["href"].startswith(
        "https://s3.example.org/my-bucket/job-done/job-done-step-to-cng-1/cng/timeseries/catchment.parquet?")
    assert parquet["alternate"] == {"s3": {"href": DIR + "timeseries/catchment.parquet"}}
    assert assets["gridded/cog/precip_rate_total.tif"]["type"] == \
        "image/tiff; application=geotiff; profile=cloud-optimized"
    assert assets["hydrofabric/divides.parquet"]["hydrowf:format"] == "geoparquet"


def test_a_zarr_store_is_its_s3_address_with_no_download_link():
    zarr = item_of(V01)["assets"]["timeseries/catchment.zarr"]
    assert zarr == {"href": DIR + "timeseries/catchment.zarr", "type": "application/vnd+zarr",
                    "roles": ["data"], "hydrowf:format": "zarr"}


def test_the_manifest_itself_is_a_metadata_asset():
    manifest = item_of(V01)["assets"]["manifest.json"]
    assert manifest["roles"] == ["metadata"] and manifest["alternate"]["s3"]["href"] == DIR + "manifest.json"


def test_without_bbox_or_period_the_item_has_no_geometry_and_the_jobs_finish_time():
    found = item_of(V01)
    assert found["geometry"] is None and "bbox" not in found
    assert found["properties"]["datetime"] == "2026-11-15T00:40:15Z"


def test_a_manifest_bbox_and_period_place_the_item_in_space_and_time():
    found = item_of(V02)
    assert found["bbox"] == [-71.3, 44.9, -70.9, 45.2]
    assert found["geometry"]["coordinates"][0][0] == [-71.3, 44.9]
    assert found["geometry"]["coordinates"][0][0] == found["geometry"]["coordinates"][0][-1]   # a closed ring
    props = found["properties"]
    assert props["datetime"] is None
    assert (props["start_datetime"], props["end_datetime"]) == ("2017-10-01T00:00:00Z", "2019-09-30T23:00:00Z")


def test_the_item_says_where_it_came_from_and_leaves_the_tools_own_fields_out():
    found = item_of(V01)
    assert found["id"] == "job-done-cng" and found["stac_version"] == "1.1.0"
    props = found["properties"]
    assert props["hydrowf:tool"] == "ngiab-to-cng@0.1.0" and props["hydrowf:data"] == "ngen-cng@1.0.0"
    assert props["hydrowf:inputs"] == [{"path": "config/realization.json", "sha256": SHA}]
    assert props["created"] == "2026-11-15T00:40:01Z"
    assert "run_metadata" not in str(found) and "variables" not in str(found)


def test_with_no_period_and_no_finish_time_the_creation_time_is_used():
    assert item_of(V01, finished_at=None)["properties"]["datetime"] == "2026-11-15T00:40:01Z"


def with_output(path):
    return {**V01, "outputs": [{"path": path, "format": "other"}]}


@pytest.mark.parametrize("bad", [
    with_output("../other-job/x.parquet"),
    with_output("a/../../x"),
    with_output("/etc/passwd"),
    with_output("a//b.parquet"),
    with_output("dir/"),
    with_output("a b.parquet"),
    with_output("{{workflow.name}}.parquet"),
    {**V01, "bbox": [-200, 0, 0, 10]},
    {**V01, "bbox": [10, 0, -10, 10]},                    # west > east: across the antimeridian, not supported
    {**V01, "bbox": [0, 10, 1, 0]},
    {**V01, "start_datetime": "2017-10-01T00:00:00+00:00"},
    {**V01, "start_datetime": "2019-10-01T00:00:00+00:00", "end_datetime": "2017-10-01T00:00:00+00:00"},
    {**V01, "start_datetime": "2017-10-01T00:00:00", "end_datetime": "2019-10-01T00:00:00"},   # no time zone
    {**V01, "outputs": V01["outputs"] + V01["outputs"][:1]},
    {**V01, "inputs": [{"path": "x", "sha256": "not-a-hash"}]},
    {**V01, "outputs": [{"path": f"f{n}.csv", "format": "csv"} for n in range(stac.MAX_ASSETS + 1)]},
    {k: v for k, v in V01.items() if k != "outputs"},
], ids=["dotdot", "dotdot-inside", "absolute", "empty-part", "trailing-slash", "space", "argo-template",
        "bbox-range", "bbox-antimeridian", "bbox-south-north", "half-period", "period-backwards", "naive-times",
        "duplicate-path", "bad-sha", "too-many-outputs", "no-outputs"])
def test_a_manifest_that_breaks_the_contract_is_refused(bad):
    with pytest.raises(ValidationError):
        stac.Manifest.model_validate(bad)
