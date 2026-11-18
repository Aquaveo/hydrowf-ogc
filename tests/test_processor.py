from pathlib import Path

import pytest
from pygeoapi.process.base import ProcessorExecuteError

from hydrowf_core.adapters.argo import render
from hydrowf_core.adapters.ogc import process_description
from hydrowf_platform.processor import PipelineProcessor
from hydrowf_platform.registry import RegistryError, find_pipeline, load_legos

REGISTRY = Path(__file__).parent / "fixtures" / "legos.yaml"
SIMULATION = {"name": "hydrowf_platform.processor.PipelineProcessor", "registry": str(REGISTRY),
              "pipeline": {"id": "ngiab-simulation", "version": "0.1.0"}}


def test_metadata_is_the_pipelines_process_description():
    legos = load_legos(REGISTRY)
    expected = process_description(find_pipeline(legos, "ngiab-simulation", "0.1.0"), legos)
    assert PipelineProcessor(SIMULATION).metadata == expected


def test_execute_is_left_to_the_manager():
    with pytest.raises(ProcessorExecuteError):
        PipelineProcessor(SIMULATION).execute({"feature": "cat-1"})


def test_an_unknown_pipeline_fails_when_loaded():
    with pytest.raises(RegistryError, match="ngiab-simulation@9.9.9 is not in the registry"):
        PipelineProcessor({**SIMULATION, "pipeline": {"id": "ngiab-simulation", "version": "9.9.9"}})


def test_a_broken_registry_fails_when_loaded(tmp_path):
    broken = tmp_path / "legos.yaml"
    good, bad = "tool: {id: ngiab-run, version: 2.1.0}", "tool: {id: ngiab-run, version: 9.9.9}"
    broken.write_text(REGISTRY.read_text().replace(good, bad))
    with pytest.raises(RegistryError, match="ngiab-run@9.9.9 is not a registered tool"):
        load_legos(broken)


# ---- the registry the image ships (registry/legos.yaml), not the fixture

SHIPPED = Path(__file__).parents[1] / "registry" / "legos.yaml"
CALIBRATION_INPUTS = {"gage": "gage-01031510", "start_date": "2017-10-01", "end_date": "2019-09-30",
                      "hydrofabric": "s3://my-bucket/hydrofabric/v2.2/conus_nextgen.gpkg"}


def test_the_shipped_registry_loads_with_both_pipelines():
    legos = load_legos(SHIPPED)                  # runs hydrowf_core.registry.check on the whole file
    assert {"ngiab-simulation", "ngiab-calibration"} <= {lego.id for lego in legos if lego.kind == "pipeline"}


def test_calibration_renders_with_its_defaults():
    legos = load_legos(SHIPPED)
    workflow = render(find_pipeline(legos, "ngiab-calibration", "0.1.0"), legos, CALIBRATION_INPUTS,
                      lambda image: None, name="job-1")
    [calibrate] = [t for t in workflow["spec"]["templates"] if t["name"] == "step-calibrate"]
    assert calibrate["container"]["command"] == ["/opt/ngiab_calibrate.sh"]
    assert calibrate["container"]["args"][2:] == ["gage-01031510", "100", "365", "0.5"]
    # uid 1000 has no passwd entry in the image: without HOME, ~/.ngiab is /.ngiab and the step fails
    assert {"name": "HOME", "value": calibrate["container"]["workingDir"]} in calibrate["container"]["env"]
