from pathlib import Path

import pytest
from pygeoapi.process.base import ProcessorExecuteError

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
