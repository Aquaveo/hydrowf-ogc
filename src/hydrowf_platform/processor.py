"""PipelineProcessor: one pygeoapi process per pipeline lego.

The processor is the process's description only. Running is the manager's job (ArgoManager
submits to Argo), so `execute` is never called; the same split as 52North's KubernetesProcessor.

pygeoapi config:

    resources:
      ngiab-simulation:
        type: process
        processor:
          name: hydrowf_platform.processor.PipelineProcessor
          registry: registry/legos.yaml
          pipeline: {id: ngiab-simulation, version: 0.1.0}

With the processes coming from hydrowf-registry instead (ArgoManager, HYDROWF_REGISTRY_URL), the manager
builds each one with PipelineProcessor.from_legos.
"""
from __future__ import annotations

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from hydrowf_core.adapters.ogc import process_description
from hydrowf_core.models import Pipeline
from hydrowf_core.models.lego import Lego
from hydrowf_platform.registry import find_pipeline, load_legos


class PipelineProcessor(BaseProcessor):
    def __init__(self, processor_def: dict):
        self.legos = load_legos(processor_def["registry"])
        ref = processor_def["pipeline"]
        self.pipeline = find_pipeline(self.legos, ref["id"], str(ref["version"]))
        super().__init__(processor_def, process_description(self.pipeline, self.legos))

    @classmethod
    def from_legos(cls, process_id: str, pipeline: Pipeline, legos: list[Lego]) -> PipelineProcessor:
        """The process `process_id` (e.g. ngiab-simulation@0.1.0) running `pipeline`, with `legos` holding
        its tools and data. The description's id is the process id, so it matches the process's URL."""
        processor = cls.__new__(cls)
        processor.legos, processor.pipeline = legos, pipeline
        description = {**process_description(pipeline, legos), "id": process_id}
        BaseProcessor.__init__(processor, {"name": process_id}, description)
        return processor

    def execute(self, data: dict, outputs: dict | None = None):
        raise ProcessorExecuteError(user_msg="pipelines run through hydrowf_platform.manager.ArgoManager")

    def __repr__(self):
        return f"<PipelineProcessor> {self.pipeline.id}@{self.pipeline.version}"
