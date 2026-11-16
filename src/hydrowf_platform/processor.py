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
"""
from __future__ import annotations

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

from hydrowf_core.adapters.ogc import process_description
from hydrowf_platform.registry import find_pipeline, load_legos


class PipelineProcessor(BaseProcessor):
    def __init__(self, processor_def: dict):
        self.legos = load_legos(processor_def["registry"])
        ref = processor_def["pipeline"]
        self.pipeline = find_pipeline(self.legos, ref["id"], str(ref["version"]))
        super().__init__(processor_def, process_description(self.pipeline, self.legos))

    def execute(self, data: dict, outputs: dict | None = None):
        raise ProcessorExecuteError(user_msg="pipelines run through hydrowf_platform.manager.ArgoManager")

    def __repr__(self):
        return f"<PipelineProcessor> {self.pipeline.id}@{self.pipeline.version}"
