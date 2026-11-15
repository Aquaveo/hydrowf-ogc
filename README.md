# hydrowf-platform

hydrowf backend: OGC API - Processes plugins and publishing, on top of
[hydrowf-core](https://github.com/Aquaveo/hydrowf-core).

hydrowf-core holds the lego models (tools, data, pipelines) and the Argo adapter: `render()`
writes an Argo Workflow from a pipeline lego and its input values, and `ArgoRuntime` submits it
and reads its status and outputs. This repository will serve that over HTTP (a pygeoapi process
plugin and job manager) and publish run outputs.

The CWL → Argo translator that used to live here was replaced by hydrowf-core's Argo adapter.
What running the NGIAB tools on Argo taught us is still in [docs/argo-findings.md](docs/argo-findings.md).

## Tests

```sh
uv run pytest
```
