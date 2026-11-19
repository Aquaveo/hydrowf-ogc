# hydrowf-platform

hydrowf backend: OGC API - Processes plugins and publishing, on top of
[hydrowf-core](https://github.com/Aquaveo/hydrowf-core).

hydrowf-core holds the lego models (tools, data, pipelines) and the Argo adapter: `render()`
writes an Argo Workflow from a pipeline lego and its input values, and `ArgoRuntime` submits it
and reads its status and outputs. This repository serves that over HTTP:

- **hydrowf-ogc** ([docs/ogc.md](docs/ogc.md)): the OGC API - Processes as a service, pygeoapi on
  Starlette with Keycloak bearer tokens; every job belongs to its caller. Built by the
  [Dockerfile](Dockerfile).

The CWL → Argo translator that used to live here was replaced by hydrowf-core's Argo adapter.
What running the NGIAB tools on Argo taught us is still in [docs/argo-findings.md](docs/argo-findings.md).

## Tests

```sh
uv run pytest
```
