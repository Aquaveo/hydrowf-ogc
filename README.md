# hydrowf-ogc

The hydrowf service that runs pipelines for users: the OGC API - Processes (`/ogc`), pygeoapi on
Starlette with Keycloak bearer tokens, on top of [hydrowf-core](https://github.com/Aquaveo/hydrowf-core).
Clients list the pipelines, run one (an Argo workflow), follow it, download its results and dismiss it;
every job belongs to its caller. The pipelines come from
[hydrowf-registry](https://github.com/Aquaveo/hydrowf-registry), or from [registry/legos.yaml](registry/legos.yaml)
without one. How to run it, its rules and what was verified: [docs/ogc.md](docs/ogc.md). The image is
built by the [Dockerfile](Dockerfile).

hydrowf-core holds the lego models (tools, data, pipelines) and the Argo adapter: `render()` writes an
Argo Workflow from a pipeline lego and its input values, and `ArgoRuntime` submits it and reads its
status and outputs. This service exposes that over HTTP (`hydrowf_ogc.app`) and enforces who may run and
read what (`hydrowf_ogc.manager.ArgoManager`).

This repository was called hydrowf-platform until 2026-11-19. The CWL → Argo translator that used to
live here was replaced by hydrowf-core's Argo adapter; what running the NGIAB tools on Argo taught us is
still in [docs/argo-findings.md](docs/argo-findings.md).

## Tests

```sh
uv run pytest
```
