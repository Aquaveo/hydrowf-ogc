# hydrowf-ogc: the OGC API - Processes service

pygeoapi's Processes and Jobs endpoints as a standalone service: a small Starlette app
([app.py](../src/hydrowf_ogc/app.py)) on uvicorn, in front of pygeoapi's API and ArgoManager.
Clients (the Tethys client app, the CLI, notebooks, the agent) call it with a Keycloak access token.

```
client ──HTTPS + Bearer JWT──▶ uvicorn (Starlette)
                                 └─ /ogc/* → caller_for() → worker thread: acting_as(owner)
                                               → pygeoapi.api.processes → ArgoManager ──▶ Argo
```

## Run it on the local kind cluster

From the repository root, with Argo at `https://localhost:2746` (`kubectl -n argo port-forward
svc/argo-server 2746:2746`) and Keycloak's `hydrowf` realm reachable:

```bash
docker build -t hydrowf-ogc .

cat > ogc.env <<EOF
HYDROWF_OIDC_ISSUER=http://localhost:8080/realms/hydrowf
HYDROWF_OIDC_AUDIENCE=hydrowf-api
EOF

docker run --rm --network host --env-file ogc.env hydrowf-ogc       # http://localhost:8000/ogc
```

The image bakes in [pygeoapi-config.yml](../pygeoapi-config.yml), which is the **local kind config**
(Argo at `localhost`, `verify_ssl: false`): any other cluster mounts its own over
`/opt/hydrowf/pygeoapi-config.yml` **and** sets `PYGEOAPI_SERVER_URL` (its default,
`http://localhost:8000/ogc`, would put localhost in every link). The environment:

| Variable | What it sets |
|---|---|
| `PYGEOAPI_SERVER_URL` | The public URL of `/ogc`. pygeoapi builds every link from it, and the routes are served under its path |
| `HYDROWF_OIDC_ISSUER`, `HYDROWF_OIDC_AUDIENCE` | Bearer tokens (see below). Without the issuer, only the catalogue is open and every job route is 401 |
| `HYDROWF_ARGO_HOST`, `HYDROWF_ARGO_TOKEN` | Another Argo server than the config's |
| `HYDROWF_S3_ACCESS_KEY`, `HYDROWF_S3_SECRET_KEY`, `HYDROWF_S3_PUBLIC_ENDPOINT` | Presigned result links, when the config has `downloads` |
| `HYDROWF_S3_INTERNAL_ENDPOINT` | Where this service reaches S3 to list a directory output's files (default: the public endpoint) |
| `HYDROWF_S3_READER_ACCESS_KEY`, `HYDROWF_S3_READER_SECRET_KEY` | kind: the read-only S3 user read keys are issued from (`credentials`, below). On EKS leave them unset: the pod's IAM role is used |
| `HYDROWF_STS_ENDPOINT` | Another STS than the config's `credentials.sts_endpoint` (kind: RustFS's own; on AWS omit it) |
| `HYDROWF_K8S_API`, `HYDROWF_K8S_TOKEN_FILE`, `HYDROWF_K8S_CA_FILE` | The step-key vendor's Kubernetes API access outside the cluster (kind, the service in Docker); in the cluster the pod's own service account is used |
| `PORT`, `VENDOR_PORT` | The public API's port (8000) and the step-key vendor's (8001; only with `credentials.steps`) |
| `HYDROWF_REGISTRY_URL` | hydrowf-registry's URL: the processes are then its public pipelines (see below), not the config's |
| `FORWARDED_ALLOW_IPS` | The ingress addresses whose `X-Forwarded-*` headers the public port trusts (the vendor's port trusts none) |

Job results are `s3://` links unless the manager config has a `downloads` section
([downloads.py](../src/hydrowf_ogc/downloads.py)); with it, `href`s are presigned HTTPS GETs that
expire, and `s3` keeps the address another pipeline takes as an input. How an output looks depends on
how its data lego stores it (`archive`, hydrowf-core):

| Stored as | `s3` | In the results |
|---|---|---|
| one `.tgz` (`archive: tgz`, e.g. `ngen-run`; and every output of jobs from before files) | `…/ngen-run.tgz` | one `href`, `type: application/gzip` |
| a file (the default, `archive: none`) | `…/<output>/<file name>` | one `href`, typed by the file name |
| a directory of files (the default) | `…/<output>/` | `files`: each file's `path`, `size` and `href`, at most 1000 (`truncated` says if there were more); `error` if they can't be listed |

A directory output whose data lego is `driver: stac` (with `item_from`, by default `manifest.json`)
also gets an `item`: a STAC 1.1 Item built from the manifest the tool wrote at the top of the directory
([stac.py](../src/hydrowf_ogc/stac.py) has the manifest's contract). Each output the manifest lists is
an asset keyed by its path: `href` downloads a single file (presigned), a Zarr store is its `s3://`
address, and every asset keeps its `s3` address as the `alternate`. The asset's `type` comes from the
manifest's `format`, and its `hydrowf:schema` from `schema` (e.g. `teehr.secondary_timeseries@0.8.0`).
The manifest's `bbox` (WGS84) is the Item's geometry and its `start_datetime`/`end_datetime` its
period; without them the geometry is null and `datetime` is the job's finish time. A manifest that is
missing, over 1 MiB or breaks the contract (a path outside the directory, a bad bbox) gives an
`item_error` instead; the links answer either way. A data lego whose tool writes no manifest sets
`item_from: null`. The Item is built on every request from the manifest (outputs don't change once
written); /ogc writes nothing to S3.

The key needs GetObject, and ListBucket for directories. On kind, mount a copy of the config with
`downloads: {endpoint: http://localhost:9000}` and port-forward `svc/minio` to 9000. Only the service
keeps those links from anonymous callers, so never turn `downloads` on under plain `pygeoapi serve`.

pygeoapi's API and the manager are built once, at startup: a manager config that is wrong
(`downloads`, `shared_inputs`, `retention`) stops the service there, before any request. The OpenAPI
document (`/ogc/openapi`) is generated then too, from the config and `PYGEOAPI_SERVER_URL` the service
runs with.

## Processes from the registry

With `connection.registry: {url, ttl}` in the manager config (or `HYDROWF_REGISTRY_URL`), the processes
are hydrowf-registry's public pipelines ([registry.py](../src/hydrowf_ogc/registry.py)): each
version as `<id>@<version>` (e.g. `ngiab-simulation@0.1.0`) and `<id>` as its latest version. The service
reads `GET /legos` once per `ttl` seconds (default 60). A lego that doesn't validate with this
hydrowf-core, or a pipeline whose tools aren't public, is left out and logged. If the registry can't be
reached, the last list is served; before any, the list is empty, and the registry is tried again at
most every 5 seconds. Once there is a list, a refresh never holds requests up: one request reads the
registry while the others keep the current list. Before the first list, requests wait for the first read.
Every read is bounded: connecting takes at most the resolver's time plus 5 s per address and 5 s for TLS,
and from then on a watchdog cuts the read at 15 s (request, headers and body alike). An answer is at most
32 MB, http(s) only, with no redirects; one cut short is refused. The registry is reached directly: proxy
variables (`HTTP(S)_PROXY`) aren't used, and a query string in its URL is dropped. Only public legos are
used, whatever the registry answers. A pipeline published to the registry
becomes runnable here within `ttl`, with no rebuild or restart. Without `registry`, the processes are the
config's resources, as before.

## Routes and who may call them

| Route | Methods | Who |
|---|---|---|
| `/ogc`, `/ogc/conformance`, `/ogc/openapi`, `/ogc/processes`, `/ogc/processes/{id}` | GET | anyone (the catalogue) |
| `/ogc/processes/{id}/execution` | POST | a bearer token; the job is the token's owner's |
| `/ogc/jobs`, `/ogc/jobs/{id}`, `/ogc/jobs/{id}/results`, `/ogc/jobs/{id}/credentials` | GET | a bearer token; the owner's jobs only |
| `/ogc/jobs/{id}` | DELETE | a bearer token; the owner's job only |

Any other method is 405. A request is authenticated before anything else is read. Only a POST's
body is read at all, counted as it streams in: past 1 MiB it is 413, whatever `Content-Length` says
(a chunked body has none, and Transfer-Encoding overrides it). Bodies sent with other methods are
never read. A 413 leaves the connection open: uvicorn reads and drops the rest of the body
(bandwidth, not memory), so the ingress should cap request bodies too. `caller_for()`
decides who a request acts as:

- **No bearer credential** (no `Authorization` header, or another scheme such as `Basic`): an
  anonymous read on the catalogue; 401 on a job route.
- **`Authorization: Bearer <token>`**: a valid token runs the request as its owner (its `sub`). Any
  other bearer credential (expired, forged, another audience, empty, a `sub` that can't be a label) is
  401, on the catalogue too: a bad token never falls back to anonymous.
- **An unreachable issuer** is a server error (500 in the logs), not a 401.

401s carry `WWW-Authenticate: Bearer`, with `error="invalid_token"` when a token was sent. A string
that isn't a JWT is refused without asking the issuer for keys. A key set that is reachable but empty
is a 401 too (PyJWT can't tell it from an unknown key id), logged as a warning.

## Read keys for a job's outputs

Cloud-native outputs are meant to be read in place (range requests), not downloaded. A presigned link
covers one object, which is enough for a COG or a Parquet file; a Zarr store or a folder of files needs
S3 keys. `GET /ogc/jobs/{id}/credentials` gives the job's owner keys that may only **read** that job's
outputs (the keys Argo recorded, never anything the caller names) for **15 minutes**
([credentials.py](../src/hydrowf_ogc/credentials.py); design: hydrowf-core
`docs/design/formats-and-data-inputs.md`, Part D):

```json
{"access_key_id": "...", "secret_access_key": "...", "session_token": "...", "expiration": "2026-11-20T22:00:00+00:00",
 "endpoint": "http://localhost:9000", "region": "us-east-1", "bucket": "my-bucket",
 "prefixes": ["<job>/<pod>/cng/"]}
```

```python
storage_options = {"key": c["access_key_id"], "secret": c["secret_access_key"], "token": c["session_token"],
                   "client_kwargs": {"endpoint_url": c["endpoint"], "region_name": c["region"]}}
xr.open_zarr(f"s3://{c['bucket']}/{c['prefixes'][0]}timeseries/troute.zarr", storage_options=storage_options,
             consolidated=False)      # our stores have no consolidated metadata: don't look for it
```

They come from S3 STS (`AssumeRole` with a session policy: GetObject on the outputs, ListBucket only under
them). Their parent is a read-only identity, so a session can never write: on kind, the RustFS user
`hydrowf-reader` (hydrowf-deploy `local/s3-reader.sh`); on EKS, the pod's IAM role, allowed to assume a
role that may only read the bucket. Only outputs under the job's own keys (`<job>/<pod>/<output>` in this
bucket) are covered: an output Argo recorded elsewhere (a tool or a workflow editor could forge one) gets
no keys at all. Every answer of the route is `Cache-Control: no-store`, and `HEAD` is refused. Someone
else's job, a job that hasn't succeeded, or a service without a `credentials` section is `404`; an STS that
can't be reached, or refuses, is `503`. `endpoint` is the config's `credentials.endpoint`, else the
downloads endpoint; `region` is the config's (sessions can't look up a bucket's region).

## Steps that read in place (the step-key vendor)

With `credentials.steps` in the manager config, a pipeline's dataset ports read the bucket in place:
a managed data lego, or a dataset input given as one of the caller's own job outputs (checked like file
inputs: ownership and recorded type). render gives such a step an init container that trades the pod's
projected service-account token for read keys, and the tool reads with them (hydrowf-core
`docs/design/formats-and-data-inputs.md`, Part D.4):

```yaml
credentials:
  role_arn: arn:aws:iam::0:role/hydrowf-reader
  steps:
    vendor_url: http://<this service, as step pods reach it>:8001/step-keys
    init_image: curlimages/curl@sha256:...       # any image with curl, pinned
    s3_endpoint: http://minio.argo.svc:9000      # S3 as step pods reach it
```

The vendor ([stepkeys.py](../src/hydrowf_ogc/stepkeys.py)) listens on `VENDOR_PORT` (8001), which no
ingress may route. It gives keys only when every check holds:
- the token passes a TokenReview for the audience `hydrowf-vendor` only (the pod's default service-account
  token, which the tool can see, doesn't), and names a pod of the workflows' namespace whose uid matches;
- the pod's controlling owner is the Workflow its label names, by that workflow's uid;
- the workflow carries hydrowf-ogc's labels (owner, pipeline) and is still running, and the pod is the
  very pod Argo made for one step of it (the name Argo derives for the step's node, which is Pending or
  Running): a pod built to look like the step can't take that name while the real one exists;
- the keys are that step's `hydrowf/reads`, recorded by render and checked again; they last 1 hour.

Every refusal is the same `403 refused` (the reason is logged); a body that isn't a token never reaches the
API server; an API server, Argo or STS that can't be reached is `503`, which the init container retries.

**Trust boundary.** In the workflows' namespace, these rights would each let someone get another job's
keys, so only hydrowf-ogc's service account (and cluster admins) may hold them, and the tools' service
account (`default`) none: create workflows (they could write any `hydrowf/reads`); create pods (a
look-alike can't take a running step's pod name, but could wait for one); `pods/exec` (into a step's init
container); create `serviceaccounts/token`. The vendor needs create TokenReviews and get pods
(hydrowf-deploy). The token and the keys cross the pod network in plain HTTP: keep port 8001 reachable
only from the workflows' namespace (a NetworkPolicy), or put it behind a mesh with TLS.

## Job owners

Each run belongs to the token's `sub`: execute labels the Argo workflow `hydrowf/owner=<sub>`, the
job list shows only the caller's runs, and another caller's job (or a run from before owners, with
no label) is `404` on read, results and dismiss, as if it didn't exist. pygeoapi never passes the
request to the manager, so the service hands the owner over in a context variable
([caller.py](../src/hydrowf_ogc/caller.py)), set inside the worker thread that runs the call. A
middleware in front of pygeoapi's own Starlette app would not work: it calls the API through
`loop.run_in_executor`, which does not carry context variables into its threads. A manager reached
without an owner (e.g. under `pygeoapi serve`) refuses.

Inputs are checked too, since Argo reads them with the cluster's credentials: a file or directory
input must be an output of one of the caller's own jobs, or under a `shared_inputs` prefix of the
manager config (`[hydrofabric/]` here). A key in another bucket, or with characters other than
`A-Z a-z 0-9 . _ / -`, is `400`; any other key is `403`, and nothing is submitted. An input is one
output (`<job>/<pod>/<output>`, a `.tgz`, a file or a directory ending in `/`) or something below a
shared prefix: never a whole job, a whole step or a whole shared prefix, which would stage all of it
into one pod. An output also has a type: its run records the data lego each pipeline output was written
as (the `hydrowf/formats` annotation, hydrowf-core), and an output recorded as another data lego than the
input takes is `400` (e.g. an `ngen-cng` folder where an `ngen-run` is wanted). A file inside an output,
a shared key and an output of a run from before formats have no recorded type and are not checked.

## Dismissing a job

`DELETE /ogc/jobs/{id}` answers 200 at once; the job goes away shortly after. A running job is
terminated first (its pods stop), then deleted; Argo removes its outputs (artifact GC) and then the
workflow. On kind, a job dismissed while running was gone, outputs included, in about 35-45 s. Until
then it still reads as the caller's job; poll until `GET /ogc/jobs/{id}` is 404 to be sure.

## Retention

A finished job is kept for `connection.retention.days` (30 if unset), then Argo deletes its workflow
(`ttlStrategy`). Deleting a workflow deletes its outputs and archived logs from the bucket too
(`artifactGC: OnWorkflowDeletion`, which runs a small Argo pod), so expiry and `DELETE /ogc/jobs/{id}`
both free the space. The clock starts when a job finishes, so a running job is never expired;
an *input* it takes from another of the caller's jobs can be, though (see Known gaps).
Workflows submitted before this have neither setting; remove them, and their `<job id>/` keys,
by hand.

On a new cluster, the cleanup pod needs what Argo's quick-start gives the `default` service account
in kind: a Role on `workflowartifactgctasks` (`list`, `watch`) and `workflowartifactgctasks/status`
(`patch`) bound to the workflows' service account, and S3 delete rights through the artifact
repository's credentials (or the workload identity, when there are no keys). If the cleanup fails,
the workflow stays, being deleted (`ArtifactGCError` in its conditions), and still shows in the job
list: fix the cause, then remove its `workflows.argoproj.io/artifact-gc` finalizer and delete the
`<job id>/` keys by hand. `forceFinalizerRemoval` is left off on purpose, so a failed cleanup is
seen rather than leaving files behind unnoticed.

## Resources

Each step reserves its tool's `cpu` and `ram_mb` (requests), and its memory is capped there
(limits): a step that outgrows `ram_mb` is OOMKilled and fails alone, instead of running the node
out of memory under other users' steps. CPU is not capped, so a step may use idle cores; when cores
are short, Kubernetes shares them by request. Raise a tool's `ram_mb` in `registry/legos.yaml` if
its step is OOMKilled.

Peaks measured on kind (calibration of gage-01031510, 2 years, 3 iterations; cgroup `memory.peak`,
so lower bounds for bigger domains):

| Step | Peak | Limit (`ram_mb`) |
|---|---|---|
| preprocess | 2827 MiB | 16384 MiB |
| calibrate | 1723 MiB | 4096 MiB |
| run | 887 MiB | 2048 MiB |
| to-cng | 1430 MiB | 2048 MiB |

## Bearer tokens (Keycloak)

Set `HYDROWF_OIDC_ISSUER` (the realm URL, e.g. `https://<keycloak>/realms/hydrowf`) and
`HYDROWF_OIDC_AUDIENCE` (`hydrowf-api`); an issuer without an audience stops the service at startup.
[hydrowf_core.auth](https://github.com/Aquaveo/hydrowf-core/blob/main/src/hydrowf_core/auth.py) checks the RS256 signature against the realm's keys
(found through OIDC discovery, re-read every 5 minutes), and `iss`, `aud`, `exp` and `sub`, allowing
30 s of clock skew. The keys are fetched on the first token, in a worker thread like the rest of the
request.

Realm rules:

- `hydrowf-api` is an API-only client nobody logs in through, so ID tokens never carry its audience.
- Every client that calls `/ogc` needs an **audience mapper** adding `hydrowf-api` to its access
  tokens; without it Keycloak's tokens carry only `account` and get 401.

Users log in to Keycloak through the CIROH portal as an upstream identity provider; Keycloak issues
the tokens `/ogc` checks, so `/ogc` never calls another cluster per request.

## Verified on kind

The service image (Python 3.12, pygeoapi 0.24.0, Starlette 1.10.0, uvicorn 0.54.0), Keycloak 26.0.7
tokens for two clients A and B (client credentials, audience mapper):

| Request | Answer |
|---|---|
| anonymous `GET /ogc`, `/ogc/processes`, `/ogc/processes/ngiab-simulation` | 200; every link carries `PYGEOAPI_SERVER_URL` |
| anonymous `GET /ogc/jobs` | 401, `WWW-Authenticate: Bearer` |
| `GET /ogc/jobs` or `/ogc/processes` with a garbage bearer token | 401 each |
| `GET /ogc/processes` with `Authorization: Basic …` | 200 |
| `POST /ogc/processes` | 405 |
| A executes with the shared hydrofabric | 201; A sees and lists the job, B doesn't list it |
| B `GET`, `GET …/results`, `DELETE` on A's job | 404 each |
| B executes with an output of A's job as an input | 403 |
| A executes with `{{workflow.name}}` as the feature | 400 |
| A `GET …/results` of a finished job | 200; presigned links |
| A `GET …/credentials` of a finished job (B: the same job) | 200, `no-store`, keys scoped to its 2 outputs, 15 min (B: 404) |
| xarray + s3fs with only those keys | opens `timeseries/troute.zarr` in place and reads it; listing another prefix or the bucket, and writing, are denied |
| A `DELETE` on A's job while its first step runs | 200; the workflow is terminated, then gone in about 35 s, 0 objects left |
| `/ogc/openapi` | `servers` names the running `PYGEOAPI_SERVER_URL` |
| anonymous `POST …/execution` with a 2 MB body | 401 (refused before the body is read) |
| `POST …/execution` with a token: a 2 MB body, chunked or not / a 2 MB chunked body declaring `Content-Length: 10` | 413 / 413 |
| anonymous `GET /ogc/processes` with a 3 MB chunked body | 200, the body never read |
| anonymous `GET /ogc/jobs?f=bogus` | 401 (authentication before the format check) |
| a garbage bearer token / no token | `WWW-Authenticate: Bearer error="invalid_token"` / `Bearer` |

Verified earlier through the Tethys portal, with the same manager code:

| Request | Answer |
|---|---|
| `GET /ogc/jobs/{id}` | accepted, running, successful, following the workflow's phase |
| B executes with `hydrofabric/../<A's job>/…`, or a key with `{{…}}` | 400, nothing submitted |
| B executes with `s3://other-bucket/hydrofabric/…` | 400 |
| execute (`retention: {days: 30}`) | `ttlStrategy.secondsAfterCompletion: 2592000` and `artifactGC: OnWorkflowDeletion` |
| `DELETE` on a finished job with 4 objects under `<job id>/` | the workflow and all 4 objects are gone |
| calibration with memory limits on every step | succeeded in 16 min; no step OOMKilled |
| a workflow with `secondsAfterCompletion: 15` (probe) | deleted by Argo about 40 s after it finished, its objects with it |

## Known gaps

- **No storage quota yet.** Space is bounded only by `retention.days` (30 by default). A per-user
  quota (50 GB: evict the caller's oldest finished jobs, never running ones or one this run reads,
  then refuse) is planned.
- **Groups and admins.** A caller sees only their own jobs; sharing with a Keycloak group, or an
  admin who sees everyone's, comes with the authorization work.
- **Only public pipelines.** With `registry` set, `/ogc` offers the registry's public pipelines only
  (read anonymously); a caller's private pipelines would need the registry asked with their token.
- **New pipelines wait up to `ttl`** (60 s by default) to appear, and the OpenAPI document lists the
  processes of the moment the service started.
- **`<id>` follows the latest version.** A caller who read `ngiab-simulation`'s description and then
  executes it may run a newer version published in between; each run records the version it ran (the
  workflow's `hydrowf/version` label). Pin `<id>@<version>` to be exact.
- **A job's `processID` is the pipeline id**, not the `<id>@<version>` it was started with; the version
  is in the workflow's `hydrowf/version` label.
- **The baked-in config is kind's.** A cluster that forgets to mount its own runs against
  `localhost` with TLS checks off; the new cluster's config and manifests (hydrowf-deploy) mount theirs.
- **Base images are pinned by tag**, not digest; pin them when CI builds this image.
- **Items live in job results only.** There is no STAC catalog or search yet: an Item is reached through
  its job, and only by the job's owner. Its download links expire like the others; its `s3`
  alternates last until the job is deleted. A bbox that crosses the antimeridian is refused.
- **Read keys are verified on RustFS (kind), not yet on AWS S3.** A session may list only under the job's
  outputs, and S3 answers a read of a *missing* key with 404 only to callers that may list the bucket; AWS
  may answer 403 instead, which s3fs and Zarr treat as an error, not as "missing". Zarr leaves empty chunks
  out. Test a sparse Zarr (and `consolidated=False`) on real S3 before EKS relies on this.
- **Runs from before formats follow the data lego's latest version.** A run records the data lego
  version of each output (`hydrowf/formats`), and its Item follows that version. Older runs, and runs
  whose recorded version is no longer offered, use the latest STAC version of the data lego instead.
- **Inputs are job outputs or shared data.** There are no per-user uploads yet, and a deleted or
  expired job's outputs can no longer be passed as inputs.
- **An input can expire before it is read.** Argo fetches a step's inputs when its pod starts, which
  can be much later than the submit. If the job that made an input reaches `retention.days` in
  between, the step fails to load it. The planned quota keeps the jobs a running job reads.
- **Workflows from before owners** have no `hydrowf/owner` label: nobody can see or dismiss them
  through `/ogc`. Remove them with `argo delete` or a TTL.
- **CORS.** A browser client on another origin needs CORS headers on `/ogc` first; a preflight
  `OPTIONS` is 405 today.

## Deploying

The Kubernetes manifests (Deployment, Service, the `/ogc` ingress path, the OIDC and S3 settings as
a Secret) belong in hydrowf-deploy, with the new cluster.
