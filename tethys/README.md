# The OGC API - Processes in a Tethys portal

A Tethys portal built on [tethys-uvx](https://github.com/Aquaveo/tethys-uvx) (uv, Python 3.12, Django 5.2,
uvicorn) that serves the hydrowf processes and jobs endpoints at `/ogc`. The portal has no Tethys app:
pygeoapi's own Django views run inside the Tethys process, and ArgoManager reads and writes Argo as it
does under `pygeoapi serve`.

```
client ──HTTP──▶ uvicorn (Tethys / Django)
                   └─ /ogc/* → guard() → pygeoapi.django_.views → pygeoapi.api → ArgoManager ──▶ Argo
```

## Run it on the local kind cluster

From the repository root, with Argo reachable at `https://localhost:2746` (for example
`kubectl -n argo port-forward svc/argo-server 2746:2746`):

```bash
docker build -f tethys/Dockerfile -t hydrowf-tethys .

cat > tethys.env <<EOF
TETHYS_SECRET_KEY=$(python3 -c 'import secrets; print(secrets.token_urlsafe(40))')
TETHYS_DB_ENGINE=django.db.backends.sqlite3
PORTAL_SUPERUSER_PASSWORD=<choose one>
EOF

# once per release: migrations, static files, superuser
docker run --rm --network host -v hydrowf-tethys:/home/tethys/persist --env-file tethys.env hydrowf-tethys provision.sh
# serve on http://localhost:8000
docker run --rm --network host -v hydrowf-tethys:/home/tethys/persist --env-file tethys.env hydrowf-tethys
```

SQLite lives in the `hydrowf-tethys` volume. For PostgreSQL, drop `TETHYS_DB_ENGINE` and set
`TETHYS_DB_HOST`, `TETHYS_DB_USERNAME` and `TETHYS_DB_PASSWORD`. `HYDROWF_ARGO_HOST` and
`HYDROWF_ARGO_TOKEN` point ArgoManager at another Argo server; `PYGEOAPI_SERVER_URL` must be the
public URL of `/ogc`, since pygeoapi builds every link from it.

Job results are `s3://` links unless the manager config has a `downloads` section
([downloads.py](../src/hydrowf_platform/downloads.py)); with it, each result's `href` is a presigned
HTTPS GET that expires, and `s3` keeps the address another pipeline takes as an input. The keys come
from `HYDROWF_S3_ACCESS_KEY` and `HYDROWF_S3_SECRET_KEY` (a read-only key is enough), and
`HYDROWF_S3_PUBLIC_ENDPOINT` overrides the endpoint. A section without an endpoint or a key stops the
portal at startup. Only `guard()` keeps the links from anonymous callers, so never turn `downloads` on
under `pygeoapi serve`, which has no guard. The shipped config leaves it off; on kind, mount
a copy with `downloads: {endpoint: http://localhost:9000}` over `/opt/hydrowf/pygeoapi-config.yml`
and port-forward `svc/minio` to 9000.

## How it is wired

Two keys in [portal_config.yml](portal_config.yml), under `settings.TETHYS_PORTAL_CONFIG`:

| Key | Module | What it does |
|---|---|---|
| `ADDITIONAL_SETTINGS_FILES` | `hydrowf_platform.tethys.settings` | Copies `PYGEOAPI_CONFIG`, `OPENAPI_DOCUMENT` and `API_RULES` into Tethys's settings; pygeoapi's views read them from `django.conf.settings` |
| `ADDITIONAL_URLPATTERNS` | `hydrowf_platform.tethys.urls.urlpatterns` | Mounts the landing page and the processes and jobs views at `/ogc`, ahead of Tethys's own routes |

They are two modules because Tethys imports them at different times: the settings while its own
settings load, the URLs once Django is configured. pygeoapi's views read `django.conf.settings` when
they are imported, so the settings module must not import them.

`guard()` wraps every view and decides, in order:

1. **Method**: each route lists the methods it takes (`/execution` is `POST` only; `/jobs/{id}` adds
   `DELETE`; the rest are `GET`, `HEAD`, `OPTIONS`). Anything else is `405`. pygeoapi's views don't
   check the method: `process_execution` would execute the body of a `GET`.
2. **Bearer token** (`Authorization: Bearer <token>`): a valid OIDC access token calls the view acting
   as the token's owner (its `sub`), with its claims in `request.oidc_claims`; any other bearer token,
   or a `sub` that can't be an Argo label value, is `401`, never a fallback to the rules below.
3. **Reads** (`GET`, `HEAD`, `OPTIONS`) are open only on the routes that opt in with `open_reads`: the
   landing page and the process catalogue. Jobs and their results (presigned download links) are not.
4. **Everything else** is `401`. Tethys sessions are not accepted: a job belongs to a Keycloak user,
   and a session has no Keycloak identity. Every route is `csrf_exempt`: none reads the session
   cookie, and a cross-site page can't set the `Authorization` header.

## Job owners

Each run belongs to the token's `sub`: execute labels the Argo workflow `hydrowf/owner=<sub>`, the
job list shows only the caller's runs, and another caller's job (or a run from before owners, with
no label) is `404` on read, results and dismiss, as if it didn't exist. pygeoapi never passes the
request to the manager, so `guard()` hands the owner over in a context variable
([caller.py](../src/hydrowf_platform/caller.py)); a manager reached without one (e.g. under
`pygeoapi serve`) refuses.

## Bearer tokens (Keycloak)

Set `HYDROWF_OIDC_ISSUER` (the realm URL, e.g. `https://<keycloak>/realms/hydrowf`) and
`HYDROWF_OIDC_AUDIENCE` (`hydrowf-api`); without the issuer, bearer tokens are off, and an issuer
without an audience stops the portal at startup. [auth.py](../src/hydrowf_platform/tethys/auth.py)
checks the RS256 signature against the realm's keys (found through OIDC discovery, re-read every
5 minutes), and `iss`, `aud`, `exp` and `sub`, allowing 30 s of clock skew. An unreachable issuer is a
server error, not a 401.

Realm rules:

- `hydrowf-api` is an API-only client nobody logs in through, so ID tokens never carry its audience.
- Every client that calls `/ogc` needs an **audience mapper** adding `hydrowf-api` to its access
  tokens; without it Keycloak's tokens carry only `account` and get 401.

Users log in to Keycloak through the CIROH portal as an upstream identity provider; Keycloak issues
the tokens `/ogc` checks, so `/ogc` never calls another cluster per request.

## Why in-process, not a proxy

Two ways to put Tethys in front of the OGC API were compared:

- **(a) in-process** (this directory): Tethys imports pygeoapi's Django adapter. One service, one
  `server.url`, and Tethys's middleware applies directly (`guard()` decides who may call).
- **(b) proxy**: a login-protected Tethys view that streams requests to a separate `pygeoapi serve`.
  Two services, `Location` and `Prefer` headers to forward, and pygeoapi's `server.url` set to the
  proxied URL.

(a) works with no Tethys app and no changes to pygeoapi, so (b) only adds parts. Revisit (b) if pygeoapi
ever has to scale apart from the portal.

Single-app mode (`MULTIPLE_APP_MODE: false`) is not needed for `/ogc`: additional URL patterns are
mounted at the root in any mode. It matters once a client Tethys app is installed in the same portal.

## Verified on kind

tethys-uvx v0.1.0 (Tethys 4.6.2.dev8, Django 5.2.18, Python 3.12.15), pygeoapi 0.24.0:

| Request | Answer |
|---|---|
| `GET /ogc/processes`, `GET /ogc/processes/ngiab-simulation` | 200; every link carries `/ogc` |
| `POST /ogc/processes/ngiab-simulation/execution` with a token, `Prefer: respond-async` | 201, `Location: …/ogc/jobs/{id}`; the Argo workflow is named after the job |
| `GET /ogc/jobs/{id}` | accepted, running, successful, following the workflow's phase |
| `GET /ogc/jobs/{id}/results` | the `run` and `cng` outputs as `s3://` links; with `downloads`, presigned links (curl downloads `cng.tgz`; a changed or missing signature is 403) |
| anonymous `GET /ogc/jobs`, `/ogc/jobs/{id}`, `/ogc/jobs/{id}/results` | 401 |
| anonymous `POST …/execution` | 401 |
| anonymous `GET …/execution` with a JSON body | 405 |
| Keycloak 26.0.7 token (client credentials, audience mapper), `POST …/execution` and `GET /ogc/jobs/{id}` | 201 with `Location`; status follows Argo |
| tampered token | 401 |
| two clients (different `sub`s): A submits | the workflow carries `hydrowf/owner=<A's sub>`; A's `/ogc/jobs` lists it, B's is empty |
| B `GET`, `GET …/results`, `DELETE` on A's job | 404 each; the workflow is still there |
| A `DELETE` on A's job | 200; the workflow is gone |
| a workflow from before owners (no label) | 404 |

## Known gaps

- **Groups and admins.** A caller sees only their own jobs; sharing with a Keycloak group, or an
  admin who sees everyone's, comes with the authorization work.
- **Inputs are any S3 key.** A file input is any `s3://<bucket>/<key>`, and outputs are stored under
  `<job id>/<pod>/…`. A caller who knows another job's id (a uuid1, not a secret) could pass its
  outputs into their own run and download that. Closing it means checking input keys against the
  caller's own jobs or a per-user prefix.
- **Workflows from before owners** have no `hydrowf/owner` label: nobody can see or dismiss them
  through `/ogc`. Remove them with `argo delete` or a TTL.
- **A broken URL module fails quietly.** Tethys logs and skips an `ADDITIONAL_URLPATTERNS` entry it
  cannot import, so a syntax error in `urls.py` shows up as a 404 on `/ogc`, not a failed start.
  `tests/test_tethys.py` imports it, so CI catches it.
- **Static files.** `provision.sh` collects static files inside the provisioning container, not the
  persist volume, so a separately started portal does not have them. The `/ogc` endpoints return JSON
  and don't need them; check it with the tethys-app Helm chart's provisioning Job.
- **CORS.** A browser's preflight `OPTIONS` carries no credentials, so it gets 401 on the jobs routes.
  A browser client on another origin needs CORS headers on `/ogc` first.

## Deploying

The tethys-app Helm chart (`oci://ghcr.io/aquaveo/charts/tethys-app`, 0.1.5) deploys an image like this
one as a single app at the root of a domain, with a provisioning Job and optional bundled PostgreSQL.
Use PostgreSQL there; SQLite is for a single local container.
