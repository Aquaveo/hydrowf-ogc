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
2. **Bearer token** (`Authorization: Bearer <token>`): a valid OIDC access token calls the view, with
   its claims in `request.oidc_claims`; any other bearer token is `401`, never a fallback to the rules
   below.
3. **Reads** (`GET`, `HEAD`, `OPTIONS`) are open.
4. **Writes** need a logged-in Tethys session that passes Django's CSRF check (`403` otherwise, logged
   on `django.security.csrf`), else `401`. Every route is `csrf_exempt` so the middleware leaves this to
   `guard()`: pygeoapi exempts `process_execution` itself, which would leave session executes unchecked.

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
  `server.url`, Tethys's login and middleware apply directly.
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
| `POST /ogc/processes/ngiab-simulation/execution`, logged in, `Prefer: respond-async` | 201, `Location: …/ogc/jobs/{id}`; the Argo workflow is named after the job |
| `GET /ogc/jobs/{id}` | accepted, running, successful, following the workflow's phase |
| `GET /ogc/jobs/{id}/results` | the `run` and `cng` outputs as `s3://` links |
| anonymous `POST …/execution` | 401 |
| anonymous `GET …/execution` with a JSON body | 405 |
| logged-in session `POST …/execution` without `X-CSRFToken` | 403 |
| logged-in `DELETE /ogc/jobs/{id}` with `X-CSRFToken` | reaches pygeoapi (404 for an unknown job) |
| Keycloak 26.0.7 token (client credentials, audience mapper), `POST …/execution` and `GET /ogc/jobs/{id}` | 201 with `Location`; status follows Argo |
| tampered token | 401 |

## Known gaps

- **Authorization.** Any valid token or logged-in user may execute, and dismiss any job. Who owns a
  job (the token's `sub` as an Argo workflow label) and what groups may do comes with the
  authorization work.
- **A broken URL module fails quietly.** Tethys logs and skips an `ADDITIONAL_URLPATTERNS` entry it
  cannot import, so a syntax error in `urls.py` shows up as a 404 on `/ogc`, not a failed start.
  `tests/test_tethys.py` imports it, so CI catches it.
- **Static files.** `provision.sh` collects static files inside the provisioning container, not the
  persist volume, so a separately started portal does not have them. The `/ogc` endpoints return JSON
  and don't need them; check it with the tethys-app Helm chart's provisioning Job.

## Deploying

The tethys-app Helm chart (`oci://ghcr.io/aquaveo/charts/tethys-app`, 0.1.5) deploys an image like this
one as a single app at the root of a domain, with a provisioning Job and optional bundled PostgreSQL.
Use PostgreSQL there; SQLite is for a single local container.
