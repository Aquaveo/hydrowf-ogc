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

`guard()` wraps every view: `GET`, `HEAD` and `OPTIONS` are open; anything else needs a logged-in Tethys
user and answers `401` with a JSON body otherwise. Django's CSRF check stays on.

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
| anonymous `POST …/execution` | 401 (`guard()`) |
| anonymous `DELETE /ogc/jobs/{id}` | 403 (Tethys's `CsrfViewMiddleware`, before `guard()` runs) |
| logged-in `DELETE /ogc/jobs/{id}` with `X-CSRFToken` | reaches pygeoapi (404 for an unknown job) |

## Known gaps

- **Bearer tokens.** `guard()` sees only the Django session. Tethys's token and JWT authentication
  (`REST_FRAMEWORK` in Tethys's settings) applies to Django REST framework views only, and pygeoapi's
  views are plain Django functions, so a script sending `Authorization: Bearer <JWT>` gets 401. The
  plan: validate a Keycloak JWT against the realm's JWKS in `guard()`, and skip the CSRF check only for
  those requests. Browser login can use social-auth's `keycloak` backend, already in the image.
- **A broken URL module fails quietly.** Tethys logs and skips an `ADDITIONAL_URLPATTERNS` entry it
  cannot import, so a syntax error in `urls.py` shows up as a 404 on `/ogc`, not a failed start.
- **Static files.** `provision.sh` collects static files inside the provisioning container, not the
  persist volume, so a separately started portal does not have them. The `/ogc` endpoints return JSON
  and don't need them; check it with the tethys-app Helm chart's provisioning Job.

## Deploying

The tethys-app Helm chart (`oci://ghcr.io/aquaveo/charts/tethys-app`, 0.1.5) deploys an image like this
one as a single app at the root of a domain, with a provisioning Job and optional bundled PostgreSQL.
Use PostgreSQL there; SQLite is for a single local container.
