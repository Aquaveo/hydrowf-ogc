# hydrowf-ogc: the OGC API - Processes service (hydrowf_platform.ogc) on uvicorn, port 8000.
#
#   docker build -t hydrowf-ogc .                                   # from the repository root
#   docker run --rm -p 8000:8000 -e HYDROWF_OIDC_ISSUER=... -e HYDROWF_OIDC_AUDIENCE=... hydrowf-ogc
#
# The baked-in pygeoapi-config.yml is the LOCAL KIND config (Argo at localhost, verify_ssl: false):
# every other cluster mounts its own over /opt/hydrowf/pygeoapi-config.yml (hydrowf-deploy) AND sets
# PYGEOAPI_SERVER_URL (the public URL, ending in /ogc; the localhost default goes into every link).
# The rest of the environment: HYDROWF_OIDC_ISSUER and HYDROWF_OIDC_AUDIENCE (bearer tokens),
# HYDROWF_ARGO_HOST and HYDROWF_ARGO_TOKEN (Argo), HYDROWF_S3_ACCESS_KEY, HYDROWF_S3_SECRET_KEY and
# HYDROWF_S3_PUBLIC_ENDPOINT (presigned result links), and FORWARDED_ALLOW_IPS
# (the ingress addresses whose X-Forwarded-* headers uvicorn trusts). The OpenAPI document is
# generated at startup from the config the service runs with.

ARG PYTHON=3.12-slim-bookworm
ARG UV=0.13.0

FROM ghcr.io/astral-sh/uv:${UV} AS uv

FROM python:${PYTHON} AS builder
COPY --from=uv /uv /usr/local/bin/uv
RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*                  # hydrowf-core is pinned by git revision
ENV UV_PROJECT_ENVIRONMENT=/opt/venv UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
WORKDIR /src

# The locked dependencies first (a layer that changes only with uv.lock), then the project itself
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable


FROM python:${PYTHON}
RUN useradd --system --uid 10001 --no-create-home hydrowf
COPY --from=builder /opt/venv /opt/venv
COPY pygeoapi-config.yml /opt/hydrowf/
COPY registry /opt/hydrowf/registry
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYGEOAPI_CONFIG=/opt/hydrowf/pygeoapi-config.yml \
    HYDROWF_REGISTRY=/opt/hydrowf/registry/legos.yaml \
    PYGEOAPI_SERVER_URL=http://localhost:8000/ogc
WORKDIR /opt/hydrowf
USER 10001
EXPOSE 8000
CMD ["uvicorn", "--factory", "hydrowf_platform.ogc:app_from_env", "--host", "0.0.0.0", "--port", "8000", \
     "--proxy-headers"]
