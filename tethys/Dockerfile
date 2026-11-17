# A Tethys portal (tethys-uvx: uv, Python 3.12, uvicorn) serving the OGC API - Processes at /ogc.
#
#   docker build -f tethys/Dockerfile -t hydrowf-tethys .          # from the repository root
#
# The portal reads Argo through ArgoManager, configured by pygeoapi-config.yml (HYDROWF_ARGO_HOST,
# HYDROWF_ARGO_TOKEN override it). Provision once per release, then serve:
#
#   docker run --rm --network host -v hydrowf-tethys:/home/tethys/persist $ENV hydrowf-tethys provision.sh
#   docker run --rm --network host -v hydrowf-tethys:/home/tethys/persist $ENV hydrowf-tethys
#
# with ENV="-e TETHYS_SECRET_KEY=... -e TETHYS_DB_ENGINE=django.db.backends.sqlite3" (SQLite in the
# volume; drop TETHYS_DB_ENGINE and set TETHYS_DB_HOST/USERNAME/PASSWORD for PostgreSQL).

ARG TETHYS_UVX=v0.1.0

FROM ghcr.io/aquaveo/tethys-uvx:builder-${TETHYS_UVX} AS builder

# The locked dependencies first (a layer that changes only with uv.lock), then the project itself
COPY pyproject.toml uv.lock README.md /src/
RUN cd /src && uv export --frozen --no-dev --no-emit-project -o /tmp/requirements.txt \
    && uv pip install -r /tmp/requirements.txt
COPY src /src/src
RUN uv pip install --no-deps /src

# pygeoapi's config, the lego registry, and the OpenAPI document generated from them
COPY pygeoapi-config.yml /opt/hydrowf/
COPY registry /opt/hydrowf/registry
ENV PYGEOAPI_CONFIG=/opt/hydrowf/pygeoapi-config.yml \
    PYGEOAPI_OPENAPI=/opt/hydrowf/pygeoapi-openapi.yml \
    HYDROWF_REGISTRY=/opt/hydrowf/registry/legos.yaml \
    PYGEOAPI_SERVER_URL=http://localhost:8000/ogc
RUN pygeoapi openapi generate $PYGEOAPI_CONFIG --output-file $PYGEOAPI_OPENAPI


FROM ghcr.io/aquaveo/tethys-uvx:runtime-base-${TETHYS_UVX}

COPY --from=builder /opt/python /opt/python
COPY --from=builder /opt/conda /opt/conda
COPY --from=builder /opt/hydrowf /opt/hydrowf
COPY tethys/portal_config.yml /config/portal_config.yml
ENV PYGEOAPI_CONFIG=/opt/hydrowf/pygeoapi-config.yml \
    PYGEOAPI_OPENAPI=/opt/hydrowf/pygeoapi-openapi.yml \
    HYDROWF_REGISTRY=/opt/hydrowf/registry/legos.yaml \
    PYGEOAPI_SERVER_URL=http://localhost:8000/ogc
