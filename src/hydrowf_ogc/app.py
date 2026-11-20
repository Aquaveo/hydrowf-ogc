"""hydrowf-ogc: the OGC API - Processes as a service, pygeoapi's Processes and Jobs on Starlette.

    uvicorn --factory hydrowf_ogc.app:app_from_env --host 0.0.0.0 --port 8000 --proxy-headers

Reads PYGEOAPI_CONFIG like pygeoapi does (the OpenAPI document is generated from it at startup, so it
names the running PYGEOAPI_SERVER_URL, and the processes of that moment), and HYDROWF_OIDC_ISSUER and
HYDROWF_OIDC_AUDIENCE for bearer tokens (hydrowf_core.auth; without an issuer, only the open reads work).
The routes live under the path of server.url (http://<host>/ogc serves /ogc/processes etc.), since
pygeoapi builds every link as server.url + "/processes":

    GET    /  /conformance  /openapi  /processes  /processes/{id}   open: anyone may read the catalogue
    POST   /processes/{id}/execution                                 a bearer token: the job is theirs
    GET    /jobs  /jobs/{id}  /jobs/{id}/results                     a bearer token: their jobs only
    DELETE /jobs/{id}

API and its manager are built once, at startup (a bad manager config stops the service there).
A request is authenticated before anything else is read; only a POST's body is read at all, counted as
it streams in and refused past MAX_BODY bytes (no header is trusted for its size). Every request
runs pygeoapi's handler in a worker thread, as the token's owner (hydrowf_ogc.caller): the
owner is handed to the thread, not left in a context variable, because pygeoapi's own Starlette app
(loop.run_in_executor) does not carry context variables into its threads. That is also why this is
not pygeoapi.starlette_app with a middleware in front: it builds its API at import, serves collections,
tiles and STAC, and would lose the owner on the way to the manager.
"""
from __future__ import annotations

import copy
from contextlib import aclosing
from collections.abc import Callable
from http import HTTPStatus
from urllib.parse import urlparse

import pygeoapi.api as core_api
import pygeoapi.api.processes as processes_api
from pygeoapi.api import API, APIRequest, apply_gzip
from pygeoapi.config import get_config
from pygeoapi.openapi import get_oas
from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, Response
from starlette.routing import Route

from hydrowf_core.auth import BearerAuth, bearer_from_env, bearer_token
from hydrowf_ogc.caller import acting_as, owner_from

Result = tuple[dict, int, str | bytes]                 # what pygeoapi's API functions return
ApiFunction = Callable[..., Result]                    # (api, api_request, *path params) -> Result
MAX_BODY = 1024 * 1024                                 # bytes; an execute request is a few hundred


class Unauthorized(Exception):
    """The request may not go on: a 401 with this message. `token_sent`: it carried a bearer token
    that isn't valid for us (RFC 6750's invalid_token), rather than none."""

    def __init__(self, message: str, token_sent: bool = False):
        super().__init__(message)
        self.token_sent = token_sent


def caller_for(authorization: str | None, bearer: BearerAuth | None, open_reads: bool) -> str | None:
    """Who a request acts as: the owner id of its bearer token, or None for an anonymous open read.

    `authorization` is the request's Authorization header (None without one), `bearer` the token
    checker (None when bearer tokens are off), `open_reads` whether the route is a catalogue read.
    Raises Unauthorized when the request may not go on. Runs in a worker thread: bearer.claims may
    fetch the issuer's keys.
    """
    token = bearer_token(authorization)
    if token is None:                          # no bearer credential: no header, or another scheme
        if open_reads:
            return None
        raise Unauthorized("Authentication required")
    if bearer is None:                         # bearer tokens are off: no token can be valid
        raise Unauthorized("Invalid bearer token", token_sent=True)
    claims = bearer.claims(token) if token else None    # "Bearer" alone: malformed, never anonymous
    owner = owner_from(claims) if claims else None      # claims is None for any token not valid for us
    if owner is None:
        raise Unauthorized("Invalid bearer token", token_sent=True)
    return owner


def unauthorized(err: Unauthorized) -> JSONResponse:
    challenge = 'Bearer error="invalid_token"' if err.token_sent else "Bearer"
    return JSONResponse({"error": str(err)}, status_code=401, headers={"WWW-Authenticate": challenge})


class BodyTooLarge(Exception):
    """A body past MAX_BODY bytes: a 413."""


async def read_body(request: Request) -> bytes:
    """The request's body, counted as it streams in: BodyTooLarge as soon as it passes MAX_BODY.
    Content-Length is not trusted: a chunked body has none, and h11 lets Transfer-Encoding override it."""
    chunks, size = [], 0
    async with aclosing(request.stream()) as stream:    # closed when we stop early, like Starlette's body()
        async for chunk in stream:
            size += len(chunk)
            if size > MAX_BODY:
                raise BodyTooLarge
            chunks.append(chunk)
    return b"".join(chunks)


def to_response(headers: dict, status: int, content: str | bytes | dict) -> Response:
    """pygeoapi's (headers, status, content) as a Starlette response, gzipped when it asked for it."""
    if status != HTTPStatus.NO_CONTENT:                # a 204 has no body, not even gzip's header
        content = apply_gzip(headers, content)
    if isinstance(content, dict):
        response = JSONResponse(content, status_code=status)
    elif headers.get("Content-Type") == "text/html":
        response = HTMLResponse(content, status_code=status)
    else:
        response = Response(content, status_code=status)
    response.headers.update(headers)
    return response


def endpoint(api: API, bearer: BearerAuth | None, functions: dict[str, ApiFunction], open_reads: bool):
    """The Starlette endpoint that runs functions[method] for the caller its token names."""
    def call(function: ApiFunction, api_request: APIRequest, args: tuple, owner: str | None) -> Result:
        if owner is None:
            return function(api, api_request, *args)
        with acting_as(owner):                         # set in the worker thread that runs the call
            return function(api, api_request, *args)

    async def handle(request: Request) -> Response:
        try:                                           # first: nothing is read for a caller we refuse
            owner = await run_in_threadpool(caller_for, request.headers.get("Authorization"), bearer, open_reads)
        except Unauthorized as err:                    # (a thread: the token check may fetch the issuer's keys)
            return unauthorized(err)
        api_request = APIRequest(request, api.locales)  # not from_starlette: it reads any body, unbounded
        if request.method == "POST":                   # the only route that takes a body (execution)
            try:
                api_request._data = await read_body(request)  # what from_starlette sets; b"" otherwise
            except BodyTooLarge:
                return JSONResponse({"error": f"the body is over {MAX_BODY} bytes"}, status_code=413)
        if not api_request.is_valid():                 # an unsupported ?f= or Accept
            return to_response(*api.get_format_exception(api_request))
        method = "GET" if request.method == "HEAD" else request.method
        args = tuple(request.path_params.values())     # process_id or job_id, in path order
        return to_response(*await run_in_threadpool(call, functions[method], api_request, args, owner))

    return handle


def create_app(config: dict, openapi: dict, bearer: BearerAuth | None) -> Starlette:
    """The service for a pygeoapi config (its manager is built here, once) and token checker."""
    api = API(config, openapi)
    prefix = urlparse(config["server"]["url"]).path.rstrip("/")

    def route(path: str, functions: dict[str, ApiFunction], open_reads: bool = False) -> Route:
        return Route(prefix + path or "/", endpoint(api, bearer, functions, open_reads), methods=list(functions))

    app = Starlette(routes=[
        route("", {"GET": core_api.landing_page}, open_reads=True),
        route("/conformance", {"GET": core_api.conformance}, open_reads=True),
        route("/openapi", {"GET": core_api.openapi_}, open_reads=True),
        route("/processes", {"GET": processes_api.describe_processes}, open_reads=True),
        route("/processes/{process_id}", {"GET": processes_api.describe_processes}, open_reads=True),
        # Jobs and their results (presigned download links) belong to a caller: no open reads
        route("/processes/{process_id}/execution", {"POST": processes_api.execute_process}),
        route("/jobs", {"GET": processes_api.get_jobs}),
        route("/jobs/{job_id}", {"GET": processes_api.get_jobs, "DELETE": processes_api.delete_job}),
        route("/jobs/{job_id}/results", {"GET": processes_api.get_job_result}),
    ])
    app.state.api = api
    return app


def app_from_env() -> Starlette:
    """uvicorn's factory: the config PYGEOAPI_CONFIG names, and its OpenAPI document generated now,
    so the document's server URL and processes are the running service's, not the image build's."""
    config = get_config()
    return create_app(config, get_oas(copy.deepcopy(config)), bearer_from_env())
