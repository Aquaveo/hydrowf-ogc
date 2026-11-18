"""The OGC API - Processes routes under /ogc, for Tethys (ADDITIONAL_URLPATTERNS).

Tethys puts these ahead of its own routes, at the root. Only the landing page and the Processes and
Jobs endpoints of pygeoapi.django_.urls: no collections, tiles or admin. Without trailing slashes,
because pygeoapi builds every link as server.url + "/processes" etc. (server.url ends in /ogc).
"""
from collections.abc import Callable
from functools import wraps

from django.http import HttpRequest, HttpResponse, HttpResponseNotAllowed, JsonResponse
from django.urls import path
from pygeoapi.django_ import views

from hydrowf_platform.caller import acting_as, owner_from
from hydrowf_platform.tethys import auth

View = Callable[..., HttpResponse]
SAFE_METHODS = ("GET", "HEAD", "OPTIONS")


def unauthorized(message: str = "Authentication required") -> JsonResponse:
    return JsonResponse({"error": message}, status=401)


def bearer_token(request: HttpRequest) -> str | None:
    """The token of an `Authorization: Bearer <token>` header; None when the request has no bearer
    credential (no header, or another scheme). A bearer header without a token gives "", which
    guard() refuses: a malformed credential is never read as no credential. Only extracts:
    auth.BearerAuth validates."""
    parts = request.headers.get("Authorization", "").split(None, 1)    # any whitespace; [] without one
    if not parts or parts[0].lower() != "bearer":
        return None
    return parts[1].strip() if len(parts) > 1 else ""


def guard(view: View, methods: tuple[str, ...] = SAFE_METHODS, open_reads: bool = False) -> View:
    """`view` as Tethys serves it: the methods it takes and who may call it.

    - A method not in `methods`: 405, before anything else. pygeoapi's views don't check the method
      (process_execution executes the body of a GET), so each route lists what it takes.
    - Authorization: Bearer <token>: a valid OIDC access token calls the view acting as the token's
      owner (its `sub`, see hydrowf_platform.caller), with its claims in request.oidc_claims. Any other
      bearer token is a 401, never a fallback to the open rules.
    - GET, HEAD, OPTIONS on `open_reads` routes (the landing page and the process catalogue): open.
    - Anything else: 401. Tethys sessions are not accepted: jobs belong to a Keycloak user, and a
      session has no Keycloak identity.

    Every wrapper is csrf_exempt: no route reads the session cookie, and a cross-site page can't set
    the Authorization header, so there is nothing for CSRF to forge.
    """
    @wraps(view)
    def wrapper(request, *args, **kwargs):
        if request.method not in methods:
            return HttpResponseNotAllowed(methods)
        token = bearer_token(request)
        if token is not None:
            bearer = auth.bearer_auth()
            claims = bearer.claims(token) if bearer else None
            owner = owner_from(claims) if claims else None
            if owner is None:
                return unauthorized("Invalid bearer token")
            request.oidc_claims = claims
            with acting_as(owner):
                return view(request, *args, **kwargs)
        if open_reads and request.method in SAFE_METHODS:
            return view(request, *args, **kwargs)
        return unauthorized()

    wrapper.csrf_exempt = True
    return wrapper


urlpatterns = [
    path("ogc", guard(views.landing_page, open_reads=True)),
    path("ogc/processes", guard(views.processes, open_reads=True)),
    path("ogc/processes/<str:process_id>", guard(views.processes, open_reads=True)),
    # Jobs and their results (presigned download links) belong to a caller: no open_reads
    path("ogc/processes/<str:process_id>/execution", guard(views.process_execution, ("POST",))),
    path("ogc/jobs", guard(views.jobs)),
    path("ogc/jobs/<str:job_id>", guard(views.jobs, SAFE_METHODS + ("DELETE",))),
    path("ogc/jobs/<str:job_id>/results", guard(views.job_results)),
]
