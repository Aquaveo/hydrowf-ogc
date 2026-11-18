"""The OGC API - Processes routes under /ogc, for Tethys (ADDITIONAL_URLPATTERNS).

Tethys puts these ahead of its own routes, at the root. Only the landing page and the Processes and
Jobs endpoints of pygeoapi.django_.urls: no collections, tiles or admin. Without trailing slashes,
because pygeoapi builds every link as server.url + "/processes" etc. (server.url ends in /ogc).
"""
import logging
from collections.abc import Callable
from functools import wraps

from django.http import HttpRequest, HttpResponse, HttpResponseNotAllowed, JsonResponse
from django.middleware.csrf import CsrfViewMiddleware
from django.urls import path
from pygeoapi.django_ import views

from hydrowf_platform.tethys import auth

LOGGER = logging.getLogger("django.security.csrf")    # where CsrfViewMiddleware logs its rejections

View = Callable[..., HttpResponse]
SAFE_METHODS = ("GET", "HEAD", "OPTIONS")


def unauthorized(message: str = "Authentication required") -> JsonResponse:
    return JsonResponse({"error": message}, status=401)


def bearer_token(request: HttpRequest) -> str | None:
    """The token of an `Authorization: Bearer <token>` header; None when the request has no bearer
    credential (no header, or another scheme, so the session decides). A bearer header without a
    token gives "", which guard() refuses: a malformed credential is never read as no credential.
    Only extracts: auth.BearerAuth validates."""
    parts = request.headers.get("Authorization", "").split(None, 1)    # any whitespace; [] without one
    if not parts or parts[0].lower() != "bearer":
        return None
    return parts[1].strip() if len(parts) > 1 else ""


class CsrfCheck(CsrfViewMiddleware):
    """Django's CSRF check on demand: _reject logs like the middleware and returns the reason."""

    def _reject(self, request, reason):
        LOGGER.warning("Forbidden (%s): %s", reason, request.path)
        return reason


def csrf_failure(request: HttpRequest) -> str | None:
    """Why the request fails Django's CSRF check (the same one CsrfViewMiddleware runs); None if it passes."""
    check = CsrfCheck(lambda request: None)
    check.process_request(request)
    return check.process_view(request, None, (), {})


def guard(view: View, methods: tuple[str, ...] = SAFE_METHODS, open_reads: bool = False) -> View:
    """`view` as Tethys serves it: the methods it takes, who may call it, and CSRF for cookie sessions.

    - A method not in `methods`: 405, before anything else. pygeoapi's views don't check the method
      (process_execution executes the body of a GET), so each route lists what it takes.
    - Authorization: Bearer <token>: a valid OIDC access token calls the view (no CSRF: a script set
      the header, a browser doesn't send it by itself); its claims go to request.oidc_claims. Any other
      bearer token is a 401, never a fallback to the anonymous rules.
    - GET, HEAD, OPTIONS: open on `open_reads` routes (the landing page and the process catalogue).
      Elsewhere, as every other method: closed unless a route opts in, so a new route starts private.
    - Otherwise a logged-in Tethys session (else 401); a write must also pass Django's CSRF check (403
      otherwise), which reads always pass.

    Every wrapper is csrf_exempt, so CsrfViewMiddleware leaves the decision to guard(): pygeoapi exempts
    process_execution itself (views.py), which would leave session executes unchecked.
    """
    @wraps(view)
    def wrapper(request, *args, **kwargs):
        if request.method not in methods:
            return HttpResponseNotAllowed(methods)
        token = bearer_token(request)
        if token is not None:
            bearer = auth.bearer_auth()
            claims = bearer.claims(token) if bearer else None
            if claims is None:
                return unauthorized("Invalid bearer token")
            request.oidc_claims = claims
            return view(request, *args, **kwargs)
        if open_reads and request.method in SAFE_METHODS:
            return view(request, *args, **kwargs)
        if not request.user.is_authenticated:
            return unauthorized()
        if csrf_failure(request) is not None:                  # the reason is logged, not sent
            return JsonResponse({"error": "CSRF check failed"}, status=403)
        return view(request, *args, **kwargs)

    wrapper.csrf_exempt = True
    return wrapper


urlpatterns = [
    path("ogc", guard(views.landing_page, open_reads=True)),
    path("ogc/processes", guard(views.processes, open_reads=True)),
    path("ogc/processes/<str:process_id>", guard(views.processes, open_reads=True)),
    # Jobs and their results (presigned download links) need a caller: no open_reads
    path("ogc/processes/<str:process_id>/execution", guard(views.process_execution, ("POST",))),
    path("ogc/jobs", guard(views.jobs)),
    path("ogc/jobs/<str:job_id>", guard(views.jobs, SAFE_METHODS + ("DELETE",))),
    path("ogc/jobs/<str:job_id>/results", guard(views.job_results)),
]
