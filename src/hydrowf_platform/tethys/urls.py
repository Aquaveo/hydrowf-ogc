"""The OGC API - Processes routes under /ogc, for Tethys (ADDITIONAL_URLPATTERNS).

Tethys puts these ahead of its own routes, at the root. Only the landing page and the Processes and
Jobs endpoints of pygeoapi.django_.urls: no collections, tiles or admin. Without trailing slashes,
because pygeoapi builds every link as server.url + "/processes" etc. (server.url ends in /ogc).
"""
from collections.abc import Callable
from functools import wraps

from django.http import HttpResponse, JsonResponse
from django.urls import path
from pygeoapi.django_ import views

View = Callable[..., HttpResponse]
SAFE_METHODS = ("GET", "HEAD", "OPTIONS")


def guard(view: View) -> View:
    """`view` as Tethys serves it: reads are open, writes need a logged-in Tethys user (401 JSON otherwise).

    Django's CSRF check stays on: pygeoapi exempts process_execution from it (views.py), not jobs, so a
    session-authenticated DELETE /ogc/jobs/{id} must send X-CSRFToken (CsrfViewMiddleware answers 403
    before this wrapper runs).
    """
    @wraps(view)
    def wrapper(request, *args, **kwargs):
        if request.method in SAFE_METHODS or request.user.is_authenticated:
            return view(request, *args, **kwargs)
        return JsonResponse({"error": "Authentication required"}, status=401)
    return wrapper


urlpatterns = [
    path("ogc", guard(views.landing_page)),
    path("ogc/processes", guard(views.processes)),
    path("ogc/processes/<str:process_id>", guard(views.processes)),
    path("ogc/processes/<str:process_id>/execution", guard(views.process_execution)),
    path("ogc/jobs", guard(views.jobs)),
    path("ogc/jobs/<str:job_id>", guard(views.jobs)),
    path("ogc/jobs/<str:job_id>/results", guard(views.job_results)),
]
