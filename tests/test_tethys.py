"""hydrowf_platform.tethys with Django configured the way Tethys does it, without Tethys.

Tethys copies hydrowf_platform.tethys.settings' names into its settings (ADDITIONAL_SETTINGS_FILES) and
skips, with only a log line, an ADDITIONAL_URLPATTERNS module it cannot import: /ogc would answer 404.
Importing the module here makes a broken one fail the build instead.
"""
import importlib
import json
from pathlib import Path
from types import SimpleNamespace

import django
import pytest
from django.conf import settings
from django.http import HttpResponse
from django.test import Client, RequestFactory
from django.urls import resolve
from pygeoapi.openapi import generate_openapi_document

REPO = Path(__file__).parents[1]
CONFIG = REPO / "pygeoapi-config.yml"


@pytest.fixture(scope="module")
def urls(tmp_path_factory):
    """hydrowf_platform.tethys.urls, imported after Django is configured (once: Django can't be reconfigured)."""
    openapi = tmp_path_factory.mktemp("openapi") / "pygeoapi-openapi.yml"
    with pytest.MonkeyPatch.context() as env:
        env.setenv("PYGEOAPI_CONFIG", str(CONFIG))
        env.setenv("PYGEOAPI_OPENAPI", str(openapi))
        env.setenv("HYDROWF_REGISTRY", str(REPO / "registry" / "legos.yaml"))
        openapi.write_text(generate_openapi_document(CONFIG, "yaml"))

        tethys_settings = importlib.import_module("hydrowf_platform.tethys.settings")
        settings.configure(
            ROOT_URLCONF="hydrowf_platform.tethys.urls",
            ALLOWED_HOSTS=["testserver"],
            **{name: getattr(tethys_settings, name) for name in tethys_settings.__all__},
        )
        django.setup()
        yield importlib.import_module("hydrowf_platform.tethys.urls")


# ---- routes: /ogc/... resolves to pygeoapi's own views, each behind guard()


@pytest.mark.parametrize("url, view", [
    ("/ogc", "landing_page"),
    ("/ogc/processes", "processes"),
    ("/ogc/processes/ngiab-simulation", "processes"),
    ("/ogc/processes/ngiab-simulation/execution", "process_execution"),
    ("/ogc/jobs", "jobs"),
    ("/ogc/jobs/job-1", "jobs"),
    ("/ogc/jobs/job-1/results", "job_results"),
])
def test_ogc_routes_are_pygeoapi_views(urls, url, view):
    from pygeoapi.django_ import views
    assert resolve(url).func.__wrapped__ is getattr(views, view)       # functools.wraps keeps the original


def test_process_list_through_django(urls):
    response = Client().get("/ogc/processes", {"f": "json"})
    assert response.status_code == 200
    assert [p["id"] for p in json.loads(response.content)["processes"]] == ["ngiab-simulation"]


# ---- guard(): reads open, writes need a logged-in user


def ok(request, *args, **kwargs):
    return HttpResponse("ok")


@pytest.mark.parametrize("method, authenticated, status", [
    ("GET", False, 200),
    ("OPTIONS", False, 200),
    ("HEAD", False, 200),
    ("POST", False, 401),
    ("POST", True, 200),
    ("DELETE", False, 401),
    ("DELETE", True, 200),
    ("PATCH", False, 401)
])
def test_guard(urls, method, authenticated, status):
    request = RequestFactory().generic(method, "/ogc/jobs/job-1")
    request.user = SimpleNamespace(is_authenticated=authenticated)
    response = urls.guard(ok)(request, job_id="job-1")
    assert response.status_code == status
    if status == 401:
        assert json.loads(response.content) == {"error": "Authentication required"}
