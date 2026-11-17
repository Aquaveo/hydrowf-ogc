"""The Django settings /ogc reads, for Tethys to copy into its own (ADDITIONAL_SETTINGS_FILES).

pygeoapi's views read three, the same pygeoapi.django_.settings defines for its own project: the config
(PYGEOAPI_CONFIG environment variable), the OpenAPI document (PYGEOAPI_OPENAPI) and the URL rules.
guard() reads the OIDC issuer and audience of bearer tokens (hydrowf_platform.tethys.auth); without
HYDROWF_OIDC_ISSUER, bearer tokens are off. Tethys copies the names in __all__ only; its APPEND_SLASH,
CORS and the rest stay Tethys's.
"""
import os

from django.core.exceptions import ImproperlyConfigured
from pygeoapi.config import get_config
from pygeoapi.openapi import load_openapi_document
from pygeoapi.util import get_api_rules

__all__ = ["PYGEOAPI_CONFIG", "OPENAPI_DOCUMENT", "API_RULES", "OIDC_ISSUER", "OIDC_AUDIENCE"]

PYGEOAPI_CONFIG = get_config()
OPENAPI_DOCUMENT = load_openapi_document()
API_RULES = get_api_rules(PYGEOAPI_CONFIG)

OIDC_ISSUER = os.environ.get("HYDROWF_OIDC_ISSUER")        # e.g. https://<keycloak>/realms/hydrowf
OIDC_AUDIENCE = os.environ.get("HYDROWF_OIDC_AUDIENCE")    # the audience /ogc tokens must carry
if OIDC_ISSUER and not OIDC_AUDIENCE:                      # at startup, not on the first token
    raise ImproperlyConfigured("HYDROWF_OIDC_ISSUER is set but HYDROWF_OIDC_AUDIENCE is not")
