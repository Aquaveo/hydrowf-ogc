"""The Django settings pygeoapi's views read, for Tethys to copy into its own (ADDITIONAL_SETTINGS_FILES).

The same three pygeoapi.django_.settings defines for its own project: the config (PYGEOAPI_CONFIG
environment variable), the OpenAPI document (PYGEOAPI_OPENAPI) and the URL rules. Tethys copies the
names in __all__ only; its APPEND_SLASH, CORS and the rest stay Tethys's.
"""
from pygeoapi.config import get_config
from pygeoapi.openapi import load_openapi_document
from pygeoapi.util import get_api_rules

__all__ = ["PYGEOAPI_CONFIG", "OPENAPI_DOCUMENT", "API_RULES"]

PYGEOAPI_CONFIG = get_config()
OPENAPI_DOCUMENT = load_openapi_document()
API_RULES = get_api_rules(PYGEOAPI_CONFIG)
