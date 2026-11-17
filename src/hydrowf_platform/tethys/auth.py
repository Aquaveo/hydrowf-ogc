"""Bearer tokens on /ogc: OIDC access tokens that are JWTs (Keycloak's are), checked locally.

    bearer = bearer_auth()          # None when OIDC_ISSUER isn't set: bearer tokens are off
    claims = bearer.claims(token)   # the token's claims, or None when it isn't valid for us

A token is valid when its RS256 signature checks against the issuer's signing keys and its iss, aud,
exp and sub claims are present and match (30 s of clock skew allowed: clients call from other
clusters). The keys come from the issuer's jwks_uri (OIDC discovery, whose `issuer` must be ours),
fetched on the first token; PyJWKClient caches the key set for 5 minutes, so a key the realm drops
stops working within that time.

Deployment rules: the audience is an API-only client (hydrowf-api) that no user logs in through, so
ID tokens never carry it, and Keycloak names it in `aud` only with an audience mapper on each calling
client. OIDC_ISSUER is compared without a trailing slash, as Keycloak writes `iss`.

Settings (hydrowf_platform.tethys.settings): OIDC_ISSUER, e.g. https://<keycloak>/realms/hydrowf, and
OIDC_AUDIENCE, the audience the tokens must carry.
"""
from __future__ import annotations

import json
import logging
from functools import cache
from typing import Any
from urllib.request import urlopen

import jwt
from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from jwt.exceptions import PyJWKClientConnectionError, PyJWKClientError, PyJWKSetError, PyJWTError

LOGGER = logging.getLogger(__name__)

ALGORITHMS = ["RS256"]                       # never what the token's header asks for
REQUIRED_CLAIMS = ["exp", "iss", "aud", "sub"]
LEEWAY = 30                                  # seconds of clock skew between the issuer and us
TIMEOUT = 5                                  # seconds for discovery and key fetches


def discover_jwks_uri(issuer: str, timeout: float = TIMEOUT) -> str:
    """The issuer's jwks_uri, from its OpenID Connect discovery document (which must name `issuer`)."""
    with urlopen(f"{issuer}/.well-known/openid-configuration", timeout=timeout) as response:
        document = json.load(response)
    if document.get("issuer") != issuer:
        raise ImproperlyConfigured(f"{issuer} discovery names issuer {document.get('issuer')!r}")
    return document["jwks_uri"]


class BearerAuth:
    def __init__(self, issuer: str, audience: str, keys: Any = None):
        self.issuer = issuer.rstrip("/")
        self.audience = audience
        self._keys = keys                    # a PyJWKClient (tests pass a stub)

    @property
    def keys(self) -> Any:
        if self._keys is None:               # discovered on the first token, so a portal starts without it
            self._keys = jwt.PyJWKClient(discover_jwks_uri(self.issuer), timeout=TIMEOUT)
        return self._keys

    def claims(self, token: str) -> dict | None:
        """The claims of a token valid for this issuer and audience; None for any other token.

        The issuer failing is not an invalid token: an unreachable issuer (PyJWKClientConnectionError,
        the discovery's URLError) or a broken key set (PyJWKSetError) propagate, so it is a server
        error in the logs, not a 401. A token whose kid the key set lacks is a 401, logged as a warning
        (PyJWT gives it the same class as an empty key set).
        """
        try:
            key = self.keys.get_signing_key_from_jwt(token)
            return jwt.decode(token, key, algorithms=ALGORITHMS, issuer=self.issuer, leeway=LEEWAY,
                              audience=self.audience, options={"require": REQUIRED_CLAIMS})
        except (PyJWKClientConnectionError, PyJWKSetError):
            raise
        except PyJWKClientError as err:
            LOGGER.warning("bearer token: no signing key for it at %s: %s", self.issuer, err)
            return None
        except PyJWTError as err:
            LOGGER.info("bearer token rejected: %s", err)
            return None


@cache
def bearer_auth() -> BearerAuth | None:
    """The BearerAuth the settings describe (one per process: it holds the key cache); None when
    OIDC_ISSUER isn't set. settings.py checks the issuer comes with an audience."""
    issuer = getattr(settings, "OIDC_ISSUER", None)
    return BearerAuth(issuer, settings.OIDC_AUDIENCE) if issuer else None
