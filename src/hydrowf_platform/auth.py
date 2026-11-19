"""Bearer tokens: OIDC access tokens that are JWTs (Keycloak's are), checked locally.

    bearer = bearer_from_env()      # None when HYDROWF_OIDC_ISSUER isn't set: bearer tokens are off
    claims = bearer.claims(token)   # the token's claims, or None when it isn't valid for us

A token is valid when its RS256 signature checks against the issuer's signing keys and its iss, aud,
exp and sub claims are present and match (30 s of clock skew allowed: clients call from other
clusters). The keys come from the issuer's jwks_uri (OIDC discovery, whose `issuer` must be ours),
fetched on the first token; PyJWKClient caches the key set for 5 minutes, so a key the realm drops
stops working within that time.

Deployment rules: the audience is an API-only client (hydrowf-api) that no user logs in through, so
ID tokens never carry it, and Keycloak names it in `aud` only with an audience mapper on each calling
client. The issuer is compared without a trailing slash, as Keycloak writes `iss`.

No web framework here: the services (hydrowf_platform.ogc) pass the Authorization header in.
"""
from __future__ import annotations

import json
import logging
import os
import threading
from typing import Any
from urllib.request import urlopen

import jwt
from jwt.exceptions import PyJWKClientConnectionError, PyJWKClientError, PyJWKSetError, PyJWTError

LOGGER = logging.getLogger(__name__)

ALGORITHMS = ["RS256"]                       # never what the token's header asks for
REQUIRED_CLAIMS = ["exp", "iss", "aud", "sub"]
LEEWAY = 30                                  # seconds of clock skew between the issuer and us
TIMEOUT = 5                                  # seconds for discovery and key fetches


class AuthConfigError(RuntimeError):
    """The OIDC settings or the issuer's discovery document are wrong: a deployment error, not a 401."""


def bearer_token(authorization: str | None) -> str | None:
    """The token of an `Authorization: Bearer <token>` header value; None when the request has no
    bearer credential (no header, or another scheme). A bearer header without a token gives "", which
    callers refuse: a malformed credential is never read as no credential. Only extracts:
    BearerAuth validates."""
    parts = (authorization or "").split(None, 1)          # any whitespace; [] without one
    if not parts or parts[0].lower() != "bearer":
        return None
    return parts[1].strip() if len(parts) > 1 else ""


def discover_jwks_uri(issuer: str, timeout: float = TIMEOUT) -> str:
    """The issuer's jwks_uri, from its OpenID Connect discovery document (which must name `issuer`)."""
    with urlopen(f"{issuer}/.well-known/openid-configuration", timeout=timeout) as response:
        document = json.load(response)
    if document.get("issuer") != issuer:
        raise AuthConfigError(f"{issuer} discovery names issuer {document.get('issuer')!r}")
    return document["jwks_uri"]


class BearerAuth:
    def __init__(self, issuer: str, audience: str, keys: Any = None):
        self.issuer = issuer.rstrip("/")
        self.audience = audience
        self._keys = keys                    # a PyJWKClient (tests pass a stub)
        self._discovering = threading.Lock()   # concurrent first tokens discover once

    @property
    def keys(self) -> Any:
        if self._keys is None:               # discovered on the first token, so the service starts without it
            with self._discovering:
                if self._keys is None:
                    self._keys = jwt.PyJWKClient(discover_jwks_uri(self.issuer), timeout=TIMEOUT)
        return self._keys

    def claims(self, token: str) -> dict | None:
        """The claims of a token valid for this issuer and audience; None for any other token.

        A string that isn't a JWT is refused before the issuer is asked for keys, so garbage tokens
        never trigger discovery. An unreachable issuer (PyJWKClientConnectionError, the discovery's
        URLError) or an unparseable key set (PyJWKSetError) propagate: a server error in the logs,
        not a 401. A token whose kid the key set lacks is a 401, logged as a warning; so is a key set
        that is reachable but empty or not a JSON object, since PyJWT raises the same class for all
        three (telling them apart would mean matching its messages).
        """
        try:
            jwt.get_unverified_header(token)             # DecodeError (a PyJWTError) for a non-JWT
            key = self.keys.get_signing_key_from_jwt(token)
            return jwt.decode(token, key, algorithms=ALGORITHMS, issuer=self.issuer, leeway=LEEWAY,
                              audience=self.audience, options={"require": REQUIRED_CLAIMS})
        except (PyJWKClientConnectionError, PyJWKSetError):
            raise
        except PyJWKClientError as err:
            # %r: the message quotes the token's kid, which the caller chose (no forged log lines)
            LOGGER.warning("bearer token: no signing key for it at %s: %r", self.issuer, err)
            return None
        except PyJWTError as err:
            LOGGER.info("bearer token rejected: %r", err)
            return None


def bearer_from_env(environ: dict | None = None) -> BearerAuth | None:
    """The BearerAuth HYDROWF_OIDC_ISSUER and HYDROWF_OIDC_AUDIENCE describe; None without an issuer.
    Build it once per process: it holds the key cache."""
    environ = os.environ if environ is None else environ
    issuer = environ.get("HYDROWF_OIDC_ISSUER")          # e.g. https://<keycloak>/realms/hydrowf
    audience = environ.get("HYDROWF_OIDC_AUDIENCE")      # the audience the tokens must carry
    if not issuer:
        return None
    if not audience:                                     # at startup, not on the first token
        raise AuthConfigError("HYDROWF_OIDC_ISSUER is set but HYDROWF_OIDC_AUDIENCE is not")
    return BearerAuth(issuer, audience)
