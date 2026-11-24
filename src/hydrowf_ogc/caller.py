"""Who is calling: the owner id a request acts as, from the service (hydrowf_ogc.app) to ArgoManager.

pygeoapi calls the manager with a job id or a process id, never the request, so the caller travels
alongside it in a context variable. The service sets it (acting_as) inside the worker thread that runs
the request's pygeoapi call, for the length of that call, and the manager reads it (owner()).
Everything the manager does for a request runs on that thread: ArgoManager submits synchronously,
unlike pygeoapi's own async handler, which starts a thread of its own.

The owner id is the token's `sub` (Keycloak's user id, a UUID). It becomes the hydrowf/owner label
on the Argo workflow, so it must be a valid Kubernetes label value.

The verified token itself travels too (token()): the registry is read with it, so a caller's own private
pipelines are among their processes (hydrowf_ogc.registry). It is never logged or stored.
"""
from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

from hydrowf_core.adapters.argo import LABEL_VALUE

OWNER_LABEL = "hydrowf/owner"

_owner: ContextVar[str | None] = ContextVar("hydrowf_owner", default=None)
_token: ContextVar[str | None] = ContextVar("hydrowf_token", default=None)


class NoCaller(RuntimeError):
    """The manager was reached outside a request the service let through (e.g. under `pygeoapi serve`)."""


def owner_from(claims: dict) -> str | None:
    """The owner id of a verified token's claims; None when its `sub` can't be a label value."""
    sub = claims.get("sub")
    return sub if isinstance(sub, str) and sub and LABEL_VALUE.fullmatch(sub) else None


@contextmanager
def acting_as(owner: str, token: str | None = None) -> Iterator[None]:
    """For the length of the block: the caller is `owner`, whose verified bearer token is `token`."""
    owner_reset, token_reset = _owner.set(owner), _token.set(token)
    try:
        yield
    finally:
        _token.reset(token_reset)
        _owner.reset(owner_reset)


def owner() -> str:
    """The current request's owner id. Never a default: without one, nothing may be read or run."""
    current = _owner.get()
    if current is None:
        raise NoCaller("no caller: job routes need a verified bearer token")
    return current


def identity() -> tuple[str | None, str | None]:
    """(owner id, bearer token) of the current request; (None, None) for an anonymous open read."""
    return _owner.get(), _token.get()
