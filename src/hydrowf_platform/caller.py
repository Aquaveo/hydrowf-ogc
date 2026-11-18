"""Who is calling: the owner id a request acts as, from guard() to ArgoManager.

pygeoapi calls the manager with a job id or a process id, never the request, so the caller travels
alongside it in a context variable. guard() sets it for the length of one request (acting_as), and
the manager reads it (owner()). Everything the manager does for a request runs on that request's
thread: ArgoManager submits synchronously, unlike pygeoapi's own async handler, which starts a thread.

The owner id is the token's `sub` (Keycloak's user id, a UUID). It becomes the hydrowf/owner label
on the Argo workflow, so it must be a valid Kubernetes label value.
"""
from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

from hydrowf_core.adapters.argo import LABEL_VALUE

OWNER_LABEL = "hydrowf/owner"

_owner: ContextVar[str | None] = ContextVar("hydrowf_owner", default=None)


class NoCaller(RuntimeError):
    """The manager was reached outside a request guard() let through (e.g. under `pygeoapi serve`)."""


def owner_from(claims: dict) -> str | None:
    """The owner id of a verified token's claims; None when its `sub` can't be a label value."""
    sub = claims.get("sub")
    return sub if isinstance(sub, str) and sub and LABEL_VALUE.fullmatch(sub) else None


@contextmanager
def acting_as(owner: str) -> Iterator[None]:
    token = _owner.set(owner)
    try:
        yield
    finally:
        _owner.reset(token)


def owner() -> str:
    """The current request's owner id. Never a default: without one, nothing may be read or run."""
    current = _owner.get()
    if current is None:
        raise NoCaller("no caller: job routes need a verified bearer token")
    return current
