"""Run the service: the public OGC API on PORT (8000) and, when the config hands steps read keys
(credentials.steps), the step-key vendor on VENDOR_PORT (8001), in one process.

    python -m hydrowf_ogc.serve

The vendor's port is for step pods inside the cluster only: the ingress routes the public port, never this
one (hydrowf-deploy). Both share the manager the public app builds (its runtime, its STS vendor). One signal
handler stops both servers together (uvicorn's own handler is per server, and the second would replace the
first's).
"""
from __future__ import annotations

import asyncio
import contextlib
import os
import signal

import uvicorn

from hydrowf_ogc.app import app_from_env
from hydrowf_ogc.kube import kube_from_env
from hydrowf_ogc.stepkeys import StepKeys, step_keys_app


class Server(uvicorn.Server):
    @contextlib.contextmanager
    def capture_signals(self):                     # main() handles signals for both servers
        yield


def servers(app=None, kube=None) -> list[uvicorn.Server]:
    """The public server, and the vendor's when the manager has a Reader (steps read in place)."""
    app = app or app_from_env()
    found = [Server(uvicorn.Config(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)),
                                   proxy_headers=True))]
    manager = app.state.api.manager
    if getattr(manager, "reader", None) is not None:
        step_keys = StepKeys(kube or kube_from_env(), manager.runtime, manager.vendor, manager.bucket,
                             manager.namespace)
        found.append(Server(uvicorn.Config(step_keys_app(step_keys), host="0.0.0.0", proxy_headers=False,
                                           port=int(os.environ.get("VENDOR_PORT", 8001)))))
    return found


async def main() -> None:
    running = servers()
    loop = asyncio.get_running_loop()

    def stop() -> None:                            # the first signal drains both; a second forces them out
        force = any(server.should_exit for server in running)
        for server in running:
            server.should_exit = True
            server.force_exit = server.force_exit or force

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop)
    await asyncio.gather(*(server.serve() for server in running))


if __name__ == "__main__":
    asyncio.run(main())
