"""Entry point: python -m kap_platform <migrate|api|gateway|worker>."""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal

import uvicorn

from .config import get_settings


def _gateway_app():
    from .db import create_pool
    from .gateway.app import create_gateway
    from .store import insert_model_call

    settings = get_settings()
    state: dict = {}
    lock = asyncio.Lock()

    async def record(rec):
        if "pool" not in state:
            async with lock:
                if "pool" not in state:
                    state["pool"] = await create_pool(settings.database_url, max_size=5)
        await insert_model_call(state["pool"], rec)

    return create_gateway(settings.upstream_for, record, settings.gateway_timeout_s)


async def _run_worker(concurrency: int) -> None:
    from .db import create_pool
    from .kap.client import KapClient
    from .worker import Worker

    settings = get_settings()
    pool = await create_pool(settings.database_url, max_size=concurrency + 3)
    kap = KapClient(settings.kap_base_url, settings.tenants(), settings.kap_timeout_s)
    worker = Worker(pool, kap, settings, concurrency=concurrency)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, worker.stop)
        except NotImplementedError:  # Windows
            pass
    try:
        await worker.run()
    finally:
        await kap.aclose()
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(prog="kap_platform")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("migrate", help="apply database migrations")
    p_api = sub.add_parser("api", help="run the REST API")
    p_api.add_argument("--host", default="0.0.0.0")
    p_api.add_argument("--port", type=int, default=8080)
    p_gw = sub.add_parser("gateway", help="run the model gateway")
    p_gw.add_argument("--host", default="0.0.0.0")
    p_gw.add_argument("--port", type=int, default=8090)
    p_wk = sub.add_parser("worker", help="run the job worker")
    p_wk.add_argument("--concurrency", type=int, default=4)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    # httpx logs every request at INFO; the worker polls KAP every few seconds.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    settings = get_settings()

    if args.cmd == "migrate":
        from .db import migrate

        applied = asyncio.run(migrate(settings.database_url))
        print("applied:", ", ".join(applied) or "nothing (up to date)")
    elif args.cmd == "api":
        from .api.app import create_api

        uvicorn.run(create_api(settings), host=args.host, port=args.port, proxy_headers=False)
    elif args.cmd == "gateway":
        uvicorn.run(_gateway_app(), host=args.host, port=args.port, proxy_headers=False)
    elif args.cmd == "worker":
        asyncio.run(_run_worker(args.concurrency))


if __name__ == "__main__":
    main()
