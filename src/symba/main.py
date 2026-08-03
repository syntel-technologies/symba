"""Engine entrypoint.

boot sequence
-------------
config  ->  logging (configured at import)  ->  pools  ->  subsystems
Flyway applies migrations OUT OF BAND before this process starts
(compose depends_on: flyway service_completed_successfully). The engine
NEVER self-migrates (tests use symba.db.migrate.apply_schema).

process shape (crash-only)
---------------------------
one asyncio.TaskGroup owns every subsystem. Any subsystem raising an
unhandled exception tears down the group -> process exits -> the
orchestrator restarts it -> Postgres state makes restart safe. The three
periodic loops override this with per-pass containment (services/loops.py),
so a transient PG blip does not crash-loop the whole engine.

role gating: --role=api|sweeper|all (default all) via SYMBA_SERVER__ROLES.
"""

from __future__ import annotations

import asyncio
import signal

from symba.config import load_config
from symba.db.pool import create_pools
from symba.observability.logging import logger
from symba.services.loops import CronService, Dispatcher, RateRefiller, Sweeper
from symba.transport.grpc_server import serve_grpc
from symba.transport.http_server import serve_http
from symba.transport.state import EngineState

logger = logger.bind(service="main", context="engine")


class _Drain(Exception):
    """Internal sentinel used to unwind the TaskGroup on graceful shutdown."""


def _install_sigterm(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:  # pragma: no cover - non-unix
            pass


async def _run() -> None:
    cfg = load_config()
    logger.info(
        "[main] Booting engine",
        environment=cfg.app.environment,
        roles=cfg.server.roles,
        grpc_port=cfg.server.grpc_port,
        http_port=cfg.server.http_port,
    )

    pools = await create_pools(cfg.postgres)
    state = EngineState.build(cfg, pools)
    # Warm the rate-class cache before serving so the first claim pass reserves
    # against real config, not an empty cache.
    await state.rate_limiter.load()

    roles = set(cfg.server.roles)
    run_api = "api" in roles or "all" in roles
    run_sweeper = "sweeper" in roles or "all" in roles

    stop_event = asyncio.Event()
    _install_sigterm(stop_event)
    loops: list[Dispatcher | Sweeper | CronService | RateRefiller] = []

    try:
        async with asyncio.TaskGroup() as tg:
            if run_api:
                tg.create_task(serve_grpc(state))
                tg.create_task(serve_http(state))
                # SSE ledger fan-out feeds the operator UI; API-role only.
                tg.create_task(state.events.start())
                dispatcher = Dispatcher(state)
                refiller = RateRefiller(state)
                loops.extend((dispatcher, refiller))
                tg.create_task(dispatcher.run())
                tg.create_task(refiller.run())
            if run_sweeper:
                sweeper, cron = Sweeper(state), CronService(state)
                loops.extend((sweeper, cron))
                tg.create_task(sweeper.run())
                tg.create_task(cron.run())

            state.serving = True
            logger.info("[main] Engine serving")

            await stop_event.wait()
            logger.info("[main] Shutdown signal received; draining", drain_s=cfg.server.shutdown_drain_s)
            state.serving = False
            state.events.stop()
            for loop_obj in loops:
                loop_obj.stop()
            raise _Drain
    except* _Drain:
        pass
    finally:
        await state.rate_limiter.close()
        await pools.close()
        logger.info("[main] Engine stopped")


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
