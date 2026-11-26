"""Worker: runs queued jobs and schedules the periodic ones.

Job kinds (slice 1):
  sync_agent_jobs  {}                                  — mirror recent agent jobs of a tenant from KAP
  fetch_job        {agent_id, job_id}                  — refresh one job (after a KAP callback)
  link_trace       {job_id}                            — link recorded model calls to a finished job
  pg_export        {run_id}                            — export a PostgreSQL dataset to S3 for KAP (pgsource/export.py)
  pg_export_watch  {run_id}                            — follow KAP's pipeline job for that export (re-enqueued every ~10 s)
  eval_watch       {run_id}                            — one tick of an evaluation run (evals/runner.py, re-enqueued every ~5 s)
  wf_tick          {run_id}                            — one tick of a workflow run (workflows/runner.py)

The scheduler tick (every sync_interval_s) also starts scheduled PostgreSQL exports
and scheduled / new-data evaluation runs (evals/schedule.py).
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

import asyncpg

from . import queue, store
from .config import Settings
from .kap.client import KapClient, KapError
from .evals import runner as eval_runner
from .evals import schedule as eval_schedule
from .pgsource import export as pg_export
from .workflows import runner as wf_runner
from .traces.correlate import link_job

log = logging.getLogger(__name__)


class RetryLater(Exception):
    """Raise from a handler to retry the job with backoff (not counted as an error in logs)."""


Handler = Callable[[asyncpg.Pool, KapClient, Settings, queue.Job], Awaitable[Any]]


_USER_NAMES: dict[str, tuple[float, dict[str, str]]] = {}
USER_NAMES_TTL_S = 300


async def user_names(kap: KapClient, tenant_id: str) -> dict[str, str]:
    """KAP user id → username for a tenant, cached for a few minutes. Empty if not permitted."""
    cached = _USER_NAMES.get(tenant_id)
    if cached and time.monotonic() - cached[0] < USER_NAMES_TTL_S:
        return cached[1]
    try:
        names = {u["id"]: u["username"] for u in await kap.list_users(tenant_id)}
    except KapError as exc:
        if exc.status not in (403, 404):
            raise
        names = {}  # account lacks user:read — jobs keep their ids only
    _USER_NAMES[tenant_id] = (time.monotonic(), names)
    return names


async def handle_sync_agent_jobs(pool: asyncpg.Pool, kap: KapClient, settings: Settings, job: queue.Job) -> dict[str, int]:
    tenant_id = str(job.tenant_id)
    seen = to_link = 0
    names = await user_names(kap, tenant_id)
    for agent in await kap.list_agents(tenant_id):
        for j in await kap.recent_agent_jobs(tenant_id, agent["id"], limit=50):
            seen += 1
            if await store.upsert_kap_job(pool, tenant_id, agent, j, names):
                await queue.enqueue(pool, "link_trace", {"job_id": j["id"]}, tenant_id=tenant_id, idempotency_key=f"link:{j['id']}")
                to_link += 1
    return {"jobs_seen": seen, "to_link": to_link}


async def handle_fetch_job(pool: asyncpg.Pool, kap: KapClient, settings: Settings, job: queue.Job) -> dict[str, Any]:
    tenant_id = str(job.tenant_id)
    agent_id, job_id = job.payload["agent_id"], job.payload["job_id"]
    data = await kap.get_agent_job(tenant_id, agent_id, job_id)
    agent = {"id": agent_id, "name": job.payload.get("agent_name")}
    if await store.upsert_kap_job(pool, tenant_id, agent, data, await user_names(kap, tenant_id)):
        await queue.enqueue(pool, "link_trace", {"job_id": job_id}, tenant_id=tenant_id, idempotency_key=f"link:{job_id}")
    return {"status": data["status"]}


async def handle_link_trace(pool: asyncpg.Pool, kap: KapClient, settings: Settings, job: queue.Job) -> dict[str, Any]:
    job_id = job.payload["job_id"]
    window = await store.job_window(pool, job_id)
    if window is None:
        return {"skipped": "unknown job"}
    slack = timedelta(seconds=settings.correlation_slack_s)
    if window.running_at is None or window.finished_at is None:
        await store.set_trace_status(pool, job_id, "none", 0)
        return {"status": "none", "reason": "job never ran"}
    calls = await store.unlinked_calls_near(pool, window.tenant_id, window.running_at - slack, window.finished_at + slack)
    others = await store.overlapping_jobs(pool, window, slack)
    link = link_job(window, calls, others, settings.correlation_slack_s)
    if link.status == "none" and datetime.now(timezone.utc) - window.finished_at < timedelta(seconds=60):
        # the gateway records after replying; give late records a moment before giving up
        raise RetryLater("no calls recorded yet")
    if link.status == "linked":
        await store.link_calls(pool, job_id, link.call_ids)
    await store.set_trace_status(pool, job_id, link.status, len(link.call_ids))
    return {"status": link.status, "calls": len(link.call_ids)}


HANDLERS: dict[str, Handler] = {
    "sync_agent_jobs": handle_sync_agent_jobs,
    "fetch_job": handle_fetch_job,
    "link_trace": handle_link_trace,
    "pg_export": pg_export.handle_pg_export,
    "pg_export_watch": pg_export.handle_pg_export_watch,
    "eval_watch": eval_runner.handle_eval_watch,
    "wf_tick": wf_runner.handle_wf_tick,
}


class Worker:
    def __init__(self, pool: asyncpg.Pool, kap: KapClient, settings: Settings, concurrency: int = 4, handlers: dict[str, Handler] | None = None):
        self.pool, self.kap, self.settings = pool, kap, settings
        self.concurrency = concurrency
        self.handlers = handlers or HANDLERS
        self._wake = asyncio.Event()
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()

    async def run(self) -> None:
        listener = await self.pool.acquire()

        def on_notify(*_: Any) -> None:
            self._wake.set()

        await listener.add_listener(queue.CHANNEL, on_notify)
        tasks = [asyncio.create_task(self._loop(i)) for i in range(self.concurrency)]
        tasks += [asyncio.create_task(self._scheduler()), asyncio.create_task(self._housekeeping())]
        try:
            await self._stop.wait()
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await listener.remove_listener(queue.CHANNEL, on_notify)
            await self.pool.release(listener)

    async def _scheduler(self) -> None:
        interval = self.settings.sync_interval_s
        while not self._stop.is_set():
            bucket = int(time.time() // interval)
            for tenant in self.settings.tenants():
                if tenant.sync_jobs:
                    await queue.enqueue(self.pool, "sync_agent_jobs", tenant_id=tenant.id, idempotency_key=f"sync:{bucket}", max_attempts=1)
            if "pg_export" in self.handlers:
                try:
                    await pg_export.schedule_tick(self.pool, self.settings)
                except Exception:
                    log.exception("scheduled PostgreSQL exports could not be started")
            if "eval_watch" in self.handlers:
                try:
                    await eval_schedule.schedule_tick(self.pool, self.kap, self.settings)
                except Exception:
                    log.exception("scheduled evaluations could not be started")
            await self._sleep(interval)

    async def _housekeeping(self) -> None:
        while not self._stop.is_set():
            with contextlib.suppress(Exception):
                n = await queue.sweep_expired(self.pool)
                if n:
                    log.warning("requeued %d jobs with expired leases", n)
                await queue.purge_finished(self.pool)
            await self._sleep(30)

    async def _sleep(self, seconds: float) -> None:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)

    async def _loop(self, n: int) -> None:
        worker_id = f"{self.settings.worker_id}/{n}"
        while not self._stop.is_set():
            job = await queue.claim(self.pool, worker_id, self.settings.lease_s, list(self.handlers))
            if job is None:
                self._wake.clear()
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self._wake.wait(), timeout=5)
                continue
            await self.run_one(job, worker_id)

    async def run_one(self, job: queue.Job, worker_id: str) -> None:
        heartbeat = asyncio.create_task(self._heartbeat(job, worker_id))
        try:
            result = await self.handlers[job.kind](self.pool, self.kap, self.settings, job)
            await queue.complete(self.pool, job.id, result)
        except RetryLater as exc:
            await queue.fail(self.pool, job, f"retry: {exc}")
        except KapError as exc:
            # 4xx other than 429 will not get better by retrying
            await queue.fail(self.pool, job, str(exc), retry=exc.status >= 500 or exc.status == 429)
            log.warning("job %s (%s) failed: %s", job.id, job.kind, exc)
        except Exception as exc:
            await queue.fail(self.pool, job, f"{type(exc).__name__}: {exc}")
            log.exception("job %s (%s) failed", job.id, job.kind)
        finally:
            heartbeat.cancel()

    async def _heartbeat(self, job: queue.Job, worker_id: str) -> None:
        with contextlib.suppress(asyncio.CancelledError):
            while True:
                await asyncio.sleep(self.settings.lease_s / 3)
                await queue.extend_lease(self.pool, job.id, worker_id, self.settings.lease_s)
