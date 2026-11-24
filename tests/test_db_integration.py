"""Integration tests against a real Postgres. Skipped unless
PLATFORM_TEST_DATABASE_URL points at a database the tests may use; they create
and drop their own schema (kap_platform_test) inside it.

    docker run -d --name platform-test-db -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:17-alpine
    PLATFORM_TEST_DATABASE_URL=postgresql://postgres:test@localhost:55432/postgres pytest
"""
import asyncio
import os
import uuid
from datetime import timedelta

import asyncpg
import pytest

from kap_platform import queue, store
from kap_platform.config import Settings
from kap_platform.db import create_pool, migrate
from kap_platform.worker import RetryLater, handle_link_trace

BASE = os.environ.get("PLATFORM_TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not BASE, reason="PLATFORM_TEST_DATABASE_URL not set")
SCHEMA = "kap_platform_test"
TENANT = "71b6c98a-7f7a-4a8b-ab31-132c85bdc88b"


def dsn() -> str:
    sep = "&" if "?" in BASE else "?"
    return f"{BASE}{sep}search_path={SCHEMA}"


@pytest.fixture
async def pool():
    admin = await asyncpg.connect(BASE)
    await admin.execute(f"drop schema if exists {SCHEMA} cascade; create schema {SCHEMA}")
    await admin.close()
    await migrate(dsn())
    p = await create_pool(dsn(), max_size=6)
    yield p
    await p.close()
    admin = await asyncpg.connect(BASE)
    await admin.execute(f"drop schema if exists {SCHEMA} cascade")
    await admin.close()


async def test_migrate_is_idempotent(pool):
    assert await migrate(dsn()) == []


async def test_enqueue_idempotency_per_tenant(pool):
    a = await queue.enqueue(pool, "sync_agent_jobs", tenant_id=TENANT, idempotency_key="sync:1")
    b = await queue.enqueue(pool, "sync_agent_jobs", tenant_id=TENANT, idempotency_key="sync:1")
    c = await queue.enqueue(pool, "sync_agent_jobs", tenant_id=str(uuid.uuid4()), idempotency_key="sync:1")
    assert a is not None and b is None and c is not None


async def test_concurrent_claims_never_share_a_job(pool):
    for i in range(20):
        await queue.enqueue(pool, "link_trace", {"i": i}, tenant_id=TENANT)
    claimed = await asyncio.gather(*[queue.claim(pool, f"w{n}") for n in range(30)])
    ids = [j.id for j in claimed if j]
    assert len(ids) == 20 and len(set(ids)) == 20


async def test_priority_and_run_after_order(pool):
    late = await queue.enqueue(pool, "x", tenant_id=TENANT)
    urgent = await queue.enqueue(pool, "x", tenant_id=TENANT, priority=5)
    await queue.enqueue(pool, "x", tenant_id=TENANT, priority=9, run_after=__import__("datetime").datetime.now(__import__("datetime").timezone.utc) + timedelta(hours=1))
    assert (await queue.claim(pool, "w")).id == urgent
    assert (await queue.claim(pool, "w")).id == late
    assert await queue.claim(pool, "w") is None  # the future job is not due


async def test_fail_backs_off_then_goes_dead(pool):
    await queue.enqueue(pool, "x", tenant_id=TENANT, max_attempts=2)
    job = await queue.claim(pool, "w")
    assert await queue.fail(pool, job, "boom") == "queued"
    row = await pool.fetchrow("select status, run_after > now() as later, error from job_queue where id = $1", job.id)
    assert row["status"] == "queued" and row["later"] and row["error"] == "boom"
    await pool.execute("update job_queue set run_after = now() where id = $1", job.id)
    job = await queue.claim(pool, "w")
    assert job.attempts == 2 and await queue.fail(pool, job, "boom again") == "dead"


async def test_expired_lease_is_requeued(pool):
    await queue.enqueue(pool, "x", tenant_id=TENANT)
    job = await queue.claim(pool, "w", lease_s=60)
    await pool.execute("update job_queue set lease_until = now() - interval '1 second' where id = $1", job.id)
    assert await queue.sweep_expired(pool) == 1
    assert (await queue.claim(pool, "w2")).id == job.id


async def test_link_trace_end_to_end(pool, planner_run):
    job, calls = planner_run
    agent = {"id": str(uuid.uuid4()), "name": "sandbox_indicators_agent"}
    kap_job = {**job, "agent_id": agent["id"], "status": "succeeded", "retry_count": 0}
    for k in ("created_at", "running_at", "finished_at"):
        kap_job[k] = job[k].isoformat()
    assert await store.upsert_kap_job(pool, TENANT, agent, kap_job) is True
    for c in calls:
        await store.insert_model_call(pool, store.RecordedCall(TENANT, "agent_default", c["started_at"], c["finished_at"], "POST", "/v1/chat/completions", 200, c["request"], c["response"]))
    qjob = queue.Job(uuid.uuid4(), uuid.UUID(TENANT), "link_trace", {"job_id": job["id"]}, 1, 5)
    result = await handle_link_trace(pool, None, Settings(), qjob)
    assert result == {"status": "linked", "calls": 4}
    linked = await store.calls_for_job(pool, job["id"])
    assert [c.id for c in linked] and len(linked) == 4
    row = await store.get_job(pool, job["id"])
    assert row["trace_status"] == "linked" and row["trace_calls"] == 4


async def test_link_trace_retries_when_nothing_recorded_yet(pool, planner_run):
    job, _ = planner_run
    import datetime as dt

    now = dt.datetime.now(dt.timezone.utc)
    agent = {"id": str(uuid.uuid4()), "name": "a"}
    fresh = {**job, "agent_id": agent["id"], "status": "succeeded", "created_at": (now - timedelta(seconds=12)).isoformat(), "running_at": (now - timedelta(seconds=11)).isoformat(), "finished_at": (now - timedelta(seconds=1)).isoformat()}
    await store.upsert_kap_job(pool, TENANT, agent, fresh)
    with pytest.raises(RetryLater):
        await handle_link_trace(pool, None, Settings(), queue.Job(uuid.uuid4(), uuid.UUID(TENANT), "link_trace", {"job_id": job["id"]}, 1, 5))
