"""Postgres-backed job queue.

One table (`job_queue`), no broker:
- claim with `FOR UPDATE SKIP LOCKED`, so concurrent workers never take the same job;
- a lease (`lease_until`) that workers extend while working; a sweeper requeues
  jobs whose lease expired (crashed worker);
- retries with exponential backoff, then `dead` after `max_attempts`;
- `idempotency_key` per tenant, so periodic or repeated enqueues collapse;
- `pg_notify('job_queue', kind)` on enqueue to wake idle workers.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import asyncpg

CHANNEL = "job_queue"


@dataclass
class Job:
    id: uuid.UUID
    tenant_id: uuid.UUID | None
    kind: str
    payload: dict[str, Any]
    attempts: int
    max_attempts: int


def backoff_seconds(attempts: int) -> int:
    """10 s, 20 s, 40 s … capped at 1 h."""
    return min(3600, 10 * 2 ** max(0, attempts - 1))


async def enqueue(
    conn: asyncpg.Connection | asyncpg.Pool,
    kind: str,
    payload: dict[str, Any] | None = None,
    *,
    tenant_id: str | uuid.UUID | None = None,
    priority: int = 0,
    run_after: datetime | None = None,
    idempotency_key: str | None = None,
    max_attempts: int = 5,
) -> uuid.UUID | None:
    """Add a job. Returns its id, or None when an identical idempotency key already exists."""
    row = await conn.fetchrow(
        """
        insert into job_queue (tenant_id, kind, payload, priority, run_after, idempotency_key, max_attempts)
        values ($1, $2, $3, $4, coalesce($5, now()), $6, $7)
        on conflict do nothing
        returning id
        """,
        tenant_id,
        kind,
        payload or {},
        priority,
        run_after,
        idempotency_key,
        max_attempts,
    )
    if row is None:
        return None
    await conn.execute("select pg_notify($1, $2)", CHANNEL, kind)
    return row["id"]


async def claim(conn: asyncpg.Connection | asyncpg.Pool, worker_id: str, lease_s: int = 60, kinds: list[str] | None = None) -> Job | None:
    row = await conn.fetchrow(
        """
        update job_queue
           set status = 'running', locked_by = $1, lease_until = now() + make_interval(secs => $2),
               attempts = attempts + 1, updated_at = now()
         where id = (
               select id from job_queue
                where status = 'queued' and run_after <= now()
                  and ($3::text[] is null or kind = any($3::text[]))
                order by priority desc, run_after
                for update skip locked
                limit 1)
        returning id, tenant_id, kind, payload, attempts, max_attempts
        """,
        worker_id,
        float(lease_s),
        kinds,
    )
    return Job(**dict(row)) if row else None


async def extend_lease(conn: asyncpg.Connection | asyncpg.Pool, job_id: uuid.UUID, worker_id: str, lease_s: int = 60) -> bool:
    status = await conn.execute(
        "update job_queue set lease_until = now() + make_interval(secs => $3), updated_at = now() where id = $1 and locked_by = $2 and status = 'running'",
        job_id,
        worker_id,
        float(lease_s),
    )
    return status.endswith("1")


async def complete(conn: asyncpg.Connection | asyncpg.Pool, job_id: uuid.UUID, result: Any = None) -> None:
    await conn.execute(
        "update job_queue set status = 'succeeded', result = $2, lease_until = null, updated_at = now() where id = $1",
        job_id,
        result,
    )


async def fail(conn: asyncpg.Connection | asyncpg.Pool, job: Job, error: str, *, retry: bool = True) -> str:
    """Record a failure: requeue with backoff, or mark dead. Returns the new status."""
    if retry and job.attempts < job.max_attempts:
        await conn.execute(
            """
            update job_queue
               set status = 'queued', error = $2, locked_by = null, lease_until = null,
                   run_after = now() + make_interval(secs => $3), updated_at = now()
             where id = $1
            """,
            job.id,
            error[:4000],
            float(backoff_seconds(job.attempts)),
        )
        return "queued"
    await conn.execute(
        "update job_queue set status = 'dead', error = $2, lease_until = null, updated_at = now() where id = $1",
        job.id,
        error[:4000],
    )
    return "dead"


async def purge_finished(conn: asyncpg.Connection | asyncpg.Pool, older_than_days: int = 7) -> int:
    """Delete finished jobs (any final status) older than N days."""
    status = await conn.execute(
        "delete from job_queue where status not in ('queued', 'running') and updated_at < now() - make_interval(days => $1)",
        older_than_days,
    )
    return int(status.split()[-1])


async def sweep_expired(conn: asyncpg.Connection | asyncpg.Pool) -> int:
    """Requeue running jobs whose lease has passed (their worker died). Returns how many."""
    status = await conn.execute(
        """
        update job_queue
           set status = case when attempts >= max_attempts then 'dead' else 'queued' end,
               error = coalesce(error, '') || case when error is null then '' else E'\n' end || 'lease expired',
               locked_by = null, lease_until = null, updated_at = now()
         where status = 'running' and lease_until < now()
        """
    )
    return int(status.split()[-1])
