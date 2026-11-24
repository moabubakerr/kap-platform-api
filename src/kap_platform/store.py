"""Reads and writes for model calls and mirrored KAP jobs."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import asyncpg

from .traces.build import Call
from .traces.correlate import CallKey, JobWindow
from .traces.extract import parse_reply, summarize_request


# ---------------------------------------------------------------- model calls
@dataclass
class RecordedCall:
    tenant_id: str | None
    provider: str
    started_at: datetime
    finished_at: datetime
    method: str
    path: str
    status: int | None
    request: Any
    response: Any
    error: str | None = None


async def insert_model_call(conn: asyncpg.Pool | asyncpg.Connection, rec: RecordedCall) -> None:
    summary = summarize_request(rec.request)
    usage = parse_reply(rec.response).usage or {}
    await conn.execute(
        """
        insert into model_call (tenant_id, provider, started_at, finished_at, duration_ms, method, path, status,
                                model, stream, n_messages, first_user_text, prompt_hash, request, response,
                                prompt_tokens, completion_tokens, total_tokens, error)
        values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
        """,
        rec.tenant_id,
        rec.provider,
        rec.started_at,
        rec.finished_at,
        int((rec.finished_at - rec.started_at).total_seconds() * 1000),
        rec.method,
        rec.path,
        rec.status,
        summary.model,
        summary.stream,
        summary.n_messages,
        summary.first_user_text,
        summary.prompt_hash,
        rec.request,
        rec.response,
        usage.get("prompt_tokens"),
        usage.get("completion_tokens"),
        usage.get("total_tokens"),
        rec.error,
    )


async def unlinked_calls_near(conn: asyncpg.Pool | asyncpg.Connection, tenant_id: str, lo: datetime, hi: datetime) -> list[CallKey]:
    rows = await conn.fetch(
        """
        select id::text, tenant_id::text, started_at, first_user_text from model_call
         where kap_job_id is null and n_messages > 0 and started_at between $2 and $3
           and (tenant_id = $1 or tenant_id is null)
        """,
        tenant_id,
        lo,
        hi,
    )
    return [CallKey(r["id"], r["tenant_id"], r["started_at"], r["first_user_text"]) for r in rows]


async def link_calls(conn: asyncpg.Pool | asyncpg.Connection, job_id: str, call_ids: list[str]) -> None:
    await conn.execute("update model_call set kap_job_id = $1 where id = any($2::uuid[]) and kap_job_id is null", job_id, call_ids)


async def calls_for_job(conn: asyncpg.Pool | asyncpg.Connection, job_id: str) -> list[Call]:
    rows = await conn.fetch(
        """
        select id::text, started_at, finished_at, duration_ms, model, status, request, response,
               prompt_tokens, completion_tokens, total_tokens
          from model_call where kap_job_id = $1 order by started_at
        """,
        job_id,
    )
    return [Call(**dict(r)) for r in rows]


# ---------------------------------------------------------------- KAP jobs
TERMINAL = ("succeeded", "failed", "cancelled")


def _ts(v: Any) -> datetime | None:
    if v is None or isinstance(v, datetime):
        return v
    return datetime.fromisoformat(str(v).replace("Z", "+00:00"))


async def upsert_kap_job(conn: asyncpg.Pool | asyncpg.Connection, tenant_id: str, agent: dict[str, Any], job: dict[str, Any], user_names: dict[str, str] | None = None) -> bool:
    """Insert or update a job from the KAP API. Returns True when it is terminal and still awaits linking."""
    created_by = job.get("created_by")
    row = await conn.fetchrow(
        """
        insert into kap_job (id, tenant_id, agent_id, agent_name, status, user_prompt, payload, result, error,
                             retry_count, created_by, created_at, running_at, finished_at, created_by_name, synced_at)
        values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15, now())
        on conflict (id) do update set
          status = excluded.status, result = excluded.result, error = excluded.error,
          retry_count = excluded.retry_count, running_at = excluded.running_at,
          finished_at = excluded.finished_at, agent_name = coalesce(excluded.agent_name, kap_job.agent_name),
          created_by_name = coalesce(excluded.created_by_name, kap_job.created_by_name),
          synced_at = now()
        returning status, trace_status
        """,
        job["id"],
        tenant_id,
        job.get("agent_id") or agent.get("id"),
        agent.get("name"),
        job["status"],
        job.get("user_prompt"),
        job.get("payload"),
        job.get("result"),
        job.get("error"),
        job.get("retry_count") or 0,
        created_by,
        _ts(job.get("created_at")),
        _ts(job.get("running_at")),
        _ts(job.get("finished_at")),
        (user_names or {}).get(str(created_by)) if created_by else None,
    )
    return row["status"] in TERMINAL and row["trace_status"] == "pending"


async def job_window(conn: asyncpg.Pool | asyncpg.Connection, job_id: str) -> JobWindow | None:
    r = await conn.fetchrow("select id::text, tenant_id::text, user_prompt, running_at, finished_at from kap_job where id = $1", job_id)
    return JobWindow(**dict(r)) if r else None


async def overlapping_jobs(conn: asyncpg.Pool | asyncpg.Connection, job: JobWindow, slack: timedelta) -> list[JobWindow]:
    if job.running_at is None or job.finished_at is None:
        return []
    rows = await conn.fetch(
        """
        select id::text, tenant_id::text, user_prompt, running_at, finished_at from kap_job
         where tenant_id = $1 and id <> $2 and user_prompt = $3
           and running_at <= $5 and coalesce(finished_at, now()) >= $4
        """,
        job.tenant_id,
        job.id,
        job.user_prompt,
        job.running_at - slack,
        job.finished_at + slack,
    )
    return [JobWindow(**dict(r)) for r in rows]


async def set_trace_status(conn: asyncpg.Pool | asyncpg.Connection, job_id: str, status: str, n_calls: int) -> None:
    await conn.execute("update kap_job set trace_status = $2, trace_calls = $3 where id = $1", job_id, status, n_calls)


async def list_jobs(conn: asyncpg.Pool | asyncpg.Connection, tenant_id: str | None, limit: int, offset: int) -> tuple[list[dict[str, Any]], int]:
    rows = await conn.fetch(
        """
        select id::text, tenant_id::text, agent_id::text, agent_name, status, user_prompt, error, retry_count,
               created_by::text, created_by_name, created_at, running_at, finished_at, trace_status, trace_calls,
               count(*) over () as total
          from kap_job where ($1::uuid is null or tenant_id = $1)
         order by created_at desc limit $2 offset $3
        """,
        tenant_id,
        limit,
        offset,
    )
    total = rows[0]["total"] if rows else 0
    return [{k: v for k, v in dict(r).items() if k != "total"} for r in rows], total


async def get_job(conn: asyncpg.Pool | asyncpg.Connection, job_id: str) -> dict[str, Any] | None:
    r = await conn.fetchrow(
        """
        select id::text, tenant_id::text, agent_id::text, agent_name, status, user_prompt, payload, result, error,
               retry_count, created_by::text, created_by_name, created_at, running_at, finished_at, trace_status, trace_calls
          from kap_job where id = $1
        """,
        job_id,
    )
    return dict(r) if r else None
