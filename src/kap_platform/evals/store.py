"""Reads and writes for eval_set, eval_case, eval_run and eval_result.

Rows come back as plain dicts (ids as text). `EvalStore` is the only class
touching SQL here, so tests can swap in an in-memory store with the same methods.
"""
from __future__ import annotations

from typing import Any

import asyncpg

from .. import queue
from .. import store as kap_store
from ..traces.build import Trace, build_trace

ACTIVE = ("queued", "running")
DONE = ("scored", "error")


class Conflict(Exception):
    pass


class InUse(Exception):
    pass


SET_FIELDS = ("name", "description")
CASE_FIELDS = ("position", "question", "expected", "must_include", "must_not_include", "tags")
RUN_FIELDS = ("status", "summary", "polls", "error", "started_at", "finished_at", "agent_name")
RESULT_FIELDS = ("status", "job_id", "job_status", "answer", "checks", "judge", "passed", "reason", "metrics", "error", "submitted_at", "answered_at", "scored_at")

_SET_SELECT = """
select s.id::text, s.tenant_id::text, s.name, s.description, s.created_at, s.updated_at,
       (select count(*) from eval_case c where c.set_id = s.id)::int as case_count,
       (select jsonb_build_object('id', r.id, 'status', r.status, 'agent_name', r.agent_name, 'summary', r.summary,
                                  'created_at', r.created_at, 'finished_at', r.finished_at)
          from eval_run r where r.set_id = s.id order by r.created_at desc limit 1) as last_run
  from eval_set s
"""
_CASE_SELECT = """
select id::text, set_id::text, position, question, expected, must_include, must_not_include, tags, created_at, updated_at
  from eval_case
"""
_RUN_SELECT = """
select r.id::text, r.tenant_id::text, r.set_id::text, s.name as set_name, r.agent_id::text, r.agent_name, r.agent, r.label,
       r.options, r.status, r.summary, r.polls, r.error, r.created_at, r.started_at, r.finished_at
  from eval_run r join eval_set s on s.id = r.set_id
"""
_RESULT_SELECT = """
select id::text, run_id::text, case_id::text, position, question, expected, must_include, must_not_include, status,
       job_id::text, job_status, answer, checks, judge, passed, reason, metrics, error, submitted_at, answered_at, scored_at
  from eval_result
"""


def _set_clause(fields: dict[str, Any], allowed: tuple[str, ...], start: int) -> tuple[str, list[Any]]:
    parts, args = [], []
    for k, v in fields.items():
        if k not in allowed:
            raise ValueError(f"unknown field {k}")
        args.append(v)
        parts.append(f"{k} = ${start + len(args) - 1}")
    return ", ".join(parts), args


class EvalStore:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    # ------------------------------------------------------------ sets
    async def list_sets(self, tenant_id: str) -> list[dict[str, Any]]:
        rows = await self.pool.fetch(_SET_SELECT + " where s.tenant_id = $1 order by s.name", tenant_id)
        return [dict(r) for r in rows]

    async def get_set(self, tenant_id: str, set_id: str) -> dict[str, Any] | None:
        r = await self.pool.fetchrow(_SET_SELECT + " where s.tenant_id = $1 and s.id = $2", tenant_id, set_id)
        return dict(r) if r else None

    async def create_set(self, tenant_id: str, fields: dict[str, Any]) -> str:
        try:
            return await self.pool.fetchval(
                "insert into eval_set (tenant_id, name, description) values ($1, $2, $3) returning id::text",
                tenant_id, fields["name"], fields.get("description") or "",
            )
        except asyncpg.UniqueViolationError as exc:
            raise Conflict("A test set with this name already exists.") from exc

    async def update_set(self, tenant_id: str, set_id: str, fields: dict[str, Any]) -> None:
        if not fields:
            return
        clause, args = _set_clause(fields, SET_FIELDS, 3)
        try:
            await self.pool.execute(f"update eval_set set {clause}, updated_at = now() where tenant_id = $1 and id = $2", tenant_id, set_id, *args)
        except asyncpg.UniqueViolationError as exc:
            raise Conflict("A test set with this name already exists.") from exc

    async def delete_set(self, tenant_id: str, set_id: str) -> None:
        async with self.pool.acquire() as conn, conn.transaction():
            active = await conn.fetchval("select count(*) from eval_run where set_id = $1 and status = any($2::text[])", set_id, list(ACTIVE))
            if active:
                raise InUse("An evaluation of this test set is running. Cancel it first.")
            await conn.execute("delete from eval_set where tenant_id = $1 and id = $2", tenant_id, set_id)

    # ------------------------------------------------------------ cases
    async def list_cases(self, set_id: str) -> list[dict[str, Any]]:
        rows = await self.pool.fetch(_CASE_SELECT + " where set_id = $1 order by position, created_at", set_id)
        return [dict(r) for r in rows]

    async def get_case(self, set_id: str, case_id: str) -> dict[str, Any] | None:
        r = await self.pool.fetchrow(_CASE_SELECT + " where set_id = $1 and id = $2", set_id, case_id)
        return dict(r) if r else None

    async def add_cases(self, set_id: str, cases: list[dict[str, Any]], replace: bool = False) -> list[str]:
        async with self.pool.acquire() as conn, conn.transaction():
            if replace:
                await conn.execute("delete from eval_case where set_id = $1", set_id)
            start = await conn.fetchval("select coalesce(max(position), -1) + 1 from eval_case where set_id = $1", set_id)
            ids = []
            for i, c in enumerate(cases):
                ids.append(await conn.fetchval(
                    """insert into eval_case (set_id, position, question, expected, must_include, must_not_include, tags)
                       values ($1, $2, $3, $4, $5, $6, $7) returning id::text""",
                    set_id, start + i, c["question"], c.get("expected") or "", c.get("must_include") or [], c.get("must_not_include") or [], c.get("tags") or [],
                ))
            await conn.execute("update eval_set set updated_at = now() where id = $1", set_id)
            return ids

    async def update_case(self, set_id: str, case_id: str, fields: dict[str, Any]) -> None:
        if not fields:
            return
        clause, args = _set_clause(fields, CASE_FIELDS, 3)
        await self.pool.execute(f"update eval_case set {clause}, updated_at = now() where set_id = $1 and id = $2", set_id, case_id, *args)
        await self.pool.execute("update eval_set set updated_at = now() where id = $1", set_id)

    async def delete_case(self, set_id: str, case_id: str) -> None:
        await self.pool.execute("delete from eval_case where set_id = $1 and id = $2", set_id, case_id)
        await self.pool.execute("update eval_set set updated_at = now() where id = $1", set_id)

    # ------------------------------------------------------------ runs
    async def create_run(self, tenant_id: str, fields: dict[str, Any], cases: list[dict[str, Any]]) -> str:
        async with self.pool.acquire() as conn, conn.transaction():
            run_id = await conn.fetchval(
                """insert into eval_run (tenant_id, set_id, agent_id, agent_name, agent, label, options)
                   values ($1, $2, $3, $4, $5, $6, $7) returning id::text""",
                tenant_id, fields["set_id"], fields["agent_id"], fields.get("agent_name"), fields.get("agent"), fields.get("label") or "", fields.get("options") or {},
            )
            for i, c in enumerate(cases):
                await conn.execute(
                    """insert into eval_result (run_id, case_id, position, question, expected, must_include, must_not_include)
                       values ($1, $2, $3, $4, $5, $6, $7)""",
                    run_id, c["id"], i, c["question"], c.get("expected") or "", c.get("must_include") or [], c.get("must_not_include") or [],
                )
            return run_id

    async def list_runs(self, tenant_id: str, set_id: str | None, agent_id: str | None, limit: int, offset: int) -> tuple[list[dict[str, Any]], int]:
        rows = await self.pool.fetch(
            "select count(*) over () as total, " + _RUN_SELECT.strip().removeprefix("select ")
            + """ where r.tenant_id = $1 and ($2::uuid is null or r.set_id = $2) and ($3::uuid is null or r.agent_id = $3)
                  order by r.created_at desc limit $4 offset $5""",
            tenant_id, set_id, agent_id, limit, offset,
        )
        total = rows[0]["total"] if rows else 0
        return [{k: v for k, v in dict(r).items() if k != "total"} for r in rows], total

    async def get_run(self, tenant_id: str | None, run_id: str) -> dict[str, Any] | None:
        r = await self.pool.fetchrow(_RUN_SELECT + " where r.id = $1 and ($2::uuid is null or r.tenant_id = $2)", run_id, tenant_id)
        return dict(r) if r else None

    async def update_run(self, run_id: str, fields: dict[str, Any]) -> None:
        if not fields:
            return
        clause, args = _set_clause(fields, RUN_FIELDS, 2)
        await self.pool.execute(f"update eval_run set {clause} where id = $1", run_id, *args)

    async def delete_run(self, tenant_id: str, run_id: str) -> None:
        await self.pool.execute("delete from eval_run where tenant_id = $1 and id = $2", tenant_id, run_id)

    async def has_active_run(self, tenant_id: str, set_id: str, agent_id: str) -> bool:
        return bool(await self.pool.fetchval(
            "select count(*) from eval_run where tenant_id = $1 and set_id = $2 and agent_id = $3 and status = any($4::text[])",
            tenant_id, set_id, agent_id, list(ACTIVE),
        ))

    # ------------------------------------------------------------ results
    async def results(self, run_id: str) -> list[dict[str, Any]]:
        rows = await self.pool.fetch(_RESULT_SELECT + " where run_id = $1 order by position", run_id)
        return [dict(r) for r in rows]

    async def update_result(self, result_id: str, fields: dict[str, Any]) -> None:
        if not fields:
            return
        clause, args = _set_clause(fields, RESULT_FIELDS, 2)
        await self.pool.execute(f"update eval_result set {clause} where id = $1", result_id, *args)

    # ------------------------------------------------------------ mirrored KAP jobs and traces (top-level store)
    async def mirror_job(self, tenant_id: str, agent: dict[str, Any], job: dict[str, Any]) -> None:
        """Keep the KAP job in kap_job (Monitoring) and link its trace once it is finished."""
        if await kap_store.upsert_kap_job(self.pool, tenant_id, agent, job):
            await queue.enqueue(self.pool, "link_trace", {"job_id": job["id"]}, tenant_id=tenant_id, idempotency_key=f"link:{job['id']}")

    async def trace_status(self, job_id: str) -> str | None:
        return await self.pool.fetchval("select trace_status from kap_job where id = $1", job_id)

    async def trace(self, job_id: str) -> Trace | None:
        job = await kap_store.get_job(self.pool, job_id)
        if job is None:
            return None
        calls = await kap_store.calls_for_job(self.pool, job_id)
        return build_trace(job_id, job["created_at"], job["running_at"], job["finished_at"], calls)
