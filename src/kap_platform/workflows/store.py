"""Reads and writes for wf_workflow, wf_run and wf_step.

`WfStore` is the only class touching SQL here, so tests can swap in an
in-memory store with the same methods.
"""
from __future__ import annotations

from typing import Any

import asyncpg

from .. import queue
from .. import store as kap_store
from ..traces.build import Trace, build_trace
from .graph import Step

ACTIVE = ("queued", "running", "waiting")
RUN_FIELDS = ("status", "answer", "error", "fallback_used", "agent_jobs", "polls", "started_at", "finished_at")
STEP_FIELDS = ("status", "seq", "input", "output", "note", "chosen", "job_id", "attempts", "started_at", "finished_at")

_RUN_SELECT = """
select id::text, tenant_id::text, workflow_id, workflow_name, version, trigger, input, cfg, agents, status, answer, error,
       fallback_used, agent_jobs, polls, created_by, created_at, started_at, finished_at
  from wf_run
"""
_STEP_SELECT = """
select node_id, kind, label, status, seq, input, output, note, chosen, job_id::text, attempts, started_at, finished_at
  from wf_step
"""


def _set_clause(fields: dict[str, Any], allowed: tuple[str, ...], start: int) -> tuple[str, list[Any]]:
    parts, args = [], []
    for k, v in fields.items():
        if k not in allowed:
            raise ValueError(f"unknown field {k}")
        args.append(v)
        parts.append(f"{k} = ${start + len(args) - 1}")
    return ", ".join(parts), args


def step_of(r: Any) -> Step:
    d = dict(r)
    return Step(**{k: d.get(k) for k in ("node_id", "kind", "label", "status", "output", "note", "chosen", "seq", "job_id", "attempts", "input", "started_at", "finished_at")})


class WfStore:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    # ------------------------------------------------------------ workflows
    async def list_workflows(self, tenant_id: str) -> list[dict[str, Any]]:
        rows = await self.pool.fetch(
            """select w.id, w.doc, w.created_at, w.updated_at,
                      (select jsonb_build_object('id', r.id, 'status', r.status, 'version', r.version, 'created_at', r.created_at, 'finished_at', r.finished_at)
                         from wf_run r where r.tenant_id = w.tenant_id and r.workflow_id = w.id order by r.created_at desc limit 1) as last_run
                 from wf_workflow w where w.tenant_id = $1 order by w.id""",
            tenant_id,
        )
        return [dict(r) for r in rows]

    async def get_workflow(self, tenant_id: str, wf_id: str) -> dict[str, Any] | None:
        r = await self.pool.fetchrow("select id, doc, created_at, updated_at from wf_workflow where tenant_id = $1 and id = $2", tenant_id, wf_id)
        return dict(r) if r else None

    async def put_workflow(self, tenant_id: str, wf_id: str, doc: dict[str, Any]) -> bool:
        """Insert or replace. True when it was created."""
        r = await self.pool.fetchrow(
            """insert into wf_workflow (tenant_id, id, doc) values ($1, $2, $3)
               on conflict (tenant_id, id) do update set doc = excluded.doc, updated_at = now()
               returning (xmax = 0) as created""",
            tenant_id, wf_id, doc,
        )
        return bool(r["created"])

    async def delete_workflow(self, tenant_id: str, wf_id: str) -> None:
        await self.pool.execute("delete from wf_workflow where tenant_id = $1 and id = $2", tenant_id, wf_id)

    # ------------------------------------------------------------ runs
    async def create_run(self, tenant_id: str, fields: dict[str, Any], steps: list[Step]) -> str:
        async with self.pool.acquire() as conn, conn.transaction():
            run_id = await conn.fetchval(
                """insert into wf_run (tenant_id, workflow_id, workflow_name, version, trigger, input, cfg, agents, created_by)
                   values ($1, $2, $3, $4, $5, $6, $7, $8, $9) returning id::text""",
                tenant_id, fields["workflow_id"], fields.get("workflow_name"), fields["version"], fields.get("trigger") or "test",
                fields["input"], fields["cfg"], fields.get("agents") or {}, fields.get("created_by"),
            )
            for s in steps:
                await conn.execute("insert into wf_step (run_id, node_id, kind, label) values ($1, $2, $3, $4)", run_id, s.node_id, s.kind, s.label)
            return run_id

    async def get_run(self, tenant_id: str | None, run_id: str) -> dict[str, Any] | None:
        r = await self.pool.fetchrow(_RUN_SELECT + " where id = $1 and ($2::uuid is null or tenant_id = $2)", run_id, tenant_id)
        return dict(r) if r else None

    async def list_runs(self, tenant_id: str, workflow_id: str | None, limit: int, offset: int) -> tuple[list[dict[str, Any]], int]:
        rows = await self.pool.fetch(
            "select count(*) over () as total, "
            "(select jsonb_agg(jsonb_build_array(s.node_id, s.status, s.seq) order by s.seq nulls last, s.node_id) from wf_step s where s.run_id = wf_run.id) as path, "
            + _RUN_SELECT.strip().removeprefix("select ")
            + " where tenant_id = $1 and ($2::text is null or workflow_id = $2) order by created_at desc limit $3 offset $4",
            tenant_id, workflow_id, limit, offset,
        )
        total = rows[0]["total"] if rows else 0
        return [{k: v for k, v in dict(r).items() if k != "total"} for r in rows], total

    async def update_run(self, run_id: str, fields: dict[str, Any]) -> None:
        if not fields:
            return
        clause, args = _set_clause(fields, RUN_FIELDS, 2)
        await self.pool.execute(f"update wf_run set {clause} where id = $1", run_id, *args)

    async def try_lease(self, run_id: str, seconds: float) -> bool:
        """Take the run for one tick (False when another tick holds it)."""
        r = await self.pool.fetchval(
            """update wf_run set ticking_until = now() + make_interval(secs => $2)
                where id = $1 and (ticking_until is null or ticking_until < now()) returning 1""",
            run_id, float(seconds),
        )
        return r is not None

    async def release(self, run_id: str) -> None:
        await self.pool.execute("update wf_run set ticking_until = null where id = $1", run_id)

    # ------------------------------------------------------------ steps
    async def steps(self, run_id: str) -> dict[str, Step]:
        rows = await self.pool.fetch(_STEP_SELECT + " where run_id = $1", run_id)
        return {r["node_id"]: step_of(r) for r in rows}

    async def update_step(self, run_id: str, node_id: str, fields: dict[str, Any]) -> None:
        if not fields:
            return
        clause, args = _set_clause(fields, STEP_FIELDS, 3)
        await self.pool.execute(f"update wf_step set {clause} where run_id = $1 and node_id = $2", run_id, node_id, *args)

    # ------------------------------------------------------------ mirrored KAP jobs and traces (top-level store)
    async def mirror_job(self, tenant_id: str, agent: dict[str, Any], job: dict[str, Any]) -> None:
        if await kap_store.upsert_kap_job(self.pool, tenant_id, agent, job):
            await queue.enqueue(self.pool, "link_trace", {"job_id": job["id"]}, tenant_id=tenant_id, idempotency_key=f"link:{job['id']}")

    async def trace(self, job_id: str) -> Trace | None:
        job = await kap_store.get_job(self.pool, job_id)
        if job is None:
            return None
        calls = await kap_store.calls_for_job(self.pool, job_id)
        return build_trace(job_id, job["created_at"], job["running_at"], job["finished_at"], calls)
