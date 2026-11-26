"""REST routes for evaluations (registered by api/app.py).

  GET/POST            /api/t/{tid}/eval-sets
  GET/PATCH/DELETE    /api/t/{tid}/eval-sets/{id}                 GET includes the cases
  POST                /api/t/{tid}/eval-sets/{id}/cases           add cases (or replace all: "replace": true)
  PATCH/DELETE        /api/t/{tid}/eval-sets/{id}/cases/{case_id}
  GET/POST            /api/t/{tid}/eval-runs                      ?set_id=&agent_id= ; POST starts a run
  GET/DELETE          /api/t/{tid}/eval-runs/{id}                 GET includes one result per question
  POST                /api/t/{tid}/eval-runs/{id}/cancel
  GET/POST            /api/t/{tid}/eval-schedules                 ?set_id= ; run a test set on a schedule and/or after new data
  PATCH/DELETE        /api/t/{tid}/eval-schedules/{id}
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field, field_validator

from ..config import Settings
from ..kap.client import KapClient, KapError
from ..workflows import runner as wf_runner
from . import runner, score, start
from .store import ACTIVE, Conflict, InUse

NAME = r"^\S(.{0,98}\S)?$"
MAX_CASES = 500
AGENT_KEYS = ("id", "name", "type", "model_ref", "collections", "allowed_tools", "updated_at")


def _terms(v: list[str] | None) -> list[str]:
    return [t.strip() for t in (v or []) if t and t.strip()]


class CaseIn(BaseModel):
    question: str = Field(min_length=1, max_length=4000)
    expected: str = Field("", max_length=8000)
    must_include: list[str] = Field(default_factory=list, max_length=30)
    must_not_include: list[str] = Field(default_factory=list, max_length=30)
    tags: list[str] = Field(default_factory=list, max_length=10)

    @field_validator("must_include", "must_not_include", "tags")
    @classmethod
    def _clean(cls, v: list[str]) -> list[str]:
        out = _terms(v)
        if any(len(t) > 300 for t in out):
            raise ValueError("each term is at most 300 characters")
        return out

    @field_validator("question")
    @classmethod
    def _q(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("the question is empty")
        return v.strip()


class CasePatch(BaseModel):
    question: str | None = Field(None, min_length=1, max_length=4000)
    expected: str | None = Field(None, max_length=8000)
    must_include: list[str] | None = Field(None, max_length=30)
    must_not_include: list[str] | None = Field(None, max_length=30)
    tags: list[str] | None = Field(None, max_length=10)
    position: int | None = Field(None, ge=0)

    @field_validator("must_include", "must_not_include", "tags")
    @classmethod
    def _clean(cls, v: list[str] | None) -> list[str] | None:
        return None if v is None else _terms(v)


class SetIn(BaseModel):
    name: str = Field(pattern=NAME)
    description: str = Field("", max_length=2000)
    cases: list[CaseIn] = Field(default_factory=list, max_length=MAX_CASES)


class SetPatch(BaseModel):
    name: str | None = Field(None, pattern=NAME)
    description: str | None = Field(None, max_length=2000)


class CasesIn(BaseModel):
    cases: list[CaseIn] = Field(min_length=1, max_length=MAX_CASES)
    replace: bool = False


class RunOptions(BaseModel):
    judge: bool = True
    min_correctness: int = Field(4, ge=0, le=5)
    min_grounding: int = Field(0, ge=0, le=5)
    parallel: int = Field(2, ge=1, le=10)


class RunIn(RunOptions):
    set_id: str
    agent_id: str | None = None
    workflow_id: str | None = Field(None, max_length=41)
    version: str | None = Field(None, max_length=20)  # workflow version; default: the published one
    label: str = Field("", max_length=200)


class ScheduleIn(BaseModel):
    set_id: str
    agent_id: str | None = None
    workflow_id: str | None = Field(None, max_length=41)
    every_minutes: int | None = Field(None, ge=60, le=60 * 24 * 31)
    on_data: bool = False
    min_pass_rate: float | None = Field(None, ge=0, le=1)
    options: RunOptions = Field(default_factory=RunOptions)
    enabled: bool = True


class SchedulePatch(BaseModel):
    every_minutes: int | None = Field(None, ge=60, le=60 * 24 * 31)
    on_data: bool | None = None
    min_pass_rate: float | None = Field(None, ge=0, le=1)
    options: RunOptions | None = None
    enabled: bool | None = None


def schedule_out(s: dict[str, Any]) -> dict[str, Any]:
    return {k: s.get(k) for k in ("id", "set_id", "target_kind", "agent_id", "workflow_id", "every_minutes", "on_data", "min_pass_rate", "options", "enabled", "next_run_at", "data_seen_at", "last_run_id", "created_at", "updated_at")}


# ---------------------------------------------------------------- output shapes
def set_out(s: dict[str, Any], cases: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    out = {k: s.get(k) for k in ("id", "name", "description", "case_count", "last_run", "created_at", "updated_at")}
    if cases is not None:
        out["cases"] = [case_out(c) for c in cases]
        out["case_count"] = len(cases)
    return out


def case_out(c: dict[str, Any]) -> dict[str, Any]:
    return {k: c.get(k) for k in ("id", "position", "question", "expected", "must_include", "must_not_include", "tags", "created_at", "updated_at")}


def result_out(r: dict[str, Any]) -> dict[str, Any]:
    return {k: r.get(k) for k in (
        "id", "case_id", "position", "question", "expected", "must_include", "must_not_include", "status", "job_id", "wf_run_id", "job_status",
        "answer", "checks", "judge", "passed", "reason", "metrics", "error", "submitted_at", "answered_at", "scored_at",
    )}


def run_out(r: dict[str, Any], results: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    out = {k: r.get(k) for k in (
        "id", "set_id", "set_name", "agent_id", "agent_name", "agent", "label", "status", "summary", "error", "created_at", "started_at", "finished_at",
        "target_kind", "workflow_id", "version", "trigger", "regressed", "regress_reason",
    )}
    out["target_kind"] = out["target_kind"] or "agent"
    out["trigger"] = out["trigger"] or "manual"
    out["options"] = score.options_with_defaults(r.get("options"))
    if results is not None:
        out["results"] = [result_out(x) for x in results]
        out["summary"] = score.summarize(results)
    return out


def _page(items: list[Any], total: int, limit: int, offset: int) -> dict[str, Any]:
    return {"items": items, "total": total, "limit": limit, "offset": offset, "has_next": offset + len(items) < total}


def register(app: FastAPI, settings: Settings, require_key: Callable, err: Callable[..., HTTPException]) -> None:
    router = APIRouter(prefix="/api/t/{tenant_id}", dependencies=[Depends(require_key)])

    def tenant(tenant_id: str, request: Request) -> str:
        kap: KapClient = request.app.state.kap
        try:
            kap.tenant(tenant_id)
        except KeyError:
            raise err(404, "tenant_not_configured", "This tenant is not configured in the platform layer.")
        return tenant_id

    def store(request: Request) -> Any:
        return runner.SERVICES.store(request.app.state.pool)

    def check_id(value: str | None, what: str) -> None:
        if value is None:
            return
        try:
            uuid.UUID(value)
        except ValueError:
            raise err(404, "not_found", f"No such {what}.")

    async def get_set(st: Any, tid: str, sid: str) -> dict[str, Any]:
        check_id(sid, "test set")
        s = await st.get_set(tid, sid)
        if s is None:
            raise err(404, "not_found", "No such test set.")
        return s

    async def get_run(st: Any, tid: str, rid: str) -> dict[str, Any]:
        check_id(rid, "evaluation run")
        r = await st.get_run(tid, rid)
        if r is None:
            raise err(404, "not_found", "No such evaluation run.")
        return r

    # ------------------------------------------------------------ sets
    @router.get("/eval-sets")
    async def list_sets(tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        items = [set_out(s) for s in await st.list_sets(tid)]
        return _page(items, len(items), len(items), 0)

    @router.post("/eval-sets", status_code=201)
    async def create_set(body: SetIn, tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        try:
            sid = await st.create_set(tid, {"name": body.name.strip(), "description": body.description.strip()})
        except Conflict as exc:
            raise err(409, "conflict", str(exc))
        if body.cases:
            await st.add_cases(sid, [c.model_dump() for c in body.cases])
        return set_out(await get_set(st, tid, sid), await st.list_cases(sid))

    @router.get("/eval-sets/{set_id}")
    async def read_set(set_id: str, tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        s = await get_set(st, tid, set_id)
        return set_out(s, await st.list_cases(set_id))

    @router.patch("/eval-sets/{set_id}")
    async def patch_set(set_id: str, body: SetPatch, tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        await get_set(st, tid, set_id)
        fields = {k: v.strip() for k, v in body.model_dump(exclude_unset=True).items() if v is not None}
        try:
            await st.update_set(tid, set_id, fields)
        except Conflict as exc:
            raise err(409, "conflict", str(exc))
        return set_out(await get_set(st, tid, set_id), await st.list_cases(set_id))

    @router.delete("/eval-sets/{set_id}", status_code=204)
    async def delete_set(set_id: str, tid: str = Depends(tenant), st: Any = Depends(store)) -> Response:
        await get_set(st, tid, set_id)
        try:
            await st.delete_set(tid, set_id)
        except InUse as exc:
            raise err(409, "in_use", str(exc))
        return Response(status_code=204)

    # ------------------------------------------------------------ cases
    @router.post("/eval-sets/{set_id}/cases", status_code=201)
    async def add_cases(set_id: str, body: CasesIn, tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        s = await get_set(st, tid, set_id)
        existing = 0 if body.replace else int(s.get("case_count") or 0)
        if existing + len(body.cases) > MAX_CASES:
            raise err(422, "validation_error", f"A test set holds at most {MAX_CASES} questions.")
        await st.add_cases(set_id, [c.model_dump() for c in body.cases], replace=body.replace)
        return set_out(await get_set(st, tid, set_id), await st.list_cases(set_id))

    @router.patch("/eval-sets/{set_id}/cases/{case_id}")
    async def patch_case(set_id: str, case_id: str, body: CasePatch, tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        await get_set(st, tid, set_id)
        check_id(case_id, "question")
        if await st.get_case(set_id, case_id) is None:
            raise err(404, "not_found", "No such question.")
        fields = {k: v for k, v in body.model_dump(exclude_unset=True).items() if v is not None}
        if "question" in fields:
            fields["question"] = fields["question"].strip()
            if not fields["question"]:
                raise err(422, "validation_error", "question: the question is empty")
        await st.update_case(set_id, case_id, fields)
        return case_out(await st.get_case(set_id, case_id))

    @router.delete("/eval-sets/{set_id}/cases/{case_id}", status_code=204)
    async def delete_case(set_id: str, case_id: str, tid: str = Depends(tenant), st: Any = Depends(store)) -> Response:
        await get_set(st, tid, set_id)
        check_id(case_id, "question")
        await st.delete_case(set_id, case_id)
        return Response(status_code=204)

    # ------------------------------------------------------------ runs
    @router.get("/eval-runs")
    async def list_runs(
        set_id: str | None = None, agent_id: str | None = None, workflow_id: str | None = None,
        limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0),
        tid: str = Depends(tenant), st: Any = Depends(store),
    ) -> dict[str, Any]:
        for v, what in ((set_id, "test set"), (agent_id, "agent")):
            if v is not None:
                try:
                    uuid.UUID(v)
                except ValueError:
                    return _page([], 0, limit, offset)
        items, total = await st.list_runs(tid, set_id, agent_id, limit, offset, workflow_id)
        return _page([run_out(r) for r in items], total, limit, offset)

    @router.post("/eval-runs", status_code=201)
    async def start_run(body: RunIn, request: Request, tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        await get_set(st, tid, body.set_id)
        check_id(body.agent_id, "agent")
        options = body.model_dump(include={"judge", "min_correctness", "min_grounding", "parallel"})
        try:
            rid = await start.start_eval_run(
                st, request.app.state.kap, request.app.state.pool, tid, body.set_id,
                agent_id=body.agent_id, workflow_id=body.workflow_id, version=body.version, label=body.label, options=options,
            )
        except start.EvalRefused as exc:
            raise err(exc.status, exc.kind, exc.message)
        return run_out(await get_run(st, tid, rid), await st.results(rid))

    # ------------------------------------------------------------ schedules
    async def get_schedule(st: Any, tid: str, sid: str) -> dict[str, Any]:
        check_id(sid, "schedule")
        s = await st.get_schedule(tid, sid)
        if s is None:
            raise err(404, "not_found", "No such schedule.")
        return s

    @router.get("/eval-schedules")
    async def list_schedules(set_id: str | None = None, tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        check_id(set_id, "test set")
        items = [schedule_out(s) for s in await st.list_schedules(tid, set_id)]
        return _page(items, len(items), len(items), 0)

    @router.post("/eval-schedules", status_code=201)
    async def create_schedule(body: ScheduleIn, request: Request, tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        await get_set(st, tid, body.set_id)
        if bool(body.agent_id) == bool(body.workflow_id):
            raise err(422, "validation_error", "Choose an agent or a workflow to evaluate.")
        if not body.every_minutes and not body.on_data:
            raise err(422, "validation_error", "Choose when to run: on a schedule, after new data, or both.")
        check_id(body.agent_id, "agent")
        if body.agent_id:
            try:
                await request.app.state.kap.get_agent(tid, body.agent_id)
            except KapError as exc:
                if exc.status == 404:
                    raise err(404, "not_found", "KAP has no such agent.")
                raise
        elif await wf_runner.SERVICES.store(request.app.state.pool).get_workflow(tid, body.workflow_id) is None:
            raise err(404, "not_found", "No such workflow.")
        now = datetime.now(timezone.utc)
        sid = await st.create_schedule(tid, {
            "set_id": body.set_id, "target_kind": "agent" if body.agent_id else "workflow", "agent_id": body.agent_id, "workflow_id": body.workflow_id,
            "every_minutes": body.every_minutes, "on_data": body.on_data, "min_pass_rate": body.min_pass_rate, "options": body.options.model_dump(),
            "enabled": body.enabled, "next_run_at": now + timedelta(minutes=body.every_minutes) if body.every_minutes else None,
        })
        return schedule_out(await get_schedule(st, tid, sid))

    @router.patch("/eval-schedules/{sched_id}")
    async def patch_schedule(sched_id: str, body: SchedulePatch, tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        s = await get_schedule(st, tid, sched_id)
        fields = body.model_dump(exclude_unset=True)
        if "options" in fields and fields["options"] is None:
            fields.pop("options")
        every = fields.get("every_minutes", s["every_minutes"])
        if not every and not fields.get("on_data", s["on_data"]):
            raise err(422, "validation_error", "Choose when to run: on a schedule, after new data, or both.")
        if "every_minutes" in fields:
            fields["next_run_at"] = datetime.now(timezone.utc) + timedelta(minutes=every) if every else None
        await st.update_schedule(sched_id, fields)
        return schedule_out(await get_schedule(st, tid, sched_id))

    @router.delete("/eval-schedules/{sched_id}", status_code=204)
    async def delete_schedule(sched_id: str, tid: str = Depends(tenant), st: Any = Depends(store)) -> Response:
        await get_schedule(st, tid, sched_id)
        await st.delete_schedule(tid, sched_id)
        return Response(status_code=204)

    @router.get("/eval-runs/{run_id}")
    async def read_run(run_id: str, tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        r = await get_run(st, tid, run_id)
        return run_out(r, await st.results(run_id))

    @router.post("/eval-runs/{run_id}/cancel")
    async def cancel_run(run_id: str, tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        r = await get_run(st, tid, run_id)
        if r["status"] in ACTIVE:
            now = datetime.now(timezone.utc)
            for x in await st.results(run_id):
                if x["status"] not in ("scored", "error"):
                    await st.update_result(x["id"], {"status": "error", "passed": False, "error": "The run was cancelled.", "reason": "Cancelled", "scored_at": now})
            results = await st.results(run_id)
            await st.update_run(run_id, {"status": "cancelled", "finished_at": now, "summary": score.summarize(results)})
        return run_out(await get_run(st, tid, run_id), await st.results(run_id))

    @router.delete("/eval-runs/{run_id}", status_code=204)
    async def delete_run(run_id: str, tid: str = Depends(tenant), st: Any = Depends(store)) -> Response:
        r = await get_run(st, tid, run_id)
        if r["status"] in ACTIVE:
            raise err(409, "in_use", "The run is still going. Cancel it first.")
        await st.delete_run(tid, run_id)
        return Response(status_code=204)

    app.include_router(router)
