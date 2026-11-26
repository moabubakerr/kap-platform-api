"""REST routes for workflows (registered by api/app.py).

  GET                 /api/t/{tid}/workflows
  GET/PUT/DELETE      /api/t/{tid}/workflows/{id}                   the console's workflow document
  GET/POST            /api/t/{tid}/workflow-runs                    ?workflow_id= ; POST starts a run
  GET                 /api/t/{tid}/workflow-runs/{id}               with one entry per step
  POST                /api/t/{tid}/workflow-runs/{id}/cancel
  POST                /api/t/{tid}/workflow-runs/{id}/steps/{node}/review   a person approves or rejects a waiting step
"""
from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Literal

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field

from ..config import Settings
from ..kap.client import KapClient
from . import graph, runner
from .store import ACTIVE

WF_ID = r"^[a-z][a-z0-9_]{2,40}$"
DOC_MAX = 512_000


class DocIn(BaseModel):
    doc: dict[str, Any]


class RunIn(BaseModel):
    workflow_id: str = Field(pattern=WF_ID)
    input: str = Field(min_length=1, max_length=8000)
    version: str | None = Field(None, max_length=20)
    cfg: dict[str, Any] | None = None  # test of unsaved designer changes
    trigger: Literal["test", "api", "eval", "schedule"] = "test"
    created_by: str | None = Field(None, max_length=200)


class ReviewIn(BaseModel):
    approve: bool
    answer: str | None = Field(None, max_length=20000)
    comment: str | None = Field(None, max_length=2000)


def version_of(doc: dict[str, Any], v: str | None) -> dict[str, Any] | None:
    vers = doc.get("vers") or []
    if v:
        return next((x for x in vers if x.get("v") == v), None)
    return next((x for x in vers if x.get("st") == "Published"), None) or (vers[0] if vers else None)


def step_out(s: graph.Step) -> dict[str, Any]:
    return {k: getattr(s, k) for k in ("node_id", "kind", "label", "status", "seq", "input", "output", "note", "chosen", "job_id", "attempts", "started_at", "finished_at")}


def run_out(r: dict[str, Any], steps: dict[str, graph.Step] | None = None) -> dict[str, Any]:
    out = {k: r.get(k) for k in ("id", "workflow_id", "workflow_name", "version", "trigger", "input", "status", "answer", "error", "fallback_used", "agent_jobs", "created_by", "created_at", "started_at", "finished_at")}
    if "path" in r:  # list: [node id, status, seq] per step, in run order
        out["path"] = r.get("path") or []
    if steps is not None:
        out["cfg"] = r.get("cfg")
        order = sorted(steps.values(), key=lambda s: (s.seq is None, s.seq if s.seq is not None else 0, s.node_id))
        out["steps"] = [step_out(s) for s in order]
    return out


def wf_out(w: dict[str, Any]) -> dict[str, Any]:
    return {"id": w["id"], "doc": w["doc"], "last_run": w.get("last_run"), "created_at": w.get("created_at"), "updated_at": w.get("updated_at")}


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

    async def get_run(st: Any, tid: str, rid: str) -> dict[str, Any]:
        try:
            uuid.UUID(rid)
        except ValueError:
            raise err(404, "not_found", "No such workflow run.")
        r = await st.get_run(tid, rid)
        if r is None:
            raise err(404, "not_found", "No such workflow run.")
        return r

    # ------------------------------------------------------------ workflows
    @router.get("/workflows")
    async def list_workflows(tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        items = [wf_out(w) for w in await st.list_workflows(tid)]
        return _page(items, len(items), len(items), 0)

    @router.get("/workflows/{wf_id}")
    async def read_workflow(wf_id: str, tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        w = await st.get_workflow(tid, wf_id)
        if w is None:
            raise err(404, "not_found", "No such workflow.")
        return wf_out(w)

    @router.put("/workflows/{wf_id}")
    async def put_workflow(wf_id: str, body: DocIn, response: Response, tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        if not re.match(WF_ID, wf_id):
            raise err(422, "validation_error", "Workflow ID: lower-case letters, digits and underscores (3–41 characters, starting with a letter).")
        doc = {k: v for k, v in body.doc.items() if k != "id"}
        if not isinstance(doc.get("vers"), list) or not doc["vers"]:
            raise err(422, "validation_error", "doc.vers: a workflow needs at least one version.")
        if len(json.dumps(doc, default=str)) > DOC_MAX:
            raise err(422, "validation_error", "The workflow is too large.")
        created = await st.put_workflow(tid, wf_id, doc)
        response.status_code = 201 if created else 200
        return wf_out(await st.get_workflow(tid, wf_id))

    @router.delete("/workflows/{wf_id}", status_code=204)
    async def delete_workflow(wf_id: str, tid: str = Depends(tenant), st: Any = Depends(store)) -> Response:
        await st.delete_workflow(tid, wf_id)
        return Response(status_code=204)

    # ------------------------------------------------------------ runs
    @router.get("/workflow-runs")
    async def list_runs(workflow_id: str | None = None, limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0), tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        items, total = await st.list_runs(tid, workflow_id, limit, offset)
        return _page([run_out(r) for r in items], total, limit, offset)

    @router.post("/workflow-runs", status_code=201)
    async def start_run(body: RunIn, request: Request, tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        w = await st.get_workflow(tid, body.workflow_id)
        if w is None:
            raise err(404, "not_found", "No such workflow.")
        doc = w["doc"]
        if doc.get("dis"):
            raise err(409, "disabled", f"The workflow is disabled: {(doc['dis'] or {}).get('reason') or 'no reason given'}.")
        ver = version_of(doc, body.version)
        if ver is None:
            raise err(404, "not_found", f"The workflow has no version {body.version}." if body.version else "The workflow has no version.")
        if body.cfg is not None and body.trigger != "test":
            raise err(422, "validation_error", "Unsaved changes (cfg) can only be run as a test.")
        cfg = body.cfg if body.cfg is not None else ver.get("cfg") or {}
        kap: KapClient = request.app.state.kap
        agents = {a["name"]: a["id"] for a in await kap.list_agents(tid)}
        probs = graph.problems(cfg, agents)
        if probs:
            raise err(422, "workflow_invalid", "The workflow cannot run: " + " ".join(probs[:5]), {"problems": probs})
        used = {n[2] for n in graph.nodes_of(cfg) if n[1] in ("agent", "aggregate") and n[2]}
        steps = [graph.Step(n[0], n[1], graph.label_of(n)) for n in graph.nodes_of(cfg)]
        rid = await st.create_run(tid, {
            "workflow_id": body.workflow_id, "workflow_name": doc.get("n") or body.workflow_id, "version": ver.get("v") or "?",
            "trigger": body.trigger, "input": body.input.strip(), "cfg": cfg, "agents": {k: agents[k] for k in used}, "created_by": body.created_by,
        }, steps)
        run = await get_run(st, tid, rid)
        await runner.enqueue_tick(request.app.state.pool, run, "start")
        return run_out(run, await st.steps(rid))

    @router.get("/workflow-runs/{run_id}")
    async def read_run(run_id: str, tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        r = await get_run(st, tid, run_id)
        return run_out(r, await st.steps(run_id))

    @router.post("/workflow-runs/{run_id}/cancel")
    async def cancel_run(run_id: str, tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        r = await get_run(st, tid, run_id)
        if r["status"] in ACTIVE:
            steps = await st.steps(run_id)
            for s in steps.values():
                if s.status in ("pending", "running", "waiting"):
                    await st.update_step(run_id, s.node_id, {"status": "skipped", "note": "Cancelled"})
            await st.update_run(run_id, {"status": "cancelled", "error": "Cancelled", "finished_at": datetime.now(timezone.utc)})
        return run_out(await get_run(st, tid, run_id), await st.steps(run_id))

    @router.post("/workflow-runs/{run_id}/steps/{node_id}/review")
    async def review_step(run_id: str, node_id: str, body: ReviewIn, request: Request, tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        r = await get_run(st, tid, run_id)
        steps = await st.steps(run_id)
        s = steps.get(node_id)
        if s is None:
            raise err(404, "not_found", "No such step in this run.")
        if s.status != "waiting" or r["status"] not in ACTIVE:
            raise err(409, "conflict", "This step is not waiting for a review.")
        now = datetime.now(timezone.utc)
        who = body.comment.strip() if body.comment else ""
        if body.approve:
            text = (body.answer or "").strip() or (s.input or "")
            if not text:
                raise err(422, "validation_error", "Write the answer to return.")
            note = "Approved" + (" with an edited answer" if body.answer and body.answer.strip() != (s.input or "").strip() else "") + (f": {who}" if who else "")
            await st.update_step(run_id, node_id, {"status": "done", "output": text, "note": note, "finished_at": now})
        else:
            await st.update_step(run_id, node_id, {"status": "failed", "note": "Rejected" + (f": {who}" if who else ""), "finished_at": now})
        await runner.enqueue_tick(request.app.state.pool, r, f"review:{node_id}:{now.timestamp():.0f}")
        return run_out(await get_run(st, tid, run_id), await st.steps(run_id))

    app.include_router(router)
