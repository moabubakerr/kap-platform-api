"""Platform REST API (slice 1).

Conventions follow KAP so the console sees one platform: pages as
`{items, total, limit, offset, has_next}`, errors as `{type, message, data}`.

  GET  /health
  POST /hooks/kap/t/{tenant_id}/agents/{agent_id}/jobs   KAP job-completion callback
  POST /api/t/{tenant_id}/agents/{agent_id}/jobs         submit a KAP agent job with our callback
  GET  /api/traces                                       mirrored agent jobs with trace status
  GET  /api/traces/{job_id}                              one job with its trace steps
  *    /api/t/{tenant_id}/kap/{path}                      KAP passthrough for the console hubs (kap_proxy.py)

Console auth in slice 1 is a static bearer key (PLATFORM_API_KEYS); it is
replaced by the platform's own users, groups and grants in a later slice.
"""
from __future__ import annotations

import hmac
from contextlib import asynccontextmanager
from typing import Any

import asyncpg
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

from .. import queue, store
from . import kap_proxy
from ..config import Settings
from ..kap.client import KapClient, KapError
from ..traces.build import build_trace


class SubmitJobIn(BaseModel):
    user_prompt: str | None = None
    payload: dict[str, Any] | None = None


def _err(status: int, kind: str, message: str, data: dict[str, Any] | None = None) -> HTTPException:
    return HTTPException(status_code=status, detail={"type": kind, "message": message, "data": data or {}})


def create_api(settings: Settings, pool_factory=None, kap_factory=None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        from ..db import create_pool

        app.state.pool = await (pool_factory or (lambda: create_pool(settings.database_url)))()
        app.state.kap = (kap_factory or (lambda: KapClient(settings.kap_base_url, settings.tenants(), settings.kap_timeout_s)))()
        try:
            yield
        finally:
            await app.state.kap.aclose()
            await app.state.pool.close()

    app = FastAPI(title="KAP platform API", version="0.1.0", lifespan=lifespan)

    @app.exception_handler(HTTPException)
    async def http_error(_: Request, exc: HTTPException) -> JSONResponse:
        body = exc.detail if isinstance(exc.detail, dict) and "type" in exc.detail else {"type": "error", "message": str(exc.detail), "data": {}}
        return JSONResponse(body, status_code=exc.status_code)

    @app.exception_handler(KapError)
    async def kap_error(_: Request, exc: KapError) -> JSONResponse:
        status = exc.status if exc.status in (400, 403, 404, 409, 422, 429) else 502
        return JSONResponse({"type": "kap_error", "message": str(exc), "data": {"kap_status": exc.status, "kap_body": exc.body}}, status_code=status)

    def pool(request: Request) -> asyncpg.Pool:
        return request.app.state.pool

    def kap(request: Request) -> KapClient:
        return request.app.state.kap

    def require_key(authorization: str | None = Header(default=None)) -> None:
        if not settings.api_keys:
            raise _err(503, "auth_not_configured", "No API keys are configured (PLATFORM_API_KEYS).")
        token = (authorization or "").removeprefix("Bearer ").strip()
        if not token or not any(hmac.compare_digest(token, k) for k in settings.api_keys):
            raise _err(401, "unauthorized", "Missing or invalid bearer key.")

    @app.get("/health")
    async def health(p: asyncpg.Pool = Depends(pool)) -> dict[str, Any]:
        try:
            await p.fetchval("select 1")
            db = "ok"
        except Exception as exc:  # report, don't crash the probe
            db = f"error: {type(exc).__name__}"
        return {"status": "ok" if db == "ok" else "degraded", "database": db}

    # ------------------------------------------------------------ KAP callback
    @app.post("/hooks/kap/t/{tenant_id}/agents/{agent_id}/jobs", status_code=204)
    async def kap_callback(tenant_id: str, agent_id: str, request: Request, x_platform_hook_token: str | None = Header(default=None), p: asyncpg.Pool = Depends(pool)) -> None:
        if not settings.hook_token or not x_platform_hook_token or not hmac.compare_digest(x_platform_hook_token, settings.hook_token):
            raise _err(401, "unauthorized", "Invalid hook token.")
        body = await request.json()
        job_id = body.get("job_id")
        if not job_id:
            raise _err(422, "validation_error", "Callback body has no job_id.")
        # KAP retries failed deliveries; the idempotency key makes repeats harmless.
        await queue.enqueue(p, "fetch_job", {"agent_id": agent_id, "job_id": job_id}, tenant_id=tenant_id, idempotency_key=f"fetch:{job_id}:{body.get('status')}", priority=1)

    # ------------------------------------------------------------ agents (for the playground)
    AGENT_FIELDS = ("id", "name", "type", "description", "model_ref", "collections", "allowed_tools", "config", "timeout_seconds", "request_limit", "tool_calls_limit")

    @app.get("/api/t/{tenant_id}/agents", dependencies=[Depends(require_key)])
    async def list_agents(tenant_id: str, k: KapClient = Depends(kap)) -> dict[str, Any]:
        try:
            agents = await k.list_agents(tenant_id)
        except KeyError:
            raise _err(404, "tenant_not_configured", "This tenant is not configured in the platform layer.")
        items = [{f: a.get(f) for f in AGENT_FIELDS} for a in agents]
        return {"items": items, "total": len(items), "limit": len(items), "offset": 0, "has_next": False}

    # ------------------------------------------------------------ submit through the layer
    @app.post("/api/t/{tenant_id}/agents/{agent_id}/jobs", status_code=201, dependencies=[Depends(require_key)])
    async def submit_job(tenant_id: str, agent_id: str, body: SubmitJobIn, p: asyncpg.Pool = Depends(pool), k: KapClient = Depends(kap)) -> dict[str, Any]:
        if body.user_prompt is None and body.payload is None:
            raise _err(422, "validation_error", "Provide user_prompt and/or payload.")
        job = await k.submit_agent_job(
            tenant_id,
            agent_id,
            user_prompt=body.user_prompt,
            payload=body.payload,
            callback_url=f"{settings.public_base_url.rstrip('/')}/hooks/kap/t/{tenant_id}/agents/{agent_id}/jobs",
            callback_headers={"X-Platform-Hook-Token": settings.hook_token} if settings.hook_token else None,
        )
        # store the agent's name with the job now, so traces never show a bare id until the next sync
        try:
            name = (await k.get_agent(tenant_id, agent_id)).get("name")
        except KapError:
            name = None
        await store.upsert_kap_job(p, tenant_id, {"id": agent_id, "name": name}, job)
        return job

    # ------------------------------------------------------------ KAP passthrough (console hubs)
    @app.api_route("/api/t/{tenant_id}/kap/{path:path}", methods=["GET", "POST", "PATCH", "PUT", "DELETE"], dependencies=[Depends(require_key)])
    async def kap_passthrough(tenant_id: str, path: str, request: Request, k: KapClient = Depends(kap)) -> Response:
        try:
            k.tenant(tenant_id)
            return await kap_proxy.forward(k, tenant_id, request.method, path, request)
        except KeyError:
            raise _err(404, "tenant_not_configured", "This tenant is not configured in the platform layer.")
        except kap_proxy.ProxyRefused as exc:
            raise _err(exc.status, exc.kind, exc.message)
        except ValueError:
            raise _err(422, "validation_error", "The request body is not valid JSON.")

    # ------------------------------------------------------------ traces
    @app.get("/api/traces", dependencies=[Depends(require_key)])
    async def list_traces(tenant_id: str | None = None, limit: int = Query(20, ge=1, le=200), offset: int = Query(0, ge=0), p: asyncpg.Pool = Depends(pool)) -> dict[str, Any]:
        items, total = await store.list_jobs(p, tenant_id, limit, offset)
        return {"items": items, "total": total, "limit": limit, "offset": offset, "has_next": offset + len(items) < total}

    @app.get("/api/traces/{job_id}", dependencies=[Depends(require_key)])
    async def get_trace(job_id: str, p: asyncpg.Pool = Depends(pool), k: KapClient = Depends(kap)) -> dict[str, Any]:
        job = await store.get_job(p, job_id)
        if job is None:
            raise _err(404, "not_found", "No such job (it may not have been synced from KAP yet).")
        calls = await store.calls_for_job(p, job_id)
        trace = build_trace(job_id, job["created_at"], job["running_at"], job["finished_at"], calls)
        return {"job": job, "agent": await _agent_summary(k, job["tenant_id"], job["agent_id"]), "trace": trace.as_dict()}

    async def _agent_summary(k: KapClient, tenant_id: str, agent_id: str) -> dict[str, Any] | None:
        """The agent's limits and model, live from KAP (best effort: the trace stands without it)."""
        try:
            a = await k.get_agent(tenant_id, agent_id)
        except (KapError, KeyError):
            return None
        keys = ("id", "name", "type", "model_ref", "collections", "allowed_tools", "timeout_seconds", "max_retries", "request_limit", "tool_calls_limit", "task_priority", "max_in_flight_tasks")
        return {key: a.get(key) for key in keys}

    return app
