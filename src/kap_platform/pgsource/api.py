"""REST routes for the PostgreSQL data source (registered by api/app.py).

  POST/GET            /api/t/{tid}/pg-sources
  GET/PATCH/DELETE    /api/t/{tid}/pg-sources/{id}
  POST                /api/t/{tid}/pg-sources/{id}/test
  POST                /api/t/{tid}/pg-sources/{id}/preview
  POST/GET            /api/t/{tid}/pg-datasets
  GET/PATCH/DELETE    /api/t/{tid}/pg-datasets/{id}
  POST                /api/t/{tid}/pg-datasets/{id}/preview
  POST                /api/t/{tid}/pg-datasets/{id}/run
  GET                 /api/t/{tid}/pg-exports, /api/t/{tid}/pg-exports/{id}
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Literal

import asyncpg
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field

from ..config import Settings
from ..kap.client import KapClient
from . import build, export
from .secrets import SecretBox, SecretKeyMissing
from .source import PREVIEW_MAX, SourceUnavailable, SqlRejected, check_select, jsonable
from .store import Conflict, InUse

SslMode = Literal["disable", "allow", "prefer", "require", "verify-ca", "verify-full"]
Preset = Literal["scai_indicators", "custom"]
NAME = r"^[A-Za-z0-9][A-Za-z0-9 _.-]{0,99}$"
DATASET_NAME = r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$"  # part of the S3 key
COLLECTION_NAME = r"^[A-Za-z0-9_]{4,29}$"  # KAP: [A-Za-z0-9_], 4–32 chars; the connector adds "_pg"
KAP_KEYS = ("connector", "pipeline", "collection", "collection_id", "pipeline_id", "provisioned_at")


class SourceIn(BaseModel):
    name: str = Field(pattern=NAME)
    host: str = Field(min_length=1, max_length=255)
    port: int = Field(5432, ge=1, le=65535)
    database: str = Field(min_length=1, max_length=255)
    username: str = Field(min_length=1, max_length=255)
    password: str | None = None
    sslmode: SslMode = "prefer"


class SourcePatch(BaseModel):
    name: str | None = Field(None, pattern=NAME)
    host: str | None = Field(None, min_length=1, max_length=255)
    port: int | None = Field(None, ge=1, le=65535)
    database: str | None = Field(None, min_length=1, max_length=255)
    username: str | None = Field(None, min_length=1, max_length=255)
    password: str | None = None
    sslmode: SslMode | None = None


class PreviewIn(BaseModel):
    sql: str = Field(min_length=1)
    limit: int = Field(20, ge=1, le=PREVIEW_MAX)


class Queries(BaseModel):
    registry: str | None = None
    observations: str | None = None
    narratives: str | None = None


class DatasetIn(BaseModel):
    source_id: str
    name: str = Field(pattern=DATASET_NAME)
    preset: Preset
    queries: Queries | None = None
    published_only: bool = True
    collection_name: str = Field(pattern=COLLECTION_NAME)
    schedule_minutes: int | None = Field(None, ge=1, le=60 * 24 * 31)


class DatasetPatch(BaseModel):
    source_id: str | None = None
    name: str | None = Field(None, pattern=DATASET_NAME)
    preset: Preset | None = None
    queries: Queries | None = None
    published_only: bool | None = None
    collection_name: str | None = Field(None, pattern=COLLECTION_NAME)
    schedule_minutes: int | None = Field(None, ge=1, le=60 * 24 * 31)


# ---------------------------------------------------------------- output shapes
def source_out(r: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": r["id"], "name": r["name"], "host": r["host"], "port": r["port"], "database": r["database"],
        "username": r["username"], "sslmode": r["sslmode"], "password_set": bool(r.get("password_enc")),
        "last_test": r.get("last_test"), "created_at": r["created_at"], "updated_at": r["updated_at"],
    }


def dataset_out(r: dict[str, Any]) -> dict[str, Any]:
    kap = r.get("kap")
    return {
        "id": r["id"], "source_id": r["source_id"], "source_name": r.get("source_name"), "name": r["name"], "preset": r["preset"],
        "queries": r.get("queries") or {}, "published_only": r["published_only"], "collection_name": r["collection_name"],
        "schedule_minutes": r.get("schedule_minutes"), "next_run_at": r.get("next_run_at"),
        "kap": {k: kap.get(k) for k in KAP_KEYS} if kap else None,
        "last_run": r.get("last_run"), "created_at": r["created_at"], "updated_at": r["updated_at"],
    }


def run_out(r: dict[str, Any]) -> dict[str, Any]:
    counts = r.get("counts") or {}
    kap = r.get("kap")
    return {
        "id": r["id"], "dataset_id": r["dataset_id"], "dataset_name": r.get("dataset_name"), "trigger": r["trigger"], "status": r["status"],
        "created_at": r["created_at"], "started_at": r.get("started_at"), "finished_at": r.get("finished_at"),
        "steps": r.get("steps") or export.initial_steps(),
        "counts": {k: counts.get(k) for k in ("registry", "observations", "narratives")},
        "files": [{"name": f.get("name"), "key": f.get("key"), "bytes": f.get("bytes")} for f in (r.get("files") or [])],
        "kap": {k: kap.get(k) for k in ("collection", "pipeline_job_id", "state", "progress", "result")} if kap else None,
        "error": r.get("error"),
    }


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
        return export.SERVICES.store(request.app.state.pool)

    def box() -> SecretBox:
        try:
            return SecretBox(settings.secret_key)
        except SecretKeyMissing as exc:
            raise err(503, "secret_key_not_configured", str(exc))

    def check_id(value: str | None, what: str) -> None:
        if value is None:
            return
        try:
            uuid.UUID(value)
        except ValueError:
            raise err(404, "not_found", f"No such {what}.")

    async def get_source(st: Any, tid: str, sid: str) -> dict[str, Any]:
        check_id(sid, "source")
        src = await st.get_source(tid, sid)
        if src is None:
            raise err(404, "not_found", "No such source.")
        return src

    async def get_dataset(st: Any, tid: str, did: str) -> dict[str, Any]:
        check_id(did, "dataset")
        ds = await st.get_dataset(tid, did)
        if ds is None:
            raise err(404, "not_found", "No such dataset.")
        return ds

    def open_session(src: dict[str, Any]):
        try:
            params = export.source_params(src, settings)
        except SecretKeyMissing as exc:
            raise err(503, "secret_key_not_configured", str(exc))
        return export.SERVICES.open(params)

    # ------------------------------------------------------------ sources
    @router.post("/pg-sources", status_code=201)
    async def create_source(body: SourceIn, tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        fields = body.model_dump(exclude={"password"})
        if body.password:
            fields["password_enc"] = box().encrypt(body.password)
        try:
            return source_out(await st.create_source(tid, fields))
        except Conflict as exc:
            raise err(409, "name_taken", str(exc))

    @router.get("/pg-sources")
    async def list_sources(limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0), tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        rows, total = await st.list_sources(tid, limit, offset)
        return _page([source_out(r) for r in rows], total, limit, offset)

    @router.get("/pg-sources/{source_id}")
    async def read_source(source_id: str, tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        return source_out(await get_source(st, tid, source_id))

    @router.patch("/pg-sources/{source_id}")
    async def patch_source(source_id: str, body: SourcePatch, tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        await get_source(st, tid, source_id)
        fields = body.model_dump(exclude_unset=True, exclude={"password"})
        fields = {k: v for k, v in fields.items() if v is not None}
        if "password" in body.model_fields_set:
            fields["password_enc"] = box().encrypt(body.password) if body.password else None
        if fields.keys() & {"host", "port", "database", "username", "password_enc", "sslmode"}:
            fields["last_test"] = None  # the old result no longer describes this connection
        try:
            return source_out(await st.update_source(tid, source_id, fields))
        except Conflict as exc:
            raise err(409, "name_taken", str(exc))

    @router.delete("/pg-sources/{source_id}", status_code=204)
    async def delete_source(source_id: str, tid: str = Depends(tenant), st: Any = Depends(store)) -> Response:
        await get_source(st, tid, source_id)
        n = await st.count_datasets(tid, source_id)
        if n:
            raise err(409, "source_in_use", f"{n} dataset(s) use this source; delete them first.", {"datasets": n})
        try:
            await st.delete_source(tid, source_id)
        except InUse as exc:
            raise err(409, "source_in_use", str(exc))
        return Response(status_code=204)

    @router.post("/pg-sources/{source_id}/test")
    async def test_source(source_id: str, tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        src = await get_source(st, tid, source_id)
        result: dict[str, Any] = {"ok": False, "message": "", "server_version": None, "tables": []}
        try:
            async with open_session(src) as s:
                result["server_version"] = await s.server_version()
                result["tables"] = [{"schema": t["schema"], "name": t["name"], "rows_estimate": t["rows_estimate"]} for t in await s.tables()]
            result["ok"] = True
            result["message"] = f"Connected (PostgreSQL {result['server_version']}); {len(result['tables'])} tables and views readable."
        except SourceUnavailable as exc:
            result["message"] = f"Cannot connect: {exc}"
        except asyncpg.PostgresError as exc:
            result["message"] = f"Connected, but reading the catalog failed: {exc}"
        await st.update_source(tid, source_id, {"last_test": {"ok": result["ok"], "message": result["message"], "at": datetime.now(timezone.utc).isoformat()}})
        return result

    @router.post("/pg-sources/{source_id}/preview")
    async def preview_sql(source_id: str, body: PreviewIn, tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        src = await get_source(st, tid, source_id)
        try:
            sql = check_select(body.sql)
        except SqlRejected as exc:
            raise err(422, "sql_rejected", str(exc))
        try:
            async with open_session(src) as s:
                cols, rows, truncated = await s.sample(sql, body.limit)
        except SourceUnavailable as exc:
            raise err(502, "source_unavailable", f"Cannot connect to the source database: {exc}")
        except asyncpg.PostgresError as exc:
            raise err(422, "query_error", str(exc), {"sqlstate": getattr(exc, "sqlstate", None)})
        return {"columns": cols, "rows": [[jsonable(v) for v in r] for r in rows], "truncated": truncated}

    # ------------------------------------------------------------ datasets
    def _queries(preset: str, q: Queries | dict | None) -> dict[str, str]:
        data = q.model_dump() if isinstance(q, Queries) else dict(q or {})
        data = {k: v for k, v in data.items() if v and v.strip()}
        if preset == "custom":
            missing = [k for k in ("registry", "observations") if k not in data]
            if missing:
                raise err(422, "validation_error", f"A custom dataset needs the {' and '.join(missing)} quer{'ies' if len(missing) > 1 else 'y'}.", {"missing": missing})
            for k, v in data.items():
                try:
                    check_select(v)
                except SqlRejected as exc:
                    raise err(422, "sql_rejected", f"{k}: {exc}", {"file": k})
        return data

    def _next_run(minutes: int | None) -> datetime | None:
        return datetime.now(timezone.utc) + timedelta(minutes=minutes) if minutes else None

    @router.post("/pg-datasets", status_code=201)
    async def create_dataset(body: DatasetIn, tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        check_id(body.source_id, "source")
        if await st.get_source(tid, body.source_id) is None:
            raise err(422, "validation_error", "source_id does not name a source of this tenant.", {"field": "source_id"})
        fields = {
            "source_id": body.source_id, "name": body.name, "preset": body.preset, "queries": _queries(body.preset, body.queries),
            "published_only": body.published_only, "collection_name": body.collection_name,
            "schedule_minutes": body.schedule_minutes, "next_run_at": _next_run(body.schedule_minutes),
        }
        try:
            return dataset_out(await st.create_dataset(tid, fields))
        except Conflict as exc:
            raise err(409, "name_taken", str(exc))

    @router.get("/pg-datasets")
    async def list_datasets(source_id: str | None = None, limit: int = Query(50, ge=1, le=200), offset: int = Query(0, ge=0), tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        if source_id is not None:
            try:
                uuid.UUID(source_id)
            except ValueError:
                return _page([], 0, limit, offset)
        rows, total = await st.list_datasets(tid, source_id, limit, offset)
        return _page([dataset_out(r) for r in rows], total, limit, offset)

    @router.get("/pg-datasets/{dataset_id}")
    async def read_dataset(dataset_id: str, tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        return dataset_out(await get_dataset(st, tid, dataset_id))

    @router.patch("/pg-datasets/{dataset_id}")
    async def patch_dataset(dataset_id: str, body: DatasetPatch, tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        ds = await get_dataset(st, tid, dataset_id)
        given = body.model_fields_set
        fields: dict[str, Any] = {}
        for k in ("source_id", "name", "preset", "published_only", "collection_name"):
            v = getattr(body, k)
            if k in given and v is not None and v != ds.get(k):
                fields[k] = v
        if "source_id" in fields:
            check_id(fields["source_id"], "source")
            if await st.get_source(tid, fields["source_id"]) is None:
                raise err(422, "validation_error", "source_id does not name a source of this tenant.", {"field": "source_id"})
        preset = fields.get("preset", ds["preset"])
        if "queries" in given or "preset" in fields:
            fields["queries"] = _queries(preset, body.queries if "queries" in given else ds.get("queries"))
        if "schedule_minutes" in given and body.schedule_minutes != ds.get("schedule_minutes"):
            fields["schedule_minutes"] = body.schedule_minutes
            fields["next_run_at"] = _next_run(body.schedule_minutes)
        if fields.keys() & {"name", "collection_name"}:
            fields["kap"] = None  # new S3 prefix / KAP names: provision again on the next run
        try:
            return dataset_out(await st.update_dataset(tid, dataset_id, fields))
        except Conflict as exc:
            raise err(409, "name_taken", str(exc))

    @router.delete("/pg-datasets/{dataset_id}", status_code=204)
    async def delete_dataset(dataset_id: str, tid: str = Depends(tenant), st: Any = Depends(store)) -> Response:
        await get_dataset(st, tid, dataset_id)
        await st.delete_dataset(tid, dataset_id)
        return Response(status_code=204)

    @router.post("/pg-datasets/{dataset_id}/preview")
    async def preview_dataset(dataset_id: str, tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        ds = await get_dataset(st, tid, dataset_id)
        src = await get_source(st, tid, ds["source_id"])
        try:
            async with open_session(src) as s:
                return await build.preview(s, ds)
        except SourceUnavailable as exc:
            raise err(502, "source_unavailable", f"Cannot connect to the source database: {exc}")

    @router.post("/pg-datasets/{dataset_id}/run", status_code=201)
    async def run_dataset(dataset_id: str, request: Request, tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        await get_dataset(st, tid, dataset_id)
        try:
            run = await export.start_run(request.app.state.pool, tid, dataset_id, "manual")
        except Conflict as exc:
            raise err(409, "export_in_progress", str(exc))
        return run_out(run)

    # ------------------------------------------------------------ export runs
    @router.get("/pg-exports")
    async def list_runs(dataset_id: str | None = None, limit: int = Query(20, ge=1, le=200), offset: int = Query(0, ge=0), tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        if dataset_id is not None:
            try:
                uuid.UUID(dataset_id)
            except ValueError:
                return _page([], 0, limit, offset)
        rows, total = await st.list_runs(tid, dataset_id, limit, offset)
        return _page([run_out(r) for r in rows], total, limit, offset)

    @router.get("/pg-exports/{run_id}")
    async def read_run(run_id: str, tid: str = Depends(tenant), st: Any = Depends(store)) -> dict[str, Any]:
        check_id(run_id, "export")
        run = await st.get_run(tid, run_id)
        if run is None:
            raise err(404, "not_found", "No such export.")
        return run_out(run)

    app.include_router(router)
