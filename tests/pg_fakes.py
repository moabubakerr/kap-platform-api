"""In-memory stand-ins for the PostgreSQL data source: store, source session, S3, KAP."""
from __future__ import annotations

import copy
import uuid
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from kap_platform.pgsource import presets
from kap_platform.pgsource.store import ACTIVE, Conflict

SBX = "71b6c98a-7f7a-4a8b-ab31-132c85bdc88b"
PUB = "72C12E80-DA0C-58CD-81C3-B7EC98B82BB0"
PUB_ID = "72c12e80da0c58cd81c3b7ec98b82bb0"


def now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------- scai_indicators fixture rows
# name_en/name_ar as DETAILS_SQL returns them (already coalesced with the indicator's names)
DETAILS = [
    {"indicator_detail_id": "d1", "published_detail_id": PUB, "name_en": "Non-Hydrocarbon Exports", "name_ar": "الصادرات غير الهيدروكربونية", "unit_en": "%",
     "definition_en": "Share of non-hydrocarbon exports.", "definition_ar": "حصة الصادرات غير الهيدروكربونية.",
     "data_source_en": "National Planning Council", "baseline_value": Decimal("25.5"), "baseline_year": 2018},
    {"indicator_detail_id": "d2", "published_detail_id": None, "name_en": "Draft indicator", "name_ar": None, "unit_en": None,
     "definition_en": None, "definition_ar": None, "data_source_en": None, "baseline_value": None, "baseline_year": None},
]

VALUES = [
    {"indicator_detail_id": "d1", "period_date": date(2019, 1, 1), "granularity": "yearly", "country_en": None, "actual": Decimal("32.04"), "target": Decimal("50")},
    {"indicator_detail_id": "d1", "period_date": date(2020, 1, 1), "granularity": "yearly", "country_en": "", "actual": Decimal("0.70"), "target": None},
    {"indicator_detail_id": "d1", "period_date": date(2024, 1, 1), "granularity": "quarterly", "country_en": "Qatar", "actual": Decimal("1.5"), "target": None},
    {"indicator_detail_id": "d1", "period_date": date(2024, 7, 1), "granularity": "quarterly", "country_en": None, "actual": None, "target": None},
    {"indicator_detail_id": "d1", "period_date": date(2019, 1, 1), "granularity": "yearly", "country_en": "United Arab Emirates", "actual": Decimal("40"), "target": None},
    {"indicator_detail_id": "d2", "period_date": date(2019, 1, 1), "granularity": "yearly", "country_en": None, "actual": Decimal("1"), "target": None},
]

ANALYSIS = [
    {"analysis_id": "a1", "origin": "item", "indicator_detail_id": "d1", "period_date": date(2019, 1, 1), "granularity": "yearly", "country_en": None,
     "summary_en": "Item summary", "detailed_analysis_en": None, "npc_analysis_en": None, "benchmark_en": None,
     "summary_ar": None, "detailed_analysis_ar": None, "npc_analysis_ar": None, "benchmark_ar": None},
    {"analysis_id": "a2", "origin": "published", "indicator_detail_id": "d1", "period_date": date(2019, 1, 1), "granularity": "yearly", "country_en": None,
     "summary_en": "Exports, rose", "detailed_analysis_en": "Line 1\nLine 2", "npc_analysis_en": "NPC", "benchmark_en": "Above GCC",
     "summary_ar": "ملخص", "detailed_analysis_ar": None, "npc_analysis_ar": None, "benchmark_ar": None},
    {"analysis_id": "a3", "origin": "published", "indicator_detail_id": None, "period_date": None, "granularity": None, "country_en": None,
     "summary_en": "orphan", "detailed_analysis_en": None, "npc_analysis_en": None, "benchmark_en": None,
     "summary_ar": None, "detailed_analysis_ar": None, "npc_analysis_ar": None, "benchmark_ar": None},
]


# ---------------------------------------------------------------- source session
class FakeSession:
    """`queries` maps SQL text → (columns, rows): used by sample/count/stream, and by fetch for any SQL
    that is not a scai preset query (rows come back as dicts). `columns` maps "schema.name" → [{name, type, nullable}]."""

    def __init__(self, tables: dict[str, list[dict[str, Any]]] | None = None, queries: dict[str, tuple[list[str], list[tuple]]] | None = None):
        self.data = tables if tables is not None else {"details": DETAILS, "values": VALUES, "analysis": ANALYSIS}
        self.queries = queries or {}
        self.readable = [{"schema": "public", "name": "indicators", "rows_estimate": 470}]
        self.columns_by_table: dict[str, list[dict[str, Any]]] = {}
        self.fetched: list[tuple[str, tuple]] = []

    async def server_version(self) -> str:
        return "16.4"

    async def tables(self) -> list[dict[str, Any]]:
        return copy.deepcopy(self.readable)

    async def columns(self, schema: str, name: str) -> list[dict[str, Any]]:
        return copy.deepcopy(self.columns_by_table.get(f"{schema}.{name}", []))

    async def fetch(self, sql: str, *args: Any, savepoint: bool = False) -> list[Any]:
        self.fetched.append((sql, args))
        key = {presets.DETAILS_SQL: "details", presets.VALUES_SQL: "values", presets.NARRATIVES_SQL: "analysis"}.get(sql)
        if key is None:
            cols, rows = self._q(sql)
            return [dict(zip(cols, r)) for r in rows]
        return copy.deepcopy(self.data[key])

    def _q(self, sql: str) -> tuple[list[str], list[tuple]]:
        return self.queries[sql.strip()]

    async def sample(self, sql: str, limit: int):
        cols, rows = self._q(sql)
        return cols, rows[:limit], len(rows) > limit

    async def count(self, sql: str) -> int:
        return len(self._q(sql)[1])

    @asynccontextmanager
    async def stream(self, sql: str, prefetch: int = 2000):
        cols, rows = self._q(sql)

        async def gen():
            for r in rows:
                yield r

        yield cols, gen()


def opener_for(session: FakeSession, opened: list | None = None):
    @asynccontextmanager
    async def opener(params):
        if opened is not None:
            opened.append(params)
        yield session

    return opener


# ---------------------------------------------------------------- S3
class FakeS3:
    def __init__(self):
        self.objects: dict[str, bytes] = {}

    async def put(self, key: str, body, content_type: str = "text/csv; charset=utf-8") -> None:
        self.objects[key] = body.read()


# ---------------------------------------------------------------- KAP
class FakeKap:
    """Connectors/pipelines/collections by name, pipeline jobs, collection files."""

    def __init__(self, tenants=(SBX,)):
        self.tenants = set(tenants)
        self.objects: dict[str, list[dict[str, Any]]] = {"connectors": [], "pipelines": [], "collections": []}
        self.jobs: list[dict[str, Any]] = []
        self.files: list[dict[str, Any]] = []
        self.calls: list[tuple[str, str, Any]] = []

    def tenant(self, tenant_id: str):
        if tenant_id not in self.tenants:
            raise KeyError(tenant_id)
        return tenant_id

    async def aclose(self):
        pass

    async def paginate(self, tenant_id, path, params=None, page_size=200, max_items=None):
        for item in self.objects[path.rsplit("/", 1)[-1]]:
            yield item

    async def request(self, tenant_id, method, path, *, params=None, json=None):
        self.calls.append((method, path, json))
        parts = path.strip("/").split("/")  # api, resource, ...
        if method == "POST" and len(parts) == 2:
            obj = {"id": str(uuid.uuid4()), **json}
            self.objects[parts[1]].append(obj)
            return obj
        if method == "GET" and parts[1] == "pipelines" and parts[3] == "jobs":
            if len(parts) == 5:
                return next(j for j in self.jobs if j["id"] == parts[4])
            return {"items": sorted(self.jobs, key=lambda j: j["created_at"], reverse=True), "total": len(self.jobs), "has_next": False}
        if method == "GET" and parts[1] == "collections" and parts[3] == "files":
            return {"items": self.files, "total": len(self.files), "has_next": False}
        raise AssertionError(f"unexpected KAP call {method} {path}")


# ---------------------------------------------------------------- store
class MemoryStore:
    """Same methods as PgStore, kept in dicts (shared through `db`)."""

    def __init__(self, db: dict[str, dict[str, dict[str, Any]]]):
        self.db = db
        for t in ("sources", "datasets", "runs"):
            db.setdefault(t, {})

    # sources
    async def create_source(self, tenant_id, fields):
        if any(s["tenant_id"] == tenant_id and s["name"] == fields["name"] for s in self.db["sources"].values()):
            raise Conflict("A source with this name already exists.")
        sid = str(uuid.uuid4())
        row = {"id": sid, "tenant_id": tenant_id, "port": 5432, "sslmode": "prefer", "password_enc": None, "last_test": None, **fields, "created_at": now(), "updated_at": now()}
        self.db["sources"][sid] = row
        return dict(row)

    async def list_sources(self, tenant_id, limit, offset):
        rows = sorted((s for s in self.db["sources"].values() if s["tenant_id"] == tenant_id), key=lambda s: s["name"])
        return [dict(r) for r in rows[offset:offset + limit]], len(rows)

    async def get_source(self, tenant_id, source_id):
        s = self.db["sources"].get(source_id)
        return dict(s) if s and s["tenant_id"] == tenant_id else None

    async def update_source(self, tenant_id, source_id, fields):
        self.db["sources"][source_id].update(fields, updated_at=now())
        return await self.get_source(tenant_id, source_id)

    async def delete_source(self, tenant_id, source_id):
        return self.db["sources"].pop(source_id, None) is not None

    async def count_datasets(self, tenant_id, source_id):
        return sum(1 for d in self.db["datasets"].values() if d["source_id"] == source_id)

    # datasets
    def _ds(self, d):
        out = dict(d)
        out["source_name"] = self.db["sources"][d["source_id"]]["name"]
        runs = sorted((r for r in self.db["runs"].values() if r["dataset_id"] == d["id"]), key=lambda r: r["created_at"])
        out["last_run"] = {"id": runs[-1]["id"], "status": runs[-1]["status"], "finished_at": runs[-1]["finished_at"]} if runs else None
        return out

    async def create_dataset(self, tenant_id, fields):
        for d in self.db["datasets"].values():
            if d["tenant_id"] == tenant_id and (d["name"] == fields["name"] or d["collection_name"] == fields["collection_name"]):
                raise Conflict("A dataset with this name already exists.")
        did = str(uuid.uuid4())
        row = {"id": did, "tenant_id": tenant_id, "queries": {}, "mapping": None, "published_only": True, "schedule_minutes": None, "kap": None, "next_run_at": None, **fields, "created_at": now(), "updated_at": now()}
        self.db["datasets"][did] = row
        return self._ds(row)

    async def list_datasets(self, tenant_id, source_id, limit, offset):
        rows = sorted((d for d in self.db["datasets"].values() if d["tenant_id"] == tenant_id and (source_id is None or d["source_id"] == source_id)), key=lambda d: d["name"])
        return [self._ds(r) for r in rows[offset:offset + limit]], len(rows)

    async def get_dataset(self, tenant_id, dataset_id):
        d = self.db["datasets"].get(dataset_id)
        return self._ds(d) if d and (tenant_id is None or d["tenant_id"] == tenant_id) else None

    async def update_dataset(self, tenant_id, dataset_id, fields):
        self.db["datasets"][dataset_id].update(copy.deepcopy(fields), updated_at=now())
        return await self.get_dataset(tenant_id, dataset_id)

    async def delete_dataset(self, tenant_id, dataset_id):
        for rid in [r["id"] for r in self.db["runs"].values() if r["dataset_id"] == dataset_id]:
            del self.db["runs"][rid]
        return self.db["datasets"].pop(dataset_id, None) is not None

    async def due_datasets(self):
        due = []
        for d in self.db["datasets"].values():
            if d["schedule_minutes"] and d["next_run_at"] and d["next_run_at"] <= now():
                d["next_run_at"] = now() + timedelta(minutes=d["schedule_minutes"])
                due.append({"id": d["id"], "tenant_id": d["tenant_id"]})
        return due

    # runs
    def _run(self, r):
        out = copy.deepcopy(r)
        out["dataset_name"] = self.db["datasets"][r["dataset_id"]]["name"]
        return out

    async def create_run(self, tenant_id, dataset_id, trigger, steps):
        if any(r["dataset_id"] == dataset_id and r["status"] in ACTIVE for r in self.db["runs"].values()):
            raise Conflict("An export of this dataset is already in progress.")
        rid = str(uuid.uuid4())
        self.db["runs"][rid] = {
            "id": rid, "tenant_id": tenant_id, "dataset_id": dataset_id, "trigger": trigger, "status": "queued", "steps": copy.deepcopy(steps),
            "counts": None, "files": None, "file_hashes": None, "kap": None, "uploaded_at": None, "polls": 0, "error": None,
            "created_at": now(), "started_at": None, "finished_at": None,
        }
        return self._run(self.db["runs"][rid])

    async def get_run(self, tenant_id, run_id):
        r = self.db["runs"].get(run_id)
        return self._run(r) if r and (tenant_id is None or r["tenant_id"] == tenant_id) else None

    async def list_runs(self, tenant_id, dataset_id, limit, offset):
        rows = sorted((r for r in self.db["runs"].values() if r["tenant_id"] == tenant_id and (dataset_id is None or r["dataset_id"] == dataset_id)), key=lambda r: r["created_at"], reverse=True)
        return [self._run(r) for r in rows[offset:offset + limit]], len(rows)

    async def update_run(self, run_id, **fields):
        self.db["runs"][run_id].update(copy.deepcopy(fields))

    async def last_success_hashes(self, dataset_id, exclude_run_id):
        rows = sorted((r for r in self.db["runs"].values() if r["dataset_id"] == dataset_id and r["id"] != exclude_run_id and r["status"] == "succeeded" and r["file_hashes"]), key=lambda r: r["created_at"])
        return rows[-1]["file_hashes"] if rows else None


class FakeQueuePool:
    """Captures queue.enqueue calls (fetchrow/execute on the pool)."""

    def __init__(self):
        self.enqueued: list[dict[str, Any]] = []

    async def fetchrow(self, sql, tenant_id, kind, payload, priority, run_after, idem, max_attempts):
        self.enqueued.append({"kind": kind, "payload": payload, "tenant_id": tenant_id, "run_after": run_after, "idempotency_key": idem})
        return {"id": uuid.uuid4()}

    async def execute(self, *_):
        return "SELECT 1"

    async def fetchval(self, *_):
        return 1

    async def close(self):
        pass
