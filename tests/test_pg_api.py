"""API contract of the PostgreSQL data source, with fake pool, store, source, S3 and KAP."""
import pytest
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from kap_platform.api.app import create_api
from kap_platform.config import Settings
from kap_platform.pgsource import export

from pg_fakes import SBX, FakeKap, FakeQueuePool, FakeS3, FakeSession, MemoryStore, opener_for

AUTH = {"Authorization": "Bearer k1"}
BASE = f"/api/t/{SBX}"
SOURCE = {"name": "scai", "host": "scai-chatbot-postgres-1", "port": 5432, "database": "scai_indicators", "username": "kap_platform_ro", "password": "pw", "sslmode": "disable"}
SOURCE_KEYS = {"id", "name", "host", "port", "database", "username", "sslmode", "password_set", "last_test", "created_at", "updated_at"}
DATASET_KEYS = {"id", "source_id", "source_name", "name", "preset", "queries", "published_only", "collection_name", "schedule_minutes", "next_run_at", "kap", "last_run", "created_at", "updated_at"}
RUN_KEYS = {"id", "dataset_id", "dataset_name", "trigger", "status", "created_at", "started_at", "finished_at", "steps", "counts", "files", "kap", "error"}
PAGE_KEYS = {"items", "total", "limit", "offset", "has_next"}


@pytest.fixture
def env(monkeypatch):
    db: dict = {}
    session = FakeSession()
    opened: list = []
    monkeypatch.setattr(export, "SERVICES", export.Services(store_factory=lambda pool: MemoryStore(db), s3_factory=lambda st: FakeS3(), opener=opener_for(session, opened)))
    pool = FakeQueuePool()
    settings = Settings(api_keys=["k1"], tenants_file="none.yaml", secret_key=Fernet.generate_key().decode())

    async def pool_factory():
        return pool

    app = create_api(settings, pool_factory=pool_factory, kap_factory=lambda: FakeKap())
    with TestClient(app) as c:
        yield {"c": c, "db": db, "pool": pool, "session": session, "opened": opened}


def test_source_crud_never_returns_password(env):
    c = env["c"]
    r = c.post(f"{BASE}/pg-sources", json=SOURCE, headers=AUTH)
    assert r.status_code == 201
    src = r.json()
    assert set(src) == SOURCE_KEYS and src["password_set"] is True and src["last_test"] is None
    assert "pw" not in r.text and "password_enc" not in r.text
    stored = env["db"]["sources"][src["id"]]["password_enc"]
    assert stored and stored != "pw"

    page = c.get(f"{BASE}/pg-sources", headers=AUTH).json()
    assert set(page) == PAGE_KEYS and page["total"] == 1 and page["items"][0]["id"] == src["id"]
    assert c.get(f"{BASE}/pg-sources/{src['id']}", headers=AUTH).json()["name"] == "scai"

    r = c.patch(f"{BASE}/pg-sources/{src['id']}", json={"port": 6543}, headers=AUTH)
    assert r.status_code == 200 and r.json()["port"] == 6543 and r.json()["password_set"] is True
    assert c.patch(f"{BASE}/pg-sources/{src['id']}", json={"password": None}, headers=AUTH).json()["password_set"] is False

    assert c.post(f"{BASE}/pg-sources", json=SOURCE, headers=AUTH).status_code == 409
    assert c.get(f"{BASE}/pg-sources/not-a-uuid", headers=AUTH).status_code == 404
    assert c.delete(f"{BASE}/pg-sources/{src['id']}", headers=AUTH).status_code == 204
    assert c.get(f"{BASE}/pg-sources/{src['id']}", headers=AUTH).status_code == 404


def test_auth_tenant_and_validation_errors(env):
    c = env["c"]
    assert c.get(f"{BASE}/pg-sources").status_code == 401
    r = c.get("/api/t/00000000-0000-0000-0000-000000000000/pg-sources", headers=AUTH)
    assert r.status_code == 404 and r.json()["type"] == "tenant_not_configured"
    r = c.post(f"{BASE}/pg-sources", json={**SOURCE, "sslmode": "bogus"}, headers=AUTH)
    assert r.status_code == 422 and set(r.json()) == {"type", "message", "data"} and r.json()["type"] == "validation_error"


def test_source_test_and_preview(env):
    c = env["c"]
    sid = c.post(f"{BASE}/pg-sources", json=SOURCE, headers=AUTH).json()["id"]
    r = c.post(f"{BASE}/pg-sources/{sid}/test", headers=AUTH)
    assert r.status_code == 200
    body = r.json()
    assert body["ok"] is True and body["server_version"] == "16.4" and body["tables"] == [{"schema": "public", "name": "indicators", "rows_estimate": 470}]
    assert env["opened"][0].password == "pw" and env["opened"][0].sslmode == "disable"
    last = c.get(f"{BASE}/pg-sources/{sid}", headers=AUTH).json()["last_test"]
    assert last["ok"] is True and last["message"] and last["at"]

    env["session"].queries = {"select a from t": (["a"], [(i,) for i in range(5)])}
    r = c.post(f"{BASE}/pg-sources/{sid}/preview", json={"sql": "select a from t;", "limit": 3}, headers=AUTH)
    assert r.json() == {"columns": ["a"], "rows": [[0], [1], [2]], "truncated": True}
    r = c.post(f"{BASE}/pg-sources/{sid}/preview", json={"sql": "delete from t", "limit": 3}, headers=AUTH)
    assert r.status_code == 422 and r.json()["type"] == "sql_rejected"
    assert c.post(f"{BASE}/pg-sources/{sid}/preview", json={"sql": "select 1", "limit": 51}, headers=AUTH).status_code == 422


def test_dataset_lifecycle_preview_run_and_exports(env):
    c = env["c"]
    sid = c.post(f"{BASE}/pg-sources", json=SOURCE, headers=AUTH).json()["id"]
    body = {"source_id": sid, "name": "scai", "preset": "scai_indicators", "collection_name": "scai_ind", "schedule_minutes": 60}
    r = c.post(f"{BASE}/pg-datasets", json=body, headers=AUTH)
    assert r.status_code == 201
    ds = r.json()
    assert set(ds) == DATASET_KEYS and ds["source_name"] == "scai" and ds["kap"] is None and ds["last_run"] is None and ds["next_run_at"]
    assert ds["published_only"] is True

    assert c.post(f"{BASE}/pg-datasets", json={**body, "collection_name": "bad-name"}, headers=AUTH).status_code == 422
    assert c.post(f"{BASE}/pg-datasets", json={**body, "name": "other", "preset": "custom", "collection_name": "other_c"}, headers=AUTH).status_code == 422  # no queries
    assert c.post(f"{BASE}/pg-datasets", json=body, headers=AUTH).status_code == 409
    assert c.delete(f"{BASE}/pg-sources/{sid}", headers=AUTH).status_code == 409

    page = c.get(f"{BASE}/pg-datasets", params={"source_id": sid}, headers=AUTH).json()
    assert set(page) == PAGE_KEYS and page["total"] == 1

    r = c.patch(f"{BASE}/pg-datasets/{ds['id']}", json={"schedule_minutes": None, "published_only": False}, headers=AUTH)
    assert r.status_code == 200 and r.json()["schedule_minutes"] is None and r.json()["next_run_at"] is None and r.json()["published_only"] is False

    pv = c.post(f"{BASE}/pg-datasets/{ds['id']}/preview", headers=AUTH).json()
    assert set(pv) == {"files", "problems"} and set(pv["files"]) == {"registry", "observations", "narratives"}
    reg = pv["files"]["registry"]
    assert reg["columns"][0] == "indicator_id" and reg["count"] == len(reg["rows"]) == 4 and isinstance(pv["problems"], list)

    r = c.post(f"{BASE}/pg-datasets/{ds['id']}/run", headers=AUTH)
    assert r.status_code == 201
    run = r.json()
    assert set(run) == RUN_KEYS and run["status"] == "queued" and run["trigger"] == "manual" and run["dataset_name"] == "scai"
    assert [s["key"] for s in run["steps"]] == ["read", "write", "upload", "kap_scan", "kap_load"]
    assert all(set(s) == {"key", "title", "state", "detail", "started_at", "finished_at"} and s["state"] == "pending" for s in run["steps"])
    assert run["counts"] == {"registry": None, "observations": None, "narratives": None} and run["files"] == [] and run["kap"] is None
    assert env["pool"].enqueued[-1]["kind"] == "pg_export"
    assert c.post(f"{BASE}/pg-datasets/{ds['id']}/run", headers=AUTH).json()["type"] == "export_in_progress"

    page = c.get(f"{BASE}/pg-exports", params={"dataset_id": ds["id"]}, headers=AUTH).json()
    assert set(page) == PAGE_KEYS and page["items"][0]["id"] == run["id"]
    assert c.get(f"{BASE}/pg-exports/{run['id']}", headers=AUTH).json()["id"] == run["id"]
    assert c.get(f"{BASE}/pg-datasets/{ds['id']}", headers=AUTH).json()["last_run"]["status"] == "queued"

    assert c.delete(f"{BASE}/pg-datasets/{ds['id']}", headers=AUTH).status_code == 204
    assert c.get(f"{BASE}/pg-exports/{run['id']}", headers=AUTH).status_code == 404


def test_custom_dataset_preview_reports_column_problems(env):
    c = env["c"]
    sid = c.post(f"{BASE}/pg-sources", json=SOURCE, headers=AUTH).json()["id"]
    q = {"registry": "select indicator_id, name_en, frequency, oops from r", "observations": "select indicator_id, period, value from o"}
    env["session"].queries = {q["registry"]: (["indicator_id", "name_en", "frequency", "oops"], [("i", "n", "A", 1)]), q["observations"]: (["indicator_id", "period", "value"], [("i", "2019", 2)])}
    ds = c.post(f"{BASE}/pg-datasets", json={"source_id": sid, "name": "custom1", "preset": "custom", "queries": q, "collection_name": "custom_one"}, headers=AUTH).json()
    assert ds["queries"] == q
    pv = c.post(f"{BASE}/pg-datasets/{ds['id']}/preview", headers=AUTH).json()
    assert pv["files"]["observations"]["count"] == 1 and pv["files"]["observations"]["rows"][0][:5] == ["i", "2019", "", "", "2"]
    assert pv["files"]["registry"]["rows"] == []
    assert any("unknown columns oops" in p for p in pv["problems"]) and any("narratives" in p for p in pv["problems"])
    r = c.post(f"{BASE}/pg-datasets", json={"source_id": sid, "name": "custom2", "preset": "custom", "queries": {**q, "registry": "drop table r"}, "collection_name": "custom_two"}, headers=AUTH)
    assert r.status_code == 422 and r.json()["type"] == "sql_rejected"


def test_secret_key_missing_is_503(monkeypatch):
    db: dict = {}
    monkeypatch.setattr(export, "SERVICES", export.Services(store_factory=lambda pool: MemoryStore(db)))

    async def pool_factory():
        return FakeQueuePool()

    app = create_api(Settings(api_keys=["k1"], tenants_file="none.yaml", secret_key=""), pool_factory=pool_factory, kap_factory=lambda: FakeKap())
    with TestClient(app) as c:
        r = c.post(f"{BASE}/pg-sources", json=SOURCE, headers=AUTH)
    assert r.status_code == 503 and r.json()["type"] == "secret_key_not_configured"
