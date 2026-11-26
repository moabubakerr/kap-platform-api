"""PgStore against a real Postgres (migration 0003). Skipped unless PLATFORM_TEST_DATABASE_URL is set;
see test_db_integration.py for how to start a throwaway database."""
import os
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from kap_platform.db import create_pool, migrate
from kap_platform.pgsource.export import initial_steps
from kap_platform.pgsource.store import Conflict, PgStore

BASE = os.environ.get("PLATFORM_TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not BASE, reason="PLATFORM_TEST_DATABASE_URL not set")
SCHEMA = "kap_platform_pg_test"
TENANT = "71b6c98a-7f7a-4a8b-ab31-132c85bdc88b"


def dsn() -> str:
    sep = "&" if "?" in BASE else "?"
    return f"{BASE}{sep}search_path={SCHEMA}"


@pytest.fixture
async def st():
    admin = await asyncpg.connect(BASE)
    await admin.execute(f"drop schema if exists {SCHEMA} cascade; create schema {SCHEMA}")
    await admin.close()
    await migrate(dsn())
    pool = await create_pool(dsn(), max_size=4)
    yield PgStore(pool)
    await pool.close()
    admin = await asyncpg.connect(BASE)
    await admin.execute(f"drop schema if exists {SCHEMA} cascade")
    await admin.close()


async def test_sources_datasets_runs(st):
    src = await st.create_source(TENANT, {"name": "scai", "host": "h", "port": 5432, "database": "d", "username": "u", "password_enc": "tok", "sslmode": "disable"})
    with pytest.raises(Conflict):
        await st.create_source(TENANT, {"name": "scai", "host": "h", "database": "d", "username": "u"})
    src = await st.update_source(TENANT, src["id"], {"last_test": {"ok": True, "message": "m", "at": "now"}})
    assert src["last_test"]["ok"] is True

    past = datetime.now(timezone.utc) - timedelta(minutes=1)
    ds = await st.create_dataset(TENANT, {"source_id": src["id"], "name": "scai", "preset": "scai_indicators", "queries": {}, "collection_name": "scai_ind", "schedule_minutes": 30, "next_run_at": past})
    assert ds["source_name"] == "scai" and ds["last_run"] is None
    with pytest.raises(Conflict):
        await st.create_dataset(TENANT, {"source_id": src["id"], "name": "other", "preset": "custom", "collection_name": "scai_ind"})
    assert await st.count_datasets(TENANT, src["id"]) == 1

    # migration 0004: preset mapped + mapping jsonb (required for mapped)
    m = {"table": "public.kpi", "series_key": ["indicator"], "id_prefix": "kpi", "period": {"column": "d", "kind": "date"}, "frequency": {"value": "A"}, "value": "v", "domestic_values": [], "filters": []}
    mapped = await st.create_dataset(TENANT, {"source_id": src["id"], "name": "kpi", "preset": "mapped", "mapping": m, "collection_name": "kpi_coll"})
    assert mapped["mapping"] == m and ds["mapping"] is None
    with pytest.raises(asyncpg.CheckViolationError):
        await st.create_dataset(TENANT, {"source_id": src["id"], "name": "kpi2", "preset": "mapped", "collection_name": "kpi_coll2"})
    await st.delete_dataset(TENANT, mapped["id"])

    due = await st.due_datasets()
    assert due == [{"id": ds["id"], "tenant_id": TENANT}] and await st.due_datasets() == []

    run = await st.create_run(TENANT, ds["id"], "schedule", initial_steps())
    assert run["status"] == "queued" and run["dataset_name"] == "scai" and len(run["steps"]) == 5
    with pytest.raises(Conflict):
        await st.create_run(TENANT, ds["id"], "manual", initial_steps())
    await st.update_run(run["id"], status="succeeded", file_hashes={"registry.csv": "a"}, finished_at=datetime.now(timezone.utc))
    run2 = await st.create_run(TENANT, ds["id"], "manual", initial_steps())
    assert await st.last_success_hashes(ds["id"], run2["id"]) == {"registry.csv": "a"}
    items, total = await st.list_runs(TENANT, ds["id"], 10, 0)
    assert total == 2 and items[0]["id"] == run2["id"]
    assert (await st.get_dataset(TENANT, ds["id"]))["last_run"]["id"] == run2["id"]

    from kap_platform.pgsource.store import InUse
    with pytest.raises(InUse):
        await st.delete_source(TENANT, src["id"])
    assert await st.delete_dataset(TENANT, ds["id"]) and await st.get_run(TENANT, run["id"]) is None
    assert await st.delete_source(TENANT, src["id"])
