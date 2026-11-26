"""WfStore against a real Postgres (migration 0007). Skipped unless PLATFORM_TEST_DATABASE_URL is set;
see test_db_integration.py for how to start a throwaway database."""
import os
from datetime import datetime, timezone

import asyncpg
import pytest

from kap_platform.db import create_pool, migrate
from kap_platform.workflows.graph import Step
from kap_platform.workflows.store import WfStore

BASE = os.environ.get("PLATFORM_TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not BASE, reason="PLATFORM_TEST_DATABASE_URL not set")
SCHEMA = "kap_platform_wf_test"
TENANT = "71b6c98a-7f7a-4a8b-ab31-132c85bdc88b"
JOB = "aaaaaaaa-0000-4000-8000-000000000001"


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
    yield WfStore(pool)
    await pool.close()
    admin = await asyncpg.connect(BASE)
    await admin.execute(f"drop schema if exists {SCHEMA} cascade")
    await admin.close()


async def test_workflows_runs_steps(st):
    doc = {"n": "W", "vers": [{"v": "v1", "st": "Published", "cfg": {"nodes": [], "edges": []}}]}
    assert await st.put_workflow(TENANT, "w_one", doc) is True
    assert await st.put_workflow(TENANT, "w_one", {**doc, "n": "W2"}) is False
    assert (await st.get_workflow(TENANT, "w_one"))["doc"]["n"] == "W2"

    rid = await st.create_run(TENANT, {"workflow_id": "w_one", "workflow_name": "W2", "version": "v1", "input": "q", "cfg": {"nodes": []}, "agents": {"a": JOB}},
                              [Step("in", "entry", "Request"), Step("a", "agent", "a")])
    run = await st.get_run(TENANT, rid)
    assert run["status"] == "queued" and run["agents"] == {"a": JOB} and run["trigger"] == "test"
    assert await st.try_lease(rid, 30) is True and await st.try_lease(rid, 30) is False
    await st.release(rid)
    assert await st.try_lease(rid, 30) is True
    now = datetime.now(timezone.utc)
    await st.update_step(rid, "a", {"status": "running", "job_id": JOB, "attempts": 1, "seq": 1, "chosen": ["x"], "started_at": now})
    await st.update_run(rid, {"status": "running", "agent_jobs": 1, "started_at": now})
    steps = await st.steps(rid)
    assert steps["a"].status == "running" and steps["a"].job_id == JOB and steps["a"].chosen == ["x"] and steps["in"].status == "pending"
    items, total = await st.list_runs(TENANT, "w_one", 10, 0)
    assert total == 1 and items[0]["agent_jobs"] == 1 and items[0]["path"] == [["a", "running", 1], ["in", "pending", None]]
    assert (await st.list_workflows(TENANT))[0]["last_run"]["status"] == "running"
    assert await st.trace(JOB) is None
    await st.delete_workflow(TENANT, "w_one")
    assert await st.get_workflow(TENANT, "w_one") is None and await st.get_run(TENANT, rid) is not None  # runs are kept
