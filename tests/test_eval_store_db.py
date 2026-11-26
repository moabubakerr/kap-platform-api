"""EvalStore against a real Postgres (migration 0006). Skipped unless PLATFORM_TEST_DATABASE_URL is set;
see test_db_integration.py for how to start a throwaway database."""
import os
from datetime import datetime, timezone

import asyncpg
import pytest

from kap_platform.db import create_pool, migrate
from kap_platform.evals.store import Conflict, EvalStore, InUse

BASE = os.environ.get("PLATFORM_TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not BASE, reason="PLATFORM_TEST_DATABASE_URL not set")
SCHEMA = "kap_platform_eval_test"
TENANT = "71b6c98a-7f7a-4a8b-ab31-132c85bdc88b"
AGENT = "aaaaaaaa-0000-4000-8000-000000000001"


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
    yield EvalStore(pool)
    await pool.close()
    admin = await asyncpg.connect(BASE)
    await admin.execute(f"drop schema if exists {SCHEMA} cascade")
    await admin.close()


async def test_sets_cases_runs_results(st):
    sid = await st.create_set(TENANT, {"name": "QA", "description": "d"})
    with pytest.raises(Conflict):
        await st.create_set(TENANT, {"name": "QA"})
    await st.add_cases(sid, [{"question": "q1", "must_include": ["2.62"]}, {"question": "q2", "expected": "e", "tags": ["x"]}])
    await st.add_cases(sid, [{"question": "q3"}])
    cases = await st.list_cases(sid)
    assert [(c["question"], c["position"]) for c in cases] == [("q1", 0), ("q2", 1), ("q3", 2)] and cases[0]["must_include"] == ["2.62"]
    await st.update_case(sid, cases[2]["id"], {"question": "q3!", "must_not_include": ["no"]})
    assert (await st.get_case(sid, cases[2]["id"]))["must_not_include"] == ["no"]
    await st.delete_case(sid, cases[2]["id"])
    await st.update_set(TENANT, sid, {"description": "changed"})
    s = await st.get_set(TENANT, sid)
    assert s["case_count"] == 2 and s["description"] == "changed" and s["last_run"] is None

    cases = await st.list_cases(sid)
    rid = await st.create_run(TENANT, {"set_id": sid, "agent_id": AGENT, "agent_name": "a", "agent": {"model_ref": "m"}, "options": {"judge": False}}, cases)
    assert await st.has_active_run(TENANT, sid, AGENT)
    with pytest.raises(InUse):
        await st.delete_set(TENANT, sid)
    results = await st.results(rid)
    assert [r["question"] for r in results] == ["q1", "q2"] and results[0]["case_id"] == cases[0]["id"] and results[0]["status"] == "pending"
    now = datetime.now(timezone.utc)
    await st.update_result(results[0]["id"], {"status": "scored", "job_id": AGENT, "checks": [{"kind": "includes", "term": "2.62", "ok": True}],
                                              "judge": None, "passed": True, "metrics": {"latency_ms": 5}, "scored_at": now})
    await st.update_run(rid, {"status": "succeeded", "summary": {"passed": 1}, "polls": 3, "finished_at": now})
    run = await st.get_run(TENANT, rid)
    assert run["set_name"] == "QA" and run["agent"] == {"model_ref": "m"} and run["summary"] == {"passed": 1} and run["polls"] == 3
    assert await st.get_run("00000000-0000-0000-0000-000000000000", rid) is None
    r0 = (await st.results(rid))[0]
    assert r0["checks"][0]["ok"] is True and r0["job_id"] == AGENT and r0["metrics"] == {"latency_ms": 5}
    items, total = await st.list_runs(TENANT, sid, None, 10, 0)
    assert total == 1 and items[0]["id"] == rid
    assert (await st.list_runs(TENANT, None, AGENT, 10, 0))[1] == 1
    assert (await st.get_set(TENANT, sid))["last_run"]["status"] == "succeeded"
    assert await st.trace_status(AGENT) is None and await st.trace(AGENT) is None

    # deleting a case keeps its results (case_id becomes null); deleting the set removes runs too
    await st.delete_case(sid, cases[0]["id"])
    assert (await st.results(rid))[0]["case_id"] is None
    await st.delete_set(TENANT, sid)
    assert await st.get_run(TENANT, rid) is None
