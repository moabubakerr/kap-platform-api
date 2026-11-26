"""Evaluations of workflows, regressions and automatic runs (fake stores, KAP and judge)."""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from kap_platform.api.app import create_api
from kap_platform.config import Settings
from kap_platform.evals import runner, schedule, score
from kap_platform.workflows import runner as wf_runner

from eval_fakes import FakeJudge, MemoryEvalStore
from pg_fakes import SBX, FakeQueuePool
from wf_fakes import AGENTS, FakeModel, FakeWfKap, MemoryWfStore

AUTH = {"Authorization": "Bearer k1"}
BASE = f"/api/t/{SBX}"
Q1, Q2 = "Latest inflation?", "Inflation in Q1 2026?"
CFG = {
    "nodes": [["in", "entry", "Request", ""], ["a", "agent", "ind_agent", ""], ["out", "output", "Answer", ""]],
    "edges": [["in", "a", "seq", "", ""], ["a", "out", "seq", "", ""]],
    "np": {"a": {"retries": "0"}}, "limits": {"dur": "600 s", "steps": "12"}, "onErr": "",
}
DOC = {"n": "Indicator answer", "purpose": "", "entry": "Chat", "dis": None, "vers": [{"v": "v2", "st": "Draft", "cfg": CFG}, {"v": "v1", "st": "Published", "cfg": CFG}]}


class Kap(FakeWfKap):
    """Workflow fake KAP plus get_agent for agent targets."""

    async def get_agent(self, tenant_id, agent_id):
        name = next(n for n, i in AGENTS.items() if i == agent_id)
        return {"id": agent_id, "name": name, "model_ref": "m", "collections": ["askai_indicators"] if name == "ind_agent" else ["docs"], "allowed_tools": [], "updated_at": "x"}


@pytest.fixture
def env(monkeypatch):
    db: dict = {}
    wdb: dict = {}
    judge = FakeJudge()
    monkeypatch.setattr(runner, "SERVICES", runner.Services(store_factory=lambda pool: MemoryEvalStore(db), judge_factory=lambda s: judge))
    monkeypatch.setattr(wf_runner, "SERVICES", wf_runner.Services(store_factory=lambda pool: MemoryWfStore(wdb), model_factory=lambda s: FakeModel()))
    pool, kap = FakeQueuePool(), Kap()
    settings = Settings(api_keys=["k1"], tenants_file="none.yaml", public_base_url="http://platform-api:8080", eval_trace_wait_s=0)

    async def pool_factory():
        return pool

    with TestClient(create_api(settings, pool_factory=pool_factory, kap_factory=lambda: kap)) as c:
        c.put(f"{BASE}/workflows/ind_answer", json={"doc": DOC}, headers=AUTH)
        sid = c.post(f"{BASE}/eval-sets", json={"name": "QA", "cases": [{"question": Q1, "must_include": ["2.62"]}, {"question": Q2, "must_include": ["2.98"]}]}, headers=AUTH).json()["id"]
        yield {"c": c, "db": db, "wdb": wdb, "pool": pool, "kap": kap, "judge": judge, "settings": settings, "sid": sid}


def eval_tick(env, rid):
    return asyncio.run(runner.tick(env["pool"], env["kap"], env["settings"], rid))


def wf_ticks(env):
    for rid, r in list(env["wdb"]["runs"].items()):
        if r["status"] in ("queued", "running"):
            asyncio.run(wf_runner.tick(env["pool"], env["kap"], env["settings"], rid))


def answer(env, prompt_start, text):
    job = next(j for j in env["kap"].open_jobs() if j["user_prompt"].startswith(prompt_start))
    job.update(status="succeeded", result=text, running_at=datetime.now(timezone.utc).isoformat(), finished_at=datetime.now(timezone.utc).isoformat())


def run_workflow_eval(env, a1, a2, version="v2", label=""):
    c = env["c"]
    r = c.post(f"{BASE}/eval-runs", json={"set_id": env["sid"], "workflow_id": "ind_answer", "version": version, "label": label}, headers=AUTH)
    assert r.status_code == 201, r.text
    run = r.json()
    eval_tick(env, run["id"])  # both questions start workflow runs
    wf_ticks(env)  # each workflow submits its agent job
    answer(env, Q1, a1)
    answer(env, Q2, a2)
    wf_ticks(env)  # workflows finish
    eval_tick(env, run["id"])  # answers are scored
    return c.get(f"{BASE}/eval-runs/{run['id']}", headers=AUTH).json()


def test_workflow_evaluation(env):
    run = run_workflow_eval(env, "Inflation was 2.62%.", "It was 2.98% in Q1 2026.")
    assert run["status"] == "succeeded" and run["target_kind"] == "workflow" and run["workflow_id"] == "ind_answer" and run["version"] == "v2"
    assert run["agent"]["kind"] == "workflow" and run["agent"]["cfg"]["nodes"][1][2] == "ind_agent" and run["agent_name"] == "Indicator answer"
    assert run["summary"]["passed"] == 2 and run["regressed"] is None  # nothing to compare with
    r0 = run["results"][0]
    assert r0["wf_run_id"] and r0["answer"] == "Inflation was 2.62%." and r0["metrics"]["steps"] == ["Request (entry): done", "ind_agent (agent): done", "Answer (output): done"]
    wr = env["wdb"]["runs"][r0["wf_run_id"]]
    assert wr["trigger"] == "eval" and wr["version"] == "v2" and wr["input"] == Q1
    page = env["c"].get(f"{BASE}/eval-runs", params={"workflow_id": "ind_answer"}, headers=AUTH).json()
    assert page["total"] == 1

    # the next run breaks a question: regressed, with the question named
    run2 = run_workflow_eval(env, "Inflation was 2.62%.", "No data for Q1 2026.", label="after prompt change")
    assert run2["regressed"] is True and run2["regress_reason"] == f"Pass rate 100% → 50% · Now failing: {Q2}"
    assert env["c"].get(f"{BASE}/eval-sets/{env['sid']}", headers=AUTH).json()["last_run"]["regressed"] is True


def test_workflow_evaluation_refusals(env):
    c = env["c"]
    body = {"set_id": env["sid"]}
    assert c.post(f"{BASE}/eval-runs", json=body, headers=AUTH).status_code == 422
    assert c.post(f"{BASE}/eval-runs", json={**body, "workflow_id": "nope_wf"}, headers=AUTH).status_code == 404
    assert c.post(f"{BASE}/eval-runs", json={**body, "workflow_id": "ind_answer", "version": "v9"}, headers=AUTH).status_code == 404
    r = c.post(f"{BASE}/eval-runs", json={**body, "workflow_id": "ind_answer"}, headers=AUTH)
    assert r.status_code == 201 and r.json()["version"] == "v1"  # default: the published version
    assert c.post(f"{BASE}/eval-runs", json={**body, "workflow_id": "ind_answer"}, headers=AUTH).status_code == 409


def test_regression_rule():
    prev = [{"case_id": "a", "status": "scored", "passed": True}, {"case_id": "b", "status": "scored", "passed": False}]
    now = [{"case_id": "a", "status": "scored", "passed": True, "question": "A?"}, {"case_id": "b", "status": "scored", "passed": True, "question": "B?"}]
    assert score.regression({"pass_rate": 0.5}, prev, {"pass_rate": 1.0}, now) == (False, None)
    assert score.regression(None, None, {"pass_rate": 1.0}, now) == (None, None)
    assert score.regression(None, None, {"pass_rate": 0.5}, now, 0.8) == (True, "Pass rate 50% is below the required 80%")
    worse = [{**now[0], "passed": False}, now[1]]
    assert score.regression({"pass_rate": 0.5}, prev, {"pass_rate": 0.5}, worse) == (True, "Now failing: A?")  # same rate, but a question broke


def test_schedules(env):
    c, db = env["c"], env["db"]
    agent = AGENTS["ind_agent"]
    assert c.post(f"{BASE}/eval-schedules", json={"set_id": env["sid"], "agent_id": agent}, headers=AUTH).status_code == 422  # neither schedule nor data
    assert c.post(f"{BASE}/eval-schedules", json={"set_id": env["sid"], "every_minutes": 60 * 24}, headers=AUTH).status_code == 422  # no target
    r = c.post(f"{BASE}/eval-schedules", json={"set_id": env["sid"], "agent_id": agent, "every_minutes": 60 * 24, "on_data": True, "min_pass_rate": 0.8}, headers=AUTH)
    assert r.status_code == 201, r.text
    s = r.json()
    assert s["target_kind"] == "agent" and s["next_run_at"] and s["options"]["judge"] is True
    assert c.get(f"{BASE}/eval-schedules", params={"set_id": env["sid"]}, headers=AUTH).json()["total"] == 1

    def tick(at):
        return asyncio.run(schedule.schedule_tick(env["pool"], env["kap"], env["settings"], at))

    now = datetime.now(timezone.utc)
    assert tick(now) == []  # not due, no exports yet
    db["exports"] = {"askai_indicators": now - timedelta(hours=1), "docs": now}
    assert tick(now) == []  # first look only records the newest export of the agent's collections
    assert db["schedules"][s["id"]]["data_seen_at"] == now - timedelta(hours=1)
    db["exports"]["askai_indicators"] = now
    started = tick(now)
    assert len(started) == 1
    run = db["runs"][started[0]]
    assert run["trigger"] == "data" and run["label"] == "After new data" and run["options"]["min_pass_rate"] == 0.8 and run["agent_id"] == agent
    assert tick(now) == []  # same export: nothing new

    # due by time while the data run is still going: refused, tried again next tick
    db["schedules"][s["id"]]["next_run_at"] = now - timedelta(minutes=1)
    assert tick(now) == [] and db["schedules"][s["id"]]["next_run_at"] == now - timedelta(minutes=1)
    db["runs"][started[0]]["status"] = "succeeded"
    started = tick(now)
    assert len(started) == 1 and db["runs"][started[0]]["trigger"] == "schedule" and db["schedules"][s["id"]]["next_run_at"] > now

    r = c.patch(f"{BASE}/eval-schedules/{s['id']}", json={"enabled": False}, headers=AUTH)
    assert r.json()["enabled"] is False
    assert c.patch(f"{BASE}/eval-schedules/{s['id']}", json={"every_minutes": None, "on_data": False}, headers=AUTH).status_code == 422
    assert c.delete(f"{BASE}/eval-schedules/{s['id']}", headers=AUTH).status_code == 204


def test_workflow_schedule_watches_its_agents_collections(env):
    c, db = env["c"], env["db"]
    r = c.post(f"{BASE}/eval-schedules", json={"set_id": env["sid"], "workflow_id": "ind_answer", "on_data": True}, headers=AUTH)
    assert r.status_code == 201 and r.json()["target_kind"] == "workflow"
    now = datetime.now(timezone.utc)
    db["exports"] = {"docs": now}  # not a collection of ind_agent
    assert asyncio.run(schedule.schedule_tick(env["pool"], env["kap"], env["settings"], now)) == []
    assert db["schedules"][r.json()["id"]]["data_seen_at"] is None
