"""Evaluations: API contract and a full run (fake store, KAP and judge)."""
import asyncio
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

from kap_platform import queue
from kap_platform.api.app import create_api
from kap_platform.config import Settings
from kap_platform.evals import runner

from eval_fakes import AGENT, FakeAgentKap, FakeJudge, MemoryEvalStore, tool_trace
from pg_fakes import SBX, FakeQueuePool

AUTH = {"Authorization": "Bearer k1"}
BASE = f"/api/t/{SBX}"
Q1, Q2, Q3 = "What is the latest inflation rate?", "Compare Qatar and Kuwait inflation in March 2026.", "What was inflation in Q1 2026?"
CASES = [
    {"question": Q1, "expected": "2.62% in April 2026", "must_include": ["2.62", "April 2026"]},
    {"question": Q2, "expected": "Qatar 4.17%, Kuwait 2.06%", "must_include": ["4.17", "2.06"], "must_not_include": ["I don't know"]},
    {"question": Q3, "must_include": ["2.98"], "tags": ["quarterly"]},
]
SET_KEYS = {"id", "name", "description", "case_count", "last_run", "created_at", "updated_at"}
RUN_KEYS = {"id", "set_id", "set_name", "agent_id", "agent_name", "agent", "label", "status", "summary", "error", "created_at", "started_at", "finished_at", "options",
            "target_kind", "workflow_id", "version", "trigger", "regressed", "regress_reason"}


@pytest.fixture
def env(monkeypatch):
    db: dict = {}
    judge = FakeJudge({Q3: {"correctness": 2, "grounding": 3, "completeness": 4, "reason": "Gives the yearly figure, not Q1."}})
    monkeypatch.setattr(runner, "SERVICES", runner.Services(store_factory=lambda pool: MemoryEvalStore(db), judge_factory=lambda s: judge))
    pool = FakeQueuePool()
    kap = FakeAgentKap()
    settings = Settings(api_keys=["k1"], tenants_file="none.yaml", public_base_url="http://platform-api:8080", hook_token="hook", eval_trace_wait_s=60)

    async def pool_factory():
        return pool

    app = create_api(settings, pool_factory=pool_factory, kap_factory=lambda: kap)
    with TestClient(app) as c:
        yield {"c": c, "db": db, "pool": pool, "kap": kap, "judge": judge, "settings": settings}


def tick(env, run_id):
    return asyncio.run(runner.tick(env["pool"], env["kap"], env["settings"], run_id))


def make_set(c, cases=CASES, name="Indicators QA"):
    r = c.post(f"{BASE}/eval-sets", json={"name": name, "description": "Checked by hand", "cases": cases}, headers=AUTH)
    assert r.status_code == 201, r.text
    return r.json()


def test_sets_and_cases_crud(env):
    c = env["c"]
    s = make_set(c)
    assert set(s) == SET_KEYS | {"cases"} and s["case_count"] == 3 and s["last_run"] is None
    assert [x["question"] for x in s["cases"]] == [Q1, Q2, Q3] and s["cases"][2]["tags"] == ["quarterly"]
    assert c.post(f"{BASE}/eval-sets", json={"name": "Indicators QA"}, headers=AUTH).status_code == 409

    page = c.get(f"{BASE}/eval-sets", headers=AUTH).json()
    assert page["total"] == 1 and set(page["items"][0]) == SET_KEYS

    r = c.post(f"{BASE}/eval-sets/{s['id']}/cases", json={"cases": [{"question": "  Extra?  ", "must_include": [" a ", "", "b"]}]}, headers=AUTH)
    assert r.status_code == 201
    extra = r.json()["cases"][-1]
    assert extra["question"] == "Extra?" and extra["must_include"] == ["a", "b"] and extra["position"] == 3

    r = c.patch(f"{BASE}/eval-sets/{s['id']}/cases/{extra['id']}", json={"expected": "B", "must_include": ["b"]}, headers=AUTH)
    assert r.status_code == 200 and r.json()["expected"] == "B" and r.json()["must_include"] == ["b"]
    assert c.patch(f"{BASE}/eval-sets/{s['id']}/cases/{extra['id']}", json={"question": "   "}, headers=AUTH).status_code == 422
    assert c.delete(f"{BASE}/eval-sets/{s['id']}/cases/{extra['id']}", headers=AUTH).status_code == 204

    r = c.post(f"{BASE}/eval-sets/{s['id']}/cases", json={"cases": [{"question": "Only one"}], "replace": True}, headers=AUTH)
    assert [x["question"] for x in r.json()["cases"]] == ["Only one"]

    r = c.patch(f"{BASE}/eval-sets/{s['id']}", json={"name": "Renamed"}, headers=AUTH)
    assert r.json()["name"] == "Renamed"
    assert c.post(f"{BASE}/eval-sets", json={"name": "bad\nname"}, headers=AUTH).status_code == 422
    assert c.post(f"{BASE}/eval-sets/{s['id']}/cases", json={"cases": [{"question": ""}]}, headers=AUTH).status_code == 422
    assert c.get(f"{BASE}/eval-sets/nope", headers=AUTH).status_code == 404
    assert c.delete(f"{BASE}/eval-sets/{s['id']}", headers=AUTH).status_code == 204
    assert c.get(f"{BASE}/eval-sets/{s['id']}", headers=AUTH).status_code == 404


def test_start_run_validation(env):
    c = env["c"]
    empty = make_set(c, cases=[], name="Empty")
    r = c.post(f"{BASE}/eval-runs", json={"set_id": empty["id"], "agent_id": AGENT}, headers=AUTH)
    assert r.status_code == 422 and "no questions" in r.json()["message"]
    s = make_set(c)
    r = c.post(f"{BASE}/eval-runs", json={"set_id": s["id"], "agent_id": "bbbbbbbb-0000-4000-8000-000000000009"}, headers=AUTH)
    assert r.status_code == 404 and r.json()["message"] == "KAP has no such agent."
    assert c.post(f"{BASE}/eval-runs", json={"set_id": s["id"], "agent_id": AGENT, "parallel": 99}, headers=AUTH).status_code == 422
    assert c.post(f"{BASE}/eval-runs", json={"set_id": s["id"], "agent_id": AGENT}, headers=AUTH).status_code == 201
    r = c.post(f"{BASE}/eval-runs", json={"set_id": s["id"], "agent_id": AGENT}, headers=AUTH)
    assert r.status_code == 409
    assert c.get(f"{BASE}/eval-runs", params={"agent_id": "not-a-uuid"}, headers=AUTH).json()["total"] == 0


def test_full_run(env):
    c, kap, db, judge = env["c"], env["kap"], env["db"], env["judge"]
    s = make_set(c)
    r = c.post(f"{BASE}/eval-runs", json={"set_id": s["id"], "agent_id": AGENT, "label": "after prompt change", "parallel": 2}, headers=AUTH)
    assert r.status_code == 201
    run = r.json()
    assert set(run) == RUN_KEYS | {"results"} and run["status"] == "queued" and run["agent_name"] == "askai_indicators_agent"
    assert run["agent"]["model_ref"] == "chat_default" and run["options"]["min_correctness"] == 4 and run["label"] == "after prompt change"
    assert [x["status"] for x in run["results"]] == ["pending"] * 3 and run["summary"]["total"] == 3
    assert env["pool"].enqueued[-1]["kind"] == "eval_watch" and env["pool"].enqueued[-1]["idempotency_key"] == f"evalwatch:{run['id']}:0"

    # tick 1: two questions in flight (parallel 2), with the platform's callback
    assert tick(env, run["id"])["status"] == "running"
    res = c.get(f"{BASE}/eval-runs/{run['id']}", headers=AUTH).json()
    assert res["status"] == "running" and res["started_at"] and [x["status"] for x in res["results"]] == ["submitted", "submitted", "pending"]
    assert kap.submitted[0] == {"prompt": Q1, "callback_url": f"http://platform-api:8080/hooks/kap/t/{SBX}/agents/{AGENT}/jobs", "callback_headers": {"X-Platform-Hook-Token": "hook"}}
    assert env["pool"].enqueued[-1]["idempotency_key"] == f"evalwatch:{run['id']}:1"

    # Q1 answers correctly; its trace is linked with tool evidence
    j1 = kap.finish(Q1, '{"answer": "The latest inflation rate is 2.62% (April 2026)."}')
    db["trace_status"][j1] = "linked"
    db["traces"][j1] = tool_trace(j1, "get_indicator_value", {"value": 2.62, "period": "2026-04"}, tokens=2000)
    # Q2 fails in KAP
    kap.finish(Q2, None, status="failed", error="tool timeout")
    tick(env, run["id"])
    res = c.get(f"{BASE}/eval-runs/{run['id']}", headers=AUTH).json()
    r1, r2, r3 = res["results"]
    assert r1["status"] == "scored" and r1["passed"] is True and r1["answer"] == "The latest inflation rate is 2.62% (April 2026)."
    assert r1["checks"] == [{"kind": "includes", "term": "2.62", "ok": True}, {"kind": "includes", "term": "April 2026", "ok": True}]
    assert r1["judge"]["correctness"] == 5 and r1["metrics"]["tokens"] == 2000 and r1["metrics"]["tools"] == ["get_indicator_value"] and r1["metrics"]["latency_ms"] == 0
    assert '[get_indicator_value] {"text": "inflation"}\n→ {"value": 2.62, "period": "2026-04"}' == judge.calls[0]["evidence"]
    assert r2["status"] == "scored" and r2["passed"] is False and r2["reason"] == "The agent job failed. tool timeout" and r2["judge"] is None
    assert r3["status"] == "submitted"  # a slot came free
    assert len(judge.calls) == 1  # no judging of a failed job

    # Q3 answers with a wrong figure; its trace is not linked yet → wait
    j3 = kap.finish(Q3, "Inflation in 2026 was 3.1%.")
    tick(env, run["id"])
    r3 = c.get(f"{BASE}/eval-runs/{run['id']}", headers=AUTH).json()["results"][2]
    assert r3["status"] == "answered" and r3["answer"] == "Inflation in 2026 was 3.1%."
    db["trace_status"][j3] = "none"  # no model calls recorded: score without the trace
    out = tick(env, run["id"])
    assert out["status"] == "succeeded"
    res = c.get(f"{BASE}/eval-runs/{run['id']}", headers=AUTH).json()
    r3 = res["results"][2]
    assert r3["passed"] is False and r3["reason"] == "Missing: 2.98 · Judge: correctness 2/5 (needs 4) · Gives the yearly figure, not Q1."
    assert judge.calls[-1]["evidence"] is None
    s1 = res["summary"]
    assert res["status"] == "succeeded" and res["finished_at"] and (s1["passed"], s1["failed"], s1["errors"], s1["pass_rate"]) == (1, 2, 0, 0.3333)
    assert s1["correctness"] == 3.5 and s1["tokens_total"] == 2000
    assert {j for j, _ in db["mirrored"]} == set(kap.jobs)

    # lists and the set's last run
    page = c.get(f"{BASE}/eval-runs", params={"set_id": s["id"]}, headers=AUTH).json()
    assert page["total"] == 1 and set(page["items"][0]) == RUN_KEYS and page["items"][0]["set_name"] == "Indicators QA"
    assert c.get(f"{BASE}/eval-runs", params={"agent_id": AGENT}, headers=AUTH).json()["total"] == 1
    assert c.get(f"{BASE}/eval-sets/{s['id']}", headers=AUTH).json()["last_run"]["status"] == "succeeded"
    assert tick(env, run["id"]) == {"skipped": "run is not active"}
    assert c.delete(f"{BASE}/eval-runs/{run['id']}", headers=AUTH).status_code == 204
    assert c.get(f"{BASE}/eval-runs/{run['id']}", headers=AUTH).status_code == 404


def test_busy_kap_timeout_and_cancel(env):
    c, kap, db = env["c"], env["kap"], env["db"]
    s = make_set(c)
    run = c.post(f"{BASE}/eval-runs", json={"set_id": s["id"], "agent_id": AGENT, "judge": False, "parallel": 1}, headers=AUTH).json()
    kap.busy = True  # 429: questions stay pending
    tick(env, run["id"])
    assert [x["status"] for x in c.get(f"{BASE}/eval-runs/{run['id']}", headers=AUTH).json()["results"]] == ["pending"] * 3
    kap.busy = False
    tick(env, run["id"])
    first = next(x for x in db["results"].values() if x["run_id"] == run["id"] and x["position"] == 0)
    assert first["status"] == "submitted"
    first["submitted_at"] = first["submitted_at"] - timedelta(seconds=env["settings"].eval_job_wait_s + 1)
    tick(env, run["id"])
    res = c.get(f"{BASE}/eval-runs/{run['id']}", headers=AUTH).json()
    assert res["results"][0]["status"] == "error" and "still waiting" in res["results"][0]["error"] and res["results"][1]["status"] == "submitted"
    assert c.delete(f"{BASE}/eval-runs/{run['id']}", headers=AUTH).status_code == 409
    assert c.delete(f"{BASE}/eval-sets/{s['id']}", headers=AUTH).status_code == 409

    res = c.post(f"{BASE}/eval-runs/{run['id']}/cancel", headers=AUTH).json()
    assert res["status"] == "cancelled" and {x["status"] for x in res["results"]} == {"error"} and res["summary"]["errors"] == 3
    assert tick(env, run["id"]) == {"skipped": "run is not active"}


def test_tick_failure_marks_run_failed(env, monkeypatch):
    c = env["c"]
    s = make_set(c)
    run = c.post(f"{BASE}/eval-runs", json={"set_id": s["id"], "agent_id": AGENT}, headers=AUTH).json()

    async def boom(*_a, **_k):
        raise RuntimeError("database went away")

    monkeypatch.setattr(runner, "tick", boom)
    job = queue.Job(id=None, tenant_id=None, kind="eval_watch", payload={"run_id": run["id"]}, attempts=3, max_attempts=3)
    with pytest.raises(RuntimeError):
        asyncio.run(runner.handle_eval_watch(env["pool"], env["kap"], env["settings"], job))
    res = c.get(f"{BASE}/eval-runs/{run['id']}", headers=AUTH).json()
    assert res["status"] == "failed" and res["error"] == "RuntimeError: database went away"
