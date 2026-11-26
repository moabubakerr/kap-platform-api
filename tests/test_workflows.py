"""Workflows: graph rules, API contract and runs (fake store, KAP and model server)."""
import asyncio
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

from kap_platform.api.app import create_api
from kap_platform.config import Settings
from kap_platform.workflows import graph, runner
from kap_platform.workflows.graph import Step

from pg_fakes import SBX, FakeQueuePool
from wf_fakes import AGENTS, FakeModel, FakeWfKap, MemoryWfStore

AUTH = {"Authorization": "Bearer k1"}
BASE = f"/api/t/{SBX}"
Q = "What was inflation in March 2026?"


def cfg(nodes, edges, np=None, limits=None, onErr=""):
    return {"nodes": nodes, "edges": edges, "np": np or {}, "limits": limits or {"dur": "600 s", "steps": "12"}, "onErr": onErr, "pos": {}, "io": {}, "map": [], "mode": "", "fail": ""}


def doc(c, dis=None):
    return {"n": "Test workflow", "purpose": "p", "entry": "Chat", "evSets": [], "dis": dis, "vers": [{"v": "v2", "st": "Draft", "cfg": c}, {"v": "v1", "st": "Published", "cfg": c}]}


SEQ = cfg(
    [["in", "entry", "Request", "Chat"], ["a", "agent", "ind_agent", ""], ["b", "agent", "writer", ""], ["out", "output", "Answer", ""]],
    [["in", "a", "seq", "", ""], ["a", "b", "seq", "", ""], ["b", "out", "seq", "", ""]],
)
ROUTED = cfg(
    [["in", "entry", "Request", ""], ["r", "router", "Router", ""], ["ind", "agent", "ind_agent", ""], ["doc", "agent", "doc_agent", ""], ["out", "output", "Answer", ""]],
    [["in", "r", "seq", "", ""], ["r", "ind", "cond", "inflation, prices, indicators", ""], ["r", "doc", "cond", "strategy | documents | policy", ""], ["ind", "out", "seq", "", ""], ["doc", "out", "seq", "", ""]],
)
PARALLEL = cfg(
    [["in", "entry", "Request", ""], ["a", "agent", "ind_agent", ""], ["b", "agent", "doc_agent", ""], ["g", "aggregate", "writer", ""], ["out", "output", "Answer", ""]],
    [["in", "a", "par", "", ""], ["in", "b", "par", "", ""], ["a", "g", "seq", "", ""], ["b", "g", "seq", "", ""], ["g", "out", "seq", "", ""]],
)


@pytest.fixture
def env(monkeypatch):
    db: dict = {}
    model = FakeModel()
    monkeypatch.setattr(runner, "SERVICES", runner.Services(store_factory=lambda pool: MemoryWfStore(db), model_factory=lambda s: model))
    pool, kap = FakeQueuePool(), FakeWfKap()
    settings = Settings(api_keys=["k1"], tenants_file="none.yaml", public_base_url="http://platform-api:8080", hook_token="hook")

    async def pool_factory():
        return pool

    with TestClient(create_api(settings, pool_factory=pool_factory, kap_factory=lambda: kap)) as c:
        yield {"c": c, "db": db, "pool": pool, "kap": kap, "model": model, "settings": settings}


def tick(env, rid):
    return asyncio.run(runner.tick(env["pool"], env["kap"], env["settings"], rid))


def start(env, c_, input=Q, **kw):
    c = env["c"]
    assert c.put(f"{BASE}/workflows/test_wf", json={"doc": doc(c_)}, headers=AUTH).status_code in (200, 201)
    r = c.post(f"{BASE}/workflow-runs", json={"workflow_id": "test_wf", "input": input, **kw}, headers=AUTH)
    assert r.status_code == 201, r.text
    return r.json()


def get(env, rid):
    return env["c"].get(f"{BASE}/workflow-runs/{rid}", headers=AUTH).json()


def st(run):
    return {s["node_id"]: s["status"] for s in run["steps"]}


# ---------------------------------------------------------------- graph rules
def test_graph_rules():
    steps = {n[0]: Step(n[0], n[1], graph.label_of(n)) for n in ROUTED["nodes"]}
    steps["in"].status, steps["in"].output = "done", Q
    assert graph.advance(ROUTED, steps) == (["r"], [])
    steps["r"].status, steps["r"].chosen, steps["r"].output = "done", ["ind"], Q
    assert graph.advance(ROUTED, steps) == (["ind"], ["doc"])
    assert graph.route_by_rule(ROUTED, "r", "What is the national strategy for tourism?")[0] == "doc"
    assert graph.route_by_rule(ROUTED, "r", Q)[0] == "ind"
    target, why = graph.route_by_rule(ROUTED, "r", "Hello there")
    assert target == "doc" and "default" in why  # no match: the last path
    assert graph.route_by_rule({**ROUTED, "np": {"r": {"def": "ind_agent"}}}, "r", "Hello there")[0] == "ind"
    assert [graph.seconds(x, 1) for x in ("60 s", "2 min", "24 h", "3 days", "bad", None)] == [60, 120, 86400, 259200, 1, 1]
    assert graph.agent_prompt(Q, [("in", "Request", Q)], "in") == Q
    assert graph.agent_prompt(Q, [("a", "ind_agent", "4.17%")], "in") == f"{Q}\n\nUse these results from earlier steps:\n\n### ind_agent\n4.17%"
    bad = cfg([["in", "entry", "", ""], ["t", "tool", "kap:search", ""], ["a", "agent", "ghost", ""], ["r", "router", "", ""], ["out", "output", "", ""]], [["in", "a", "seq", "", ""], ["r", "a", "cond", "", ""]])
    probs = graph.problems(bad, AGENTS)
    assert any("tool steps cannot run" in p for p in probs) and any("ghost: no such agent" in p for p in probs)
    assert any("two conditional paths" in p for p in probs) and any("Set the condition" in p for p in probs) and any("no path from the entry" in p for p in probs)
    assert graph.problems(SEQ, AGENTS) == []


# ---------------------------------------------------------------- documents
def test_workflow_documents(env):
    c = env["c"]
    r = c.put(f"{BASE}/workflows/test_wf", json={"doc": doc(SEQ)}, headers=AUTH)
    assert r.status_code == 201 and r.json()["doc"]["n"] == "Test workflow"
    assert c.put(f"{BASE}/workflows/test_wf", json={"doc": {**doc(SEQ), "n": "Renamed"}}, headers=AUTH).status_code == 200
    page = c.get(f"{BASE}/workflows", headers=AUTH).json()
    assert page["total"] == 1 and page["items"][0]["doc"]["n"] == "Renamed"
    assert c.put(f"{BASE}/workflows/Bad-Id", json={"doc": doc(SEQ)}, headers=AUTH).status_code == 422
    assert c.put(f"{BASE}/workflows/test_wf", json={"doc": {"n": "x", "vers": []}}, headers=AUTH).status_code == 422
    assert c.delete(f"{BASE}/workflows/test_wf", headers=AUTH).status_code == 204
    assert c.get(f"{BASE}/workflows/test_wf", headers=AUTH).status_code == 404


def test_start_refusals(env):
    c = env["c"]
    assert c.post(f"{BASE}/workflow-runs", json={"workflow_id": "nope_wf", "input": Q}, headers=AUTH).status_code == 404
    c.put(f"{BASE}/workflows/dis_wf", json={"doc": doc(SEQ, dis={"reason": "unsafe answers"})}, headers=AUTH)
    r = c.post(f"{BASE}/workflow-runs", json={"workflow_id": "dis_wf", "input": Q}, headers=AUTH)
    assert r.status_code == 409 and "unsafe answers" in r.json()["message"]
    c.put(f"{BASE}/workflows/test_wf", json={"doc": doc(SEQ)}, headers=AUTH)
    bad = cfg([["in", "entry", "", ""], ["a", "agent", "ghost", ""], ["out", "output", "", ""]], [["in", "a", "seq", "", ""], ["a", "out", "seq", "", ""]])
    r = c.post(f"{BASE}/workflow-runs", json={"workflow_id": "test_wf", "input": Q, "cfg": bad}, headers=AUTH)
    assert r.status_code == 422 and r.json()["type"] == "workflow_invalid" and r.json()["data"]["problems"] == ["ghost: no such agent in KAP."]
    assert c.post(f"{BASE}/workflow-runs", json={"workflow_id": "test_wf", "input": Q, "cfg": SEQ, "trigger": "api"}, headers=AUTH).status_code == 422
    assert c.post(f"{BASE}/workflow-runs", json={"workflow_id": "test_wf", "input": Q, "version": "v9"}, headers=AUTH).status_code == 404


# ---------------------------------------------------------------- runs
def test_sequential_run(env):
    kap = env["kap"]
    run = start(env, SEQ)
    assert run["version"] == "v1" and run["status"] == "queued" and run["workflow_name"] == "Test workflow"  # the published version
    assert env["pool"].enqueued[-1]["kind"] == "wf_tick"
    tick(env, run["id"])
    r = get(env, run["id"])
    assert r["status"] == "running" and st(r) == {"in": "done", "a": "running", "b": "pending", "out": "pending"}
    j = kap.open_jobs("ind_agent")[0]
    assert j["user_prompt"] == Q and j["callback_url"].endswith(f"/agents/{AGENTS['ind_agent']}/jobs")
    kap.finish("ind_agent", '{"answer": "Inflation was 4.17% in March 2026."}')
    tick(env, run["id"])
    assert kap.open_jobs("writer")[0]["user_prompt"] == f"{Q}\n\nUse these results from earlier steps:\n\n### ind_agent\nInflation was 4.17% in March 2026."
    kap.finish("writer", "Inflation in March 2026 was 4.17%.")
    out = tick(env, run["id"])
    r = get(env, run["id"])
    assert out == {"status": "succeeded"} and r["status"] == "succeeded" and r["answer"] == "Inflation in March 2026 was 4.17%."
    assert [s["node_id"] for s in r["steps"]] == ["in", "a", "b", "out"] and [s["seq"] for s in r["steps"]] == [0, 1, 2, 3]
    assert r["agent_jobs"] == 2 and len(env["db"]["mirrored"]) >= 2
    assert tick(env, run["id"]) == {"skipped": "run is not active"}
    page = env["c"].get(f"{BASE}/workflow-runs", params={"workflow_id": "test_wf"}, headers=AUTH).json()
    assert page["total"] == 1 and page["items"][0]["path"] == [["in", "done", 0], ["a", "done", 1], ["b", "done", 2], ["out", "done", 3]] and "steps" not in page["items"][0]


def test_rule_router_skips_the_other_branch(env):
    kap = env["kap"]
    run = start(env, ROUTED)
    tick(env, run["id"])
    r = get(env, run["id"])
    assert st(r) == {"in": "done", "r": "done", "ind": "running", "doc": "skipped", "out": "pending"}
    assert next(s for s in r["steps"] if s["node_id"] == "r")["note"].startswith("Routed to ind_agent.")
    kap.finish("ind_agent", "4.17%")
    tick(env, run["id"])
    r = get(env, run["id"])
    assert r["status"] == "succeeded" and r["answer"] == "4.17%" and not kap.open_jobs("doc_agent")


def test_model_router(env):
    env["model"].route = 2
    c = {**ROUTED, "np": {"r": {"by": "writer"}}}
    run = start(env, c, input="anything")
    tick(env, run["id"])
    r = get(env, run["id"])
    assert st(r)["doc"] == "running" and st(r)["ind"] == "skipped"
    assert "Decided by the model" in next(s for s in r["steps"] if s["node_id"] == "r")["note"]
    assert "PATHS" in env["model"].calls[0][1]["content"] and "2. strategy | documents | policy" in env["model"].calls[0][1]["content"]


def test_parallel_then_aggregate(env):
    kap = env["kap"]
    run = start(env, PARALLEL)
    tick(env, run["id"])
    assert len(kap.open_jobs()) == 2  # both branches at once
    kap.finish("ind_agent", "Qatar 4.17%")
    tick(env, run["id"])
    assert st(get(env, run["id"]))["g"] == "pending"  # waits for all incoming steps
    kap.finish("doc_agent", "Strategy target 3%")
    tick(env, run["id"])
    prompt = kap.open_jobs("writer")[0]["user_prompt"]
    assert prompt.startswith(f"Request: {Q}\n\nCombine the results below") and "### ind_agent\nQatar 4.17%" in prompt and "### doc_agent\nStrategy target 3%" in prompt
    kap.finish("writer", "Combined.")
    tick(env, run["id"])
    assert get(env, run["id"])["answer"] == "Combined."

    # "First successful result" needs no aggregator agent and does not wait
    c2 = {**PARALLEL, "np": {"g": {"merge": "First successful result", "wait": "First completed step"}}}
    run = start(env, c2)
    tick(env, run["id"])
    kap.finish("doc_agent", "Doc first")
    tick(env, run["id"])
    r = get(env, run["id"])
    assert r["status"] == "succeeded" and r["answer"] == "Doc first"


def test_failure_path_to_template_fallback(env):
    kap = env["kap"]
    c = cfg(
        [["in", "entry", "", ""], ["a", "agent", "ind_agent", ""], ["fb", "fallback", "Template answer", ""], ["out", "output", "", ""]],
        [["in", "a", "seq", "", ""], ["a", "out", "seq", "", ""], ["a", "fb", "fallback", "", ""], ["fb", "out", "seq", "", ""]],
        np={"a": {"retries": "0"}, "fb": {"action": "Return a template answer", "text": "No official data is available."}},
    )
    run = start(env, c)
    tick(env, run["id"])
    kap.finish("ind_agent", None, status="failed", error="tool timeout")
    tick(env, run["id"])
    r = get(env, run["id"])
    assert r["status"] == "succeeded" and r["answer"] == "No official data is available." and r["fallback_used"] is True
    assert st(r) == {"in": "done", "a": "failed", "fb": "done", "out": "done"}
    assert next(s for s in r["steps"] if s["node_id"] == "a")["note"] == "The KAP job failed: tool timeout"


def test_retry_then_success(env):
    kap = env["kap"]
    run = start(env, SEQ)  # default retries: 1
    tick(env, run["id"])
    kap.finish("ind_agent", None, status="failed", error="model overloaded")
    tick(env, run["id"])
    s = next(x for x in get(env, run["id"])["steps"] if x["node_id"] == "a")
    assert s["status"] == "running" and s["attempts"] == 2 and s["note"].startswith("Retry 1 after: The KAP job failed")
    kap.finish("ind_agent", "4.17%")
    tick(env, run["id"])
    assert st(get(env, run["id"]))["b"] == "running"


def test_failure_without_path_stops_with_explanation(env):
    kap = env["kap"]
    run = start(env, {**SEQ, "np": {"a": {"retries": "0"}}, "onErr": "Stop and return an explanation"})
    tick(env, run["id"])
    kap.finish("ind_agent", None, status="failed", error="boom")
    tick(env, run["id"])
    r = get(env, run["id"])
    assert r["status"] == "failed" and r["error"] == "ind_agent: The KAP job failed: boom"
    assert r["answer"] == "The request could not be completed. ind_agent: The KAP job failed: boom"
    assert st(r) == {"in": "done", "a": "failed", "b": "skipped", "out": "skipped"}


def test_continue_without_result(env):
    kap = env["kap"]
    c = {**PARALLEL, "np": {"a": {"retries": "0", "onErr": "Continue without this result"}}}
    run = start(env, c)
    tick(env, run["id"])
    kap.finish("ind_agent", None, status="failed", error="x")
    kap.finish("doc_agent", "Doc only")
    tick(env, run["id"])
    prompt = kap.open_jobs("writer")[0]["user_prompt"]
    assert "### doc_agent" in prompt and "ind_agent" not in prompt


def test_validator_failure_goes_to_fallback(env):
    env["model"].check, env["model"].reason = False, "4.71 is not in the results"
    c = cfg(
        [["in", "entry", "", ""], ["a", "agent", "ind_agent", ""], ["v", "review", "Numeric verifier", ""], ["fb", "fallback", "", ""], ["out", "output", "", ""]],
        [["in", "a", "seq", "", ""], ["a", "v", "seq", "", ""], ["v", "out", "seq", "", ""], ["v", "fb", "fallback", "", ""], ["fb", "out", "seq", "", ""]],
        np={"v": {"check": "Numbers match the source data"}},
    )
    run = start(env, c)
    tick(env, run["id"])
    env["kap"].finish("ind_agent", "Inflation was 4.71%.")
    tick(env, run["id"])
    r = get(env, run["id"])
    v = next(s for s in r["steps"] if s["node_id"] == "v")
    assert v["status"] == "failed" and v["note"] == "Numbers match the source data: failed. 4.71 is not in the results"
    assert r["status"] == "succeeded" and r["fallback_used"] and r["answer"] == runner.TEMPLATE
    rule = env["model"].calls[0][1]["content"]
    assert "Every number in the answer appears in" in rule and "[ind_agent] Inflation was 4.71%." in rule

    # passing check: the answer flows through
    env["model"].check = True
    run = start(env, c)
    tick(env, run["id"])
    env["kap"].finish("ind_agent", "Inflation was 4.17%.")
    tick(env, run["id"])
    r = get(env, run["id"])
    assert r["status"] == "succeeded" and r["answer"] == "Inflation was 4.17%." and st(r)["fb"] == "skipped"


def test_human_review(env):
    c = cfg(
        [["in", "entry", "", ""], ["a", "agent", "ind_agent", ""], ["h", "review", "Human review", ""], ["out", "output", "", ""]],
        [["in", "a", "seq", "", ""], ["a", "h", "seq", "", ""], ["h", "out", "seq", "", ""]],
        np={"h": {"rt": "human"}}, limits={"dur": "1 s", "steps": "5"},
    )
    run = start(env, c)
    tick(env, run["id"])
    env["kap"].finish("ind_agent", "Draft answer 4.17%")
    out = tick(env, run["id"])
    assert out["status"] == "waiting" and out["next_in_s"] == runner.WAITING_POLL_S
    r = get(env, run["id"])
    h = next(s for s in r["steps"] if s["node_id"] == "h")
    assert r["status"] == "waiting" and h["status"] == "waiting" and h["input"] == "Draft answer 4.17%"
    # the 1 s duration limit does not apply while a person reviews
    env["db"]["runs"][run["id"]]["started_at"] -= timedelta(minutes=5)
    assert tick(env, run["id"])["status"] == "waiting"

    rv = env["c"].post(f"{BASE}/workflow-runs/{run['id']}/steps/h/review", json={"approve": True, "answer": "Final: 4.17% (March 2026)", "comment": "checked"}, headers=AUTH)
    assert rv.status_code == 200 and next(s for s in rv.json()["steps"] if s["node_id"] == "h")["note"] == "Approved with an edited answer: checked"
    assert env["pool"].enqueued[-1]["idempotency_key"].startswith(f"wftick:{run['id']}:review:h:")
    tick(env, run["id"])
    r = get(env, run["id"])
    assert r["status"] == "succeeded" and r["answer"] == "Final: 4.17% (March 2026)"
    assert env["c"].post(f"{BASE}/workflow-runs/{run['id']}/steps/h/review", json={"approve": True}, headers=AUTH).status_code == 409


def test_duration_limit_and_cancel(env):
    run = start(env, {**SEQ, "limits": {"dur": "30 s", "steps": "12"}})
    tick(env, run["id"])
    env["db"]["runs"][run["id"]]["started_at"] -= timedelta(seconds=31)
    tick(env, run["id"])
    r = get(env, run["id"])
    assert r["status"] == "failed" and r["error"] == "The workflow ran longer than its time limit (30 s)."
    assert st(r)["a"] == "skipped"

    run = start(env, SEQ)
    tick(env, run["id"])
    r = env["c"].post(f"{BASE}/workflow-runs/{run['id']}/cancel", headers=AUTH).json()
    assert r["status"] == "cancelled" and st(r)["a"] == "skipped"
    assert tick(env, run["id"]) == {"skipped": "run is not active"}


def test_kap_busy_and_step_limit(env):
    kap = env["kap"]
    kap.busy = 1
    run = start(env, SEQ)
    out = tick(env, run["id"])
    r = get(env, run["id"])
    assert out["status"] == "running" and st(r)["a"] == "pending" and "KAP is busy" in next(s for s in r["steps"] if s["node_id"] == "a")["note"]
    tick(env, run["id"])
    assert st(get(env, run["id"]))["a"] == "running"
    kap.finish("ind_agent", "first run's answer")  # leave no open job behind for the next run

    run = start(env, {**SEQ, "limits": {"dur": "600 s", "steps": "1"}})
    tick(env, run["id"])
    kap.finish("ind_agent", "4.17%")
    tick(env, run["id"])
    r = get(env, run["id"])
    assert r["status"] == "failed" and "limit of 1 agent steps" in r["error"]


def test_lease_and_tick_failure(env, monkeypatch):
    run = start(env, SEQ)
    env["db"]["leases"][run["id"]] = True
    assert tick(env, run["id"]) == {"skipped": "another tick is running"}
    env["db"]["leases"].clear()

    async def boom(*_a, **_k):
        raise RuntimeError("database went away")

    monkeypatch.setattr(runner, "tick", boom)
    from kap_platform import queue

    job = queue.Job(id=None, tenant_id=None, kind="wf_tick", payload={"run_id": run["id"]}, attempts=3, max_attempts=3)
    with pytest.raises(RuntimeError):
        asyncio.run(runner.handle_wf_tick(env["pool"], env["kap"], env["settings"], job))
    r = get(env, run["id"])
    assert r["status"] == "failed" and r["error"] == "RuntimeError: database went away"
