"""In-memory stand-ins for evaluations: store, KAP agent jobs, judge."""
from __future__ import annotations

import copy
import uuid
from datetime import datetime, timezone
from typing import Any

from kap_platform.evals.store import ACTIVE, Conflict, InUse
from kap_platform.kap.client import KapError
from kap_platform.traces.build import Step, Trace

from pg_fakes import SBX

AGENT = "aaaaaaaa-0000-4000-8000-000000000001"


def now() -> datetime:
    return datetime.now(timezone.utc)


class MemoryEvalStore:
    """Same methods as EvalStore, kept in dicts (shared through `db`)."""

    def __init__(self, db: dict[str, Any]):
        self.db = db
        for k in ("sets", "cases", "runs", "results", "mirrored", "traces", "trace_status"):
            db.setdefault(k, {} if k not in ("mirrored",) else [])

    # sets
    def _set_row(self, s: dict[str, Any]) -> dict[str, Any]:
        runs = sorted((r for r in self.db["runs"].values() if r["set_id"] == s["id"]), key=lambda r: r["created_at"], reverse=True)
        last = runs[0] if runs else None
        return {**copy.deepcopy(s), "case_count": sum(1 for c in self.db["cases"].values() if c["set_id"] == s["id"]),
                "last_run": {k: last[k] for k in ("id", "status", "agent_name", "target_kind", "workflow_id", "version", "regressed", "summary", "created_at", "finished_at")} if last else None}

    async def list_sets(self, tenant_id):
        return [self._set_row(s) for s in sorted(self.db["sets"].values(), key=lambda s: s["name"]) if s["tenant_id"] == tenant_id]

    async def get_set(self, tenant_id, set_id):
        s = self.db["sets"].get(set_id)
        return self._set_row(s) if s and s["tenant_id"] == tenant_id else None

    async def create_set(self, tenant_id, fields):
        if any(s["tenant_id"] == tenant_id and s["name"] == fields["name"] for s in self.db["sets"].values()):
            raise Conflict("A test set with this name already exists.")
        sid = str(uuid.uuid4())
        self.db["sets"][sid] = {"id": sid, "tenant_id": tenant_id, "name": fields["name"], "description": fields.get("description") or "", "created_at": now(), "updated_at": now()}
        return sid

    async def update_set(self, tenant_id, set_id, fields):
        if "name" in fields and any(s["tenant_id"] == tenant_id and s["name"] == fields["name"] and s["id"] != set_id for s in self.db["sets"].values()):
            raise Conflict("A test set with this name already exists.")
        self.db["sets"][set_id].update(fields, updated_at=now())

    async def delete_set(self, tenant_id, set_id):
        if any(r["set_id"] == set_id and r["status"] in ACTIVE for r in self.db["runs"].values()):
            raise InUse("An evaluation of this test set is running. Cancel it first.")
        self.db["sets"].pop(set_id, None)
        self.db["cases"] = {k: c for k, c in self.db["cases"].items() if c["set_id"] != set_id}
        for rid in [k for k, r in self.db["runs"].items() if r["set_id"] == set_id]:
            self.db["runs"].pop(rid)

    # cases
    async def list_cases(self, set_id):
        return [copy.deepcopy(c) for c in sorted((c for c in self.db["cases"].values() if c["set_id"] == set_id), key=lambda c: (c["position"], c["created_at"]))]

    async def get_case(self, set_id, case_id):
        c = self.db["cases"].get(case_id)
        return copy.deepcopy(c) if c and c["set_id"] == set_id else None

    async def add_cases(self, set_id, cases, replace=False):
        if replace:
            self.db["cases"] = {k: c for k, c in self.db["cases"].items() if c["set_id"] != set_id}
        start = max((c["position"] for c in self.db["cases"].values() if c["set_id"] == set_id), default=-1) + 1
        ids = []
        for i, c in enumerate(cases):
            cid = str(uuid.uuid4())
            self.db["cases"][cid] = {"id": cid, "set_id": set_id, "position": start + i, "question": c["question"], "expected": c.get("expected") or "",
                                     "must_include": list(c.get("must_include") or []), "must_not_include": list(c.get("must_not_include") or []),
                                     "tags": list(c.get("tags") or []), "created_at": now(), "updated_at": now()}
            ids.append(cid)
        return ids

    async def update_case(self, set_id, case_id, fields):
        self.db["cases"][case_id].update(fields, updated_at=now())

    async def delete_case(self, set_id, case_id):
        self.db["cases"].pop(case_id, None)

    # runs
    async def create_run(self, tenant_id, fields, cases):
        rid = str(uuid.uuid4())
        self.db["runs"][rid] = {"id": rid, "tenant_id": tenant_id, "set_id": fields["set_id"], "agent_id": fields.get("agent_id"), "agent_name": fields.get("agent_name"),
                                "agent": fields.get("agent"), "label": fields.get("label") or "", "options": fields.get("options") or {}, "status": "queued",
                                "summary": None, "polls": 0, "error": None, "created_at": now(), "started_at": None, "finished_at": None,
                                "target_kind": fields.get("target_kind") or "agent", "workflow_id": fields.get("workflow_id"), "version": fields.get("version"),
                                "trigger": fields.get("trigger") or "manual", "regressed": None, "regress_reason": None}
        for i, c in enumerate(cases):
            xid = str(uuid.uuid4())
            self.db["results"][xid] = {"id": xid, "run_id": rid, "case_id": c["id"], "position": i, "question": c["question"], "expected": c.get("expected") or "",
                                       "must_include": c.get("must_include") or [], "must_not_include": c.get("must_not_include") or [], "status": "pending",
                                       **{k: None for k in ("job_id", "wf_run_id", "job_status", "answer", "checks", "judge", "passed", "reason", "metrics", "error", "submitted_at", "answered_at", "scored_at")}}
        return rid

    def _run_row(self, r):
        return {**copy.deepcopy(r), "set_name": self.db["sets"][r["set_id"]]["name"]}

    async def list_runs(self, tenant_id, set_id, agent_id, limit, offset, workflow_id=None):
        rs = [r for r in self.db["runs"].values() if r["tenant_id"] == tenant_id and (not set_id or r["set_id"] == set_id) and (not agent_id or r["agent_id"] == agent_id)
              and (not workflow_id or r["workflow_id"] == workflow_id)]
        rs.sort(key=lambda r: r["created_at"], reverse=True)
        return [self._run_row(r) for r in rs[offset:offset + limit]], len(rs)

    async def get_run(self, tenant_id, run_id):
        r = self.db["runs"].get(run_id)
        return self._run_row(r) if r and (tenant_id is None or r["tenant_id"] == tenant_id) else None

    async def update_run(self, run_id, fields):
        self.db["runs"][run_id].update(copy.deepcopy(fields))

    async def delete_run(self, tenant_id, run_id):
        self.db["runs"].pop(run_id, None)

    async def has_active_run(self, tenant_id, set_id, target_kind, target_id):
        col = "agent_id" if target_kind == "agent" else "workflow_id"
        return any(r["set_id"] == set_id and r["target_kind"] == target_kind and r[col] == target_id and r["status"] in ACTIVE for r in self.db["runs"].values())

    async def previous_run(self, run):
        col = "agent_id" if run.get("target_kind", "agent") == "agent" else "workflow_id"
        rs = [r for r in self.db["runs"].values() if r["set_id"] == run["set_id"] and r["target_kind"] == run.get("target_kind", "agent") and r[col] == run[col]
              and r["status"] == "succeeded" and r["id"] != run["id"] and r["created_at"] < run["created_at"]]
        return self._run_row(max(rs, key=lambda r: r["created_at"])) if rs else None

    # schedules
    async def list_schedules(self, tenant_id, set_id=None):
        return [copy.deepcopy(s) for s in self.db.setdefault("schedules", {}).values() if s["tenant_id"] == tenant_id and (not set_id or s["set_id"] == set_id)]

    async def get_schedule(self, tenant_id, sched_id):
        s = self.db.setdefault("schedules", {}).get(sched_id)
        return copy.deepcopy(s) if s and (tenant_id is None or s["tenant_id"] == tenant_id) else None

    async def create_schedule(self, tenant_id, fields):
        sid = str(uuid.uuid4())
        base = {k: None for k in ("agent_id", "workflow_id", "every_minutes", "min_pass_rate", "next_run_at", "data_seen_at", "last_run_id")}
        self.db.setdefault("schedules", {})[sid] = {**base, "on_data": False, "options": {}, "enabled": True, **copy.deepcopy(fields), "id": sid, "tenant_id": tenant_id, "created_at": now(), "updated_at": now()}
        return sid

    async def update_schedule(self, sched_id, fields):
        self.db["schedules"][sched_id].update(copy.deepcopy(fields), updated_at=now())

    async def delete_schedule(self, tenant_id, sched_id):
        self.db.setdefault("schedules", {}).pop(sched_id, None)

    async def schedules_to_check(self, when):
        return [copy.deepcopy(s) for s in self.db.setdefault("schedules", {}).values() if s["enabled"] and (s["on_data"] or (s["next_run_at"] and s["next_run_at"] <= when))]

    async def latest_export(self, tenant_id, collections):
        times = [t for c, t in self.db.setdefault("exports", {}).items() if c in collections]
        return max(times) if times else None

    # results
    async def results(self, run_id):
        return [copy.deepcopy(x) for x in sorted((x for x in self.db["results"].values() if x["run_id"] == run_id), key=lambda x: x["position"])]

    async def update_result(self, result_id, fields):
        self.db["results"][result_id].update(copy.deepcopy(fields))

    # mirrored jobs and traces
    async def mirror_job(self, tenant_id, agent, job):
        self.db["mirrored"].append((job["id"], job.get("status")))
        self.db["trace_status"].setdefault(job["id"], "pending")

    async def trace_status(self, job_id):
        return self.db["trace_status"].get(job_id)

    async def trace(self, job_id):
        return self.db["traces"].get(job_id)


class FakeAgentKap:
    """One agent; jobs finish when the test says so (`finish`)."""

    def __init__(self):
        self.agents = {AGENT: {"id": AGENT, "name": "askai_indicators_agent", "type": "planner_and_executor", "model_ref": "chat_default",
                               "collections": ["askai_indicators"], "allowed_tools": ["find_indicator"], "updated_at": "2026-11-26T08:00:00Z"}}
        self.jobs: dict[str, dict[str, Any]] = {}
        self.submitted: list[dict[str, Any]] = []
        self.busy = False

    def tenant(self, tenant_id):
        if tenant_id != SBX:
            raise KeyError(tenant_id)
        return tenant_id

    async def aclose(self):
        pass

    async def get_agent(self, tenant_id, agent_id):
        if agent_id not in self.agents:
            raise KapError(404, {"type": "not_found", "message": "Agent not found"})
        return copy.deepcopy(self.agents[agent_id])

    async def submit_agent_job(self, tenant_id, agent_id, *, user_prompt=None, payload=None, callback_url=None, callback_headers=None):
        if self.busy:
            raise KapError(429, {"type": "rate_limited", "message": "slow down"})
        jid = str(uuid.uuid4())
        job = {"id": jid, "agent_id": agent_id, "status": "waiting", "user_prompt": user_prompt, "result": None, "error": None,
               "created_at": now().isoformat(), "running_at": None, "finished_at": None}
        self.jobs[jid] = job
        self.submitted.append({"prompt": user_prompt, "callback_url": callback_url, "callback_headers": callback_headers})
        return copy.deepcopy(job)

    async def get_agent_job(self, tenant_id, agent_id, job_id):
        return copy.deepcopy(self.jobs[job_id])

    def finish(self, prompt: str, result: str | None, status: str = "succeeded", error: str | None = None):
        job = next(j for j in self.jobs.values() if j["user_prompt"] == prompt)
        t = now()
        job.update(status=status, result=result, error=error, running_at=t.isoformat(), finished_at=t.isoformat())
        return job["id"]


class FakeJudge:
    def __init__(self, verdicts: dict[str, dict[str, Any]] | None = None):
        self.verdicts = verdicts or {}
        self.calls: list[dict[str, Any]] = []

    async def grade(self, question, expected, must_include, answer, evidence):
        self.calls.append({"question": question, "expected": expected, "answer": answer, "evidence": evidence})
        return dict(self.verdicts.get(question, {"correctness": 5, "grounding": 5, "completeness": 5, "reason": "Correct."}))

    async def aclose(self):
        pass


def tool_trace(job_id: str, tool: str, output: Any, status: str = "ok", tokens: int = 1000) -> Trace:
    t = now()
    steps = [Step(0, "model", "plan", t, t, 10), Step(1, "tool", tool, t, t, 5, input={"text": "inflation"}, output=output, status=status), Step(2, "model", "answer", t, t, 10)]
    return Trace(job_id=job_id, steps=steps, totals={"model_calls": 2, "tool_calls": 1, "prompt_tokens": tokens - 100, "completion_tokens": 100})
