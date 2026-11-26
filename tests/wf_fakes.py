"""In-memory stand-ins for workflows: store, KAP (agents + jobs), model server."""
from __future__ import annotations

import copy
import json
import uuid
from datetime import datetime, timezone
from typing import Any

from kap_platform.kap.client import KapError
from kap_platform.workflows.graph import Step

from pg_fakes import SBX

AGENTS = {"ind_agent": "aaaaaaaa-0000-4000-8000-0000000000a1", "doc_agent": "aaaaaaaa-0000-4000-8000-0000000000a2", "writer": "aaaaaaaa-0000-4000-8000-0000000000a3"}


def now() -> datetime:
    return datetime.now(timezone.utc)


class MemoryWfStore:
    def __init__(self, db: dict[str, Any]):
        self.db = db
        for k in ("wfs", "runs", "steps", "mirrored", "leases"):
            db.setdefault(k, [] if k == "mirrored" else {})

    async def list_workflows(self, tenant_id):
        return [{**copy.deepcopy(w), "last_run": None} for (t, _), w in sorted(self.db["wfs"].items()) if t == tenant_id]

    async def get_workflow(self, tenant_id, wf_id):
        w = self.db["wfs"].get((tenant_id, wf_id))
        return copy.deepcopy(w) if w else None

    async def put_workflow(self, tenant_id, wf_id, doc):
        created = (tenant_id, wf_id) not in self.db["wfs"]
        prev = self.db["wfs"].get((tenant_id, wf_id))
        self.db["wfs"][(tenant_id, wf_id)] = {"id": wf_id, "doc": copy.deepcopy(doc), "created_at": prev["created_at"] if prev else now(), "updated_at": now()}
        return created

    async def delete_workflow(self, tenant_id, wf_id):
        self.db["wfs"].pop((tenant_id, wf_id), None)

    async def create_run(self, tenant_id, fields, steps):
        rid = str(uuid.uuid4())
        self.db["runs"][rid] = {"id": rid, "tenant_id": tenant_id, "workflow_id": fields["workflow_id"], "workflow_name": fields.get("workflow_name"), "version": fields["version"],
                                "trigger": fields.get("trigger") or "test", "input": fields["input"], "cfg": copy.deepcopy(fields["cfg"]), "agents": dict(fields.get("agents") or {}),
                                "status": "queued", "answer": None, "error": None, "fallback_used": False, "agent_jobs": 0, "polls": 0, "created_by": fields.get("created_by"),
                                "created_at": now(), "started_at": None, "finished_at": None}
        self.db["steps"][rid] = {s.node_id: copy.deepcopy(s) for s in steps}
        return rid

    async def get_run(self, tenant_id, run_id):
        r = self.db["runs"].get(run_id)
        return copy.deepcopy(r) if r and (tenant_id is None or r["tenant_id"] == tenant_id) else None

    async def list_runs(self, tenant_id, workflow_id, limit, offset):
        rs = sorted((r for r in self.db["runs"].values() if r["tenant_id"] == tenant_id and (workflow_id is None or r["workflow_id"] == workflow_id)), key=lambda r: r["created_at"], reverse=True)
        out = []
        for r in rs[offset:offset + limit]:
            steps = sorted(self.db["steps"][r["id"]].values(), key=lambda s: (s.seq is None, s.seq or 0, s.node_id))
            out.append({**copy.deepcopy(r), "path": [[s.node_id, s.status, s.seq] for s in steps]})
        return out, len(rs)

    async def update_run(self, run_id, fields):
        self.db["runs"][run_id].update(copy.deepcopy(fields))

    async def try_lease(self, run_id, seconds):
        if self.db["leases"].get(run_id):
            return False
        self.db["leases"][run_id] = True
        return True

    async def release(self, run_id):
        self.db["leases"].pop(run_id, None)

    async def steps(self, run_id):
        return copy.deepcopy(self.db["steps"][run_id])

    async def update_step(self, run_id, node_id, fields):
        s: Step = self.db["steps"][run_id][node_id]
        for k, v in copy.deepcopy(fields).items():
            setattr(s, k, v)

    async def mirror_job(self, tenant_id, agent, job):
        self.db["mirrored"].append((job["id"], agent.get("name")))

    async def trace(self, job_id):
        return None


class FakeWfKap:
    def __init__(self):
        self.jobs: dict[str, dict[str, Any]] = {}
        self.order: list[str] = []
        self.busy = 0  # next N submits answer 429

    def tenant(self, tenant_id):
        if tenant_id != SBX:
            raise KeyError(tenant_id)
        return tenant_id

    async def aclose(self):
        pass

    async def list_agents(self, tenant_id):
        return [{"id": i, "name": n} for n, i in AGENTS.items()]

    async def submit_agent_job(self, tenant_id, agent_id, *, user_prompt=None, payload=None, callback_url=None, callback_headers=None):
        if self.busy:
            self.busy -= 1
            raise KapError(429, {"type": "rate_limited", "message": "busy"})
        name = next(n for n, i in AGENTS.items() if i == agent_id)
        jid = str(uuid.uuid4())
        self.jobs[jid] = {"id": jid, "agent_id": agent_id, "agent": name, "status": "waiting", "user_prompt": user_prompt, "result": None, "error": None, "created_at": now().isoformat(),
                          "running_at": None, "finished_at": None, "callback_url": callback_url}
        self.order.append(jid)
        return copy.deepcopy(self.jobs[jid])

    async def get_agent_job(self, tenant_id, agent_id, job_id):
        return copy.deepcopy(self.jobs[job_id])

    def open_jobs(self, agent: str | None = None):
        return [self.jobs[j] for j in self.order if self.jobs[j]["status"] == "waiting" and (agent is None or self.jobs[j]["agent"] == agent)]

    def finish(self, agent: str, result: str | None, status: str = "succeeded", error: str | None = None) -> dict[str, Any]:
        job = self.open_jobs(agent)[0]
        job.update(status=status, result=result, error=error, running_at=now().isoformat(), finished_at=now().isoformat())
        return job


class FakeModel:
    """Replies by the kind of question: routing (system mentions 'route') or checking."""

    def __init__(self, route: int = 1, check: bool = True, reason: str = "ok"):
        self.route, self.check, self.reason = route, check, reason
        self.calls: list[list[dict[str, str]]] = []

    async def complete(self, messages, max_tokens=400):
        self.calls.append(messages)
        if "route" in messages[0]["content"]:
            return json.dumps({"route": self.route, "reason": self.reason})
        return json.dumps({"pass": self.check, "reason": self.reason})

    async def aclose(self):
        pass
