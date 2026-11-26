"""Start an evaluation run of a test set against an agent or a workflow version.
Used by the REST API and by schedules (evals/schedule.py)."""
from __future__ import annotations

from typing import Any

from ..kap.client import KapClient, KapError
from . import runner

AGENT_KEYS = ("id", "name", "type", "model_ref", "collections", "allowed_tools", "updated_at")


class EvalRefused(Exception):
    def __init__(self, status: int, kind: str, message: str):
        super().__init__(message)
        self.status, self.kind, self.message = status, kind, message


async def start_eval_run(
    st: Any, kap: KapClient, pool: Any, tenant_id: str, set_id: str, *,
    agent_id: str | None = None, workflow_id: str | None = None, version: str | None = None,
    label: str = "", options: dict[str, Any] | None = None, trigger: str = "manual",
) -> str:
    """Create the run with one result per question and queue its first tick. Returns the run id; raises EvalRefused."""
    if bool(agent_id) == bool(workflow_id):
        raise EvalRefused(422, "validation_error", "Choose an agent or a workflow to evaluate.")
    s = await st.get_set(tenant_id, set_id)
    if s is None:
        raise EvalRefused(404, "not_found", "No such test set.")
    cases = await st.list_cases(set_id)
    if not cases:
        raise EvalRefused(422, "validation_error", "The test set has no questions yet.")
    fields: dict[str, Any] = {"set_id": set_id, "label": label.strip(), "options": options or {}, "trigger": trigger}
    if agent_id:
        try:
            agent = await kap.get_agent(tenant_id, agent_id)
        except KapError as exc:
            if exc.status == 404:
                raise EvalRefused(404, "not_found", "KAP has no such agent.") from None
            raise
        if await st.has_active_run(tenant_id, set_id, "agent", agent_id):
            raise EvalRefused(409, "conflict", "This test set is already being run against this agent. Wait for it to finish or cancel it.")
        fields.update(target_kind="agent", agent_id=agent_id, agent_name=agent.get("name"), agent={k: agent.get(k) for k in AGENT_KEYS})
    else:
        from ..workflows import runner as wf_runner
        from ..workflows.start import version_of

        w = await wf_runner.SERVICES.store(pool).get_workflow(tenant_id, workflow_id)
        if w is None:
            raise EvalRefused(404, "not_found", "No such workflow.")
        ver = version_of(w["doc"], version)
        if ver is None:
            raise EvalRefused(404, "not_found", f"The workflow has no version {version}." if version else "The workflow has no version.")
        if await st.has_active_run(tenant_id, set_id, "workflow", workflow_id):
            raise EvalRefused(409, "conflict", "This test set is already being run against this workflow. Wait for it to finish or cancel it.")
        name = w["doc"].get("n") or workflow_id
        fields.update(
            target_kind="workflow", workflow_id=workflow_id, version=ver.get("v"), agent_name=name,
            agent={"kind": "workflow", "name": name, "version": ver.get("v"), "status": ver.get("st"), "cfg": ver.get("cfg")},
        )
    rid = await st.create_run(tenant_id, fields, cases)
    await runner.enqueue_tick(pool, {"id": rid, "tenant_id": tenant_id}, 0)
    return rid
