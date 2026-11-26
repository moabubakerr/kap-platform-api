"""Start a workflow run: used by the REST API, by evaluations and by schedules."""
from __future__ import annotations

from typing import Any

from ..kap.client import KapClient
from . import graph, runner


class StartRefused(Exception):
    def __init__(self, status: int, kind: str, message: str, data: dict[str, Any] | None = None):
        super().__init__(message)
        self.status, self.kind, self.message, self.data = status, kind, message, data or {}


def version_of(doc: dict[str, Any], v: str | None) -> dict[str, Any] | None:
    """Version `v`, else the published one, else the newest."""
    vers = doc.get("vers") or []
    if v:
        return next((x for x in vers if x.get("v") == v), None)
    return next((x for x in vers if x.get("st") == "Published"), None) or (vers[0] if vers else None)


async def start_run(
    st: Any, kap: KapClient, pool: Any, tenant_id: str, workflow_id: str, input: str, *,
    version: str | None = None, cfg: dict[str, Any] | None = None, trigger: str = "test", created_by: str | None = None,
) -> str:
    """Create the run and its steps and queue the first tick. Returns the run id; raises StartRefused."""
    w = await st.get_workflow(tenant_id, workflow_id)
    if w is None:
        raise StartRefused(404, "not_found", "No such workflow.")
    doc = w["doc"]
    if doc.get("dis"):
        raise StartRefused(409, "disabled", f"The workflow is disabled: {(doc['dis'] or {}).get('reason') or 'no reason given'}.")
    ver = version_of(doc, version)
    if ver is None:
        raise StartRefused(404, "not_found", f"The workflow has no version {version}." if version else "The workflow has no version.")
    if cfg is not None and trigger != "test":
        raise StartRefused(422, "validation_error", "Unsaved changes (cfg) can only be run as a test.")
    cfg = cfg if cfg is not None else ver.get("cfg") or {}
    agents = {a["name"]: a["id"] for a in await kap.list_agents(tenant_id)}
    probs = graph.problems(cfg, agents)
    if probs:
        raise StartRefused(422, "workflow_invalid", "The workflow cannot run: " + " ".join(probs[:5]), {"problems": probs})
    used = {n[2] for n in graph.nodes_of(cfg) if n[1] in ("agent", "aggregate") and n[2]}
    steps = [graph.Step(n[0], n[1], graph.label_of(n)) for n in graph.nodes_of(cfg)]
    rid = await st.create_run(tenant_id, {
        "workflow_id": workflow_id, "workflow_name": doc.get("n") or workflow_id, "version": ver.get("v") or "?",
        "trigger": trigger, "input": input.strip(), "cfg": cfg, "agents": {k: agents[k] for k in used}, "created_by": created_by,
    }, steps)
    await runner.enqueue_tick(pool, {"id": rid, "tenant_id": tenant_id}, "start")
    return rid


def agents_in(doc: dict[str, Any], version: str | None = None) -> list[str]:
    """Names of the agents a workflow version uses (agent and aggregator steps)."""
    ver = version_of(doc, version)
    cfg = (ver or {}).get("cfg") or {}
    return sorted({n[2] for n in graph.nodes_of(cfg) if n[1] in ("agent", "aggregate") and n[2]})
