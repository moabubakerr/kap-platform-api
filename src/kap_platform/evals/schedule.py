"""Automatic evaluation runs (called from the worker's scheduler tick).

A schedule runs a test set against an agent or a workflow's published version:
- every N minutes (`every_minutes`), and/or
- after new data (`on_data`): when a PostgreSQL export into one of the target's
  collections succeeded after the last one already evaluated. The first check
  only records the newest export, so creating a schedule does not start a run.

A run that falls behind the previous one, or below `min_pass_rate`, is marked
regressed (evals/runner.py, score.regression).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from ..config import Settings
from ..kap.client import KapClient
from . import runner
from .start import EvalRefused, start_eval_run

log = logging.getLogger(__name__)


def _ts(v: Any) -> datetime | None:
    if v is None or isinstance(v, datetime):
        return v
    return datetime.fromisoformat(str(v).replace("Z", "+00:00"))


async def target_collections(kap: KapClient, pool: Any, s: dict[str, Any]) -> list[str]:
    """Collections the schedule's target reads: the agent's, or those of every agent in the workflow's published version."""
    tid = s["tenant_id"]
    if s["target_kind"] == "agent":
        return list((await kap.get_agent(tid, s["agent_id"])).get("collections") or [])
    from ..workflows import runner as wf_runner
    from ..workflows.start import agents_in

    w = await wf_runner.SERVICES.store(pool).get_workflow(tid, s["workflow_id"])
    if w is None:
        return []
    names = set(agents_in(w["doc"]))
    return sorted({c for a in await kap.list_agents(tid) if a.get("name") in names for c in (a.get("collections") or [])})


async def schedule_tick(pool: Any, kap: KapClient, settings: Settings, now: datetime | None = None) -> list[str]:
    """Start the runs that are due. Returns the ids of the runs started."""
    now = now or datetime.now(timezone.utc)
    st = runner.SERVICES.store(pool)
    started: list[str] = []
    for s in await st.schedules_to_check(now):
        try:
            rid = await _check(st, kap, pool, s, now)
        except Exception:
            log.exception("evaluation schedule %s could not be checked", s["id"])
            continue
        if rid:
            started.append(rid)
    return started


async def _check(st: Any, kap: KapClient, pool: Any, s: dict[str, Any], now: datetime) -> str | None:
    fields: dict[str, Any] = {}
    due = bool(s.get("every_minutes")) and (s.get("next_run_at") is None or _ts(s["next_run_at"]) <= now)
    trigger = "schedule" if due else None
    if s.get("on_data"):
        latest = _ts(await st.latest_export(s["tenant_id"], await target_collections(kap, pool, s)))
        seen = _ts(s.get("data_seen_at"))
        if latest and seen is None:
            fields["data_seen_at"] = latest  # first look: remember, do not run
        elif latest and latest > seen:
            fields["data_seen_at"] = latest
            trigger = "data"
    if due:
        fields["next_run_at"] = now + timedelta(minutes=int(s["every_minutes"]))
    rid = None
    if trigger:
        opts = dict(s.get("options") or {})
        if s.get("min_pass_rate") is not None:
            opts["min_pass_rate"] = float(s["min_pass_rate"])
        label = "Scheduled run" if trigger == "schedule" else "After new data"
        try:
            rid = await start_eval_run(
                st, kap, pool, s["tenant_id"], s["set_id"], agent_id=s.get("agent_id"), workflow_id=s.get("workflow_id"),
                label=label, options=opts, trigger=trigger,
            )
            fields["last_run_id"] = rid
        except EvalRefused as exc:
            if exc.status == 409:  # a run of this set and target is still going: try again at the next tick
                fields.pop("data_seen_at" if trigger == "data" else "next_run_at", None)
            else:
                log.warning("evaluation schedule %s: %s", s["id"], exc.message)
    if fields:
        await st.update_schedule(s["id"], fields)
    return rid
