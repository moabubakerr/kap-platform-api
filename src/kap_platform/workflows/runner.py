"""Workflow runs on the job queue.

  wf_tick  {run_id}  one tick of a run (a new delayed job each time, no sleeping):
                     1. follow the KAP jobs of running agent steps (retries, step time-outs)
                     2. human steps past their time limit
                     3. start every step that became ready; platform steps (router,
                        validator, fallback, output) finish at once, agent steps
                        submit a KAP job (with our callback, so it is traced)
                     4. decide whether the run finished

Step settings from the designer that are honoured:
  agent / aggregator  timeout, retries, onErr (Use the failure path | Continue without this result | Stop the workflow)
  aggregator          merge (Merge all results → the aggregator agent combines | First successful result), wait
  router              by (Rule-based conditions → words of the conditions | an agent's name → the model decides), def
  validator           check, onFail (Use the fallback | Stop and return an explanation)
  human review        tl, late (Reject the result | Escalate… → keeps waiting; escalation is not available)
  fallback            action (Return a template answer | Hand over to a human reviewer | Stop …), text
  workflow            limits.dur (automatic steps only), limits.steps (KAP jobs), onErr
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping

from .. import queue
from ..config import Settings
from ..evals.judge import Judge
from ..evals.runner import callback, evidence_of
from ..evals.score import answer_text
from ..kap.client import KapClient, KapError
from . import decide, graph
from .graph import Step
from .store import ACTIVE, WfStore

log = logging.getLogger(__name__)

TERMINAL = ("succeeded", "failed", "cancelled")
WAITING_POLL_S = 60
TEMPLATE = "No answer could be produced for this request. Please try again later or contact the team responsible."


class Services:
    def __init__(self, store_factory: Callable[[Any], Any] = WfStore, model_factory: Callable[[Settings], Any] = Judge):
        self.store_factory, self.model_factory = store_factory, model_factory

    def store(self, pool: Any) -> Any:
        return self.store_factory(pool)

    def model(self, settings: Settings) -> Any:
        return self.model_factory(settings)


SERVICES = Services()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _ts(v: Any) -> datetime | None:
    if v is None or isinstance(v, datetime):
        return v
    return datetime.fromisoformat(str(v).replace("Z", "+00:00"))


async def enqueue_tick(pool: Any, run: Mapping[str, Any], key: str, delay_s: float = 0) -> None:
    await queue.enqueue(
        pool, "wf_tick", {"run_id": run["id"]}, tenant_id=run["tenant_id"],
        run_after=_now() + timedelta(seconds=delay_s), idempotency_key=f"wftick:{run['id']}:{key}", max_attempts=3,
    )


class Stop(Exception):
    """The run ends now (failed), with this error and optional explanation answer."""

    def __init__(self, error: str, answer: str | None = None):
        super().__init__(error)
        self.error, self.answer = error, answer


class Tick:
    def __init__(self, st: Any, kap: KapClient, settings: Settings, run: dict[str, Any], steps: dict[str, Step]):
        self.st, self.kap, self.settings, self.run, self.steps = st, kap, settings, run, steps
        self.cfg: Mapping[str, Any] = run["cfg"]
        self.nodes = {n[0]: n for n in graph.nodes_of(self.cfg)}
        self.entry_id = next((n[0] for n in self.nodes.values() if n[1] == "entry"), "")
        self.run_fields: dict[str, Any] = {}
        self._model: Any = None
        self.deferred: set[str] = set()

    # ------------------------------------------------------------ helpers
    @property
    def tid(self) -> str:
        return self.run["tenant_id"]

    def p(self, nid: str) -> dict[str, str]:
        return graph.props(self.cfg, nid)

    async def model(self) -> Any:
        if self._model is None:
            self._model = SERVICES.model(self.settings)
        return self._model

    async def close(self) -> None:
        if self._model is not None:
            await self._model.aclose()

    async def set_step(self, nid: str, **fields: Any) -> None:
        s = self.steps[nid]
        for k, v in fields.items():
            setattr(s, k, v)
        await self.st.update_step(self.run["id"], nid, fields)

    def next_seq(self) -> int:
        return max([s.seq for s in self.steps.values() if s.seq is not None] or [-1]) + 1

    async def finish_step(self, nid: str, output: str | None, note: str | None = None, **extra: Any) -> None:
        await self.set_step(nid, status="done", output=output, note=note, finished_at=_now(), **extra)

    async def fail_step(self, nid: str, reason: str, policy: str | None = None) -> None:
        """A step failed: continue, follow its failure path, or stop the run."""
        s = self.steps[nid]
        policy = policy or self.p(nid).get("onErr") or "Use the failure path"
        if policy == "Continue without this result":
            await self.finish_step(nid, None, f"Failed; continued without this result: {reason}")
            return
        await self.set_step(nid, status="failed", note=reason, finished_at=_now())
        if policy in ("Stop the workflow", "Stop and return an explanation") or not graph.failure_handled(self.cfg, nid):
            raise Stop(f"{s.label}: {reason}", self.explanation(f"{s.label}: {reason}"))

    def explanation(self, error: str) -> str | None:
        if (self.cfg.get("onErr") or "") == "Stop and return an explanation":
            return f"The request could not be completed. {error}"
        return None

    # ------------------------------------------------------------ 1. running agent steps
    async def follow(self, nid: str) -> None:
        s = self.steps[nid]
        try:
            job = await self.kap.get_agent_job(self.tid, self.run["agents"].get(self.nodes[nid][2]) or "", s.job_id)
        except KapError as exc:
            if exc.status in (403, 404):
                await self.fail_step(nid, f"KAP no longer returns job {s.job_id}.")
                return
            raise
        name = self.nodes[nid][2]
        await self.st.mirror_job(self.tid, {"id": self.run["agents"].get(name), "name": name}, job)
        status = job.get("status")
        if status == "succeeded":
            text = answer_text(job.get("result"))
            if text:
                await self.finish_step(nid, text, f"KAP job {str(s.job_id)[:8]} answered")
            else:
                await self.retry_or_fail(nid, "The agent returned an empty answer.")
            return
        if status in TERMINAL:
            await self.retry_or_fail(nid, f"The KAP job {status}: {job.get('error') or 'no reason given'}")
            return
        limit = graph.seconds(self.p(nid).get("timeout"), 60)
        started = _ts(s.started_at)
        if started and _now() - started > timedelta(seconds=limit):
            await self.retry_or_fail(nid, f"No answer within the step's time limit ({self.p(nid).get('timeout') or '60 s'}).")

    async def retry_or_fail(self, nid: str, reason: str) -> None:
        s = self.steps[nid]
        retries = graph.int_or(self.p(nid).get("retries"), 1)
        if s.attempts <= retries:
            await self.submit(nid, s.input or self.run["input"], f"Retry {s.attempts} after: {reason}")
        else:
            await self.fail_step(nid, reason)

    # ------------------------------------------------------------ 2. human steps
    async def check_waiting(self, nid: str) -> None:
        s = self.steps[nid]
        p = self.p(nid)
        limit = graph.seconds(p.get("tl"), 86400)
        started = _ts(s.started_at)
        if started and _now() - started > timedelta(seconds=limit) and p.get("late") == "Reject the result":
            await self.fail_step(nid, f"Not reviewed within {p.get('tl') or '24 h'}; rejected.", "Use the failure path")

    # ------------------------------------------------------------ 3. starting steps
    async def submit(self, nid: str, prompt: str, note: str | None = None) -> None:
        name = self.nodes[nid][2]
        max_jobs = graph.int_or((self.cfg.get("limits") or {}).get("steps"), 12)
        if self.run["agent_jobs"] >= max_jobs:
            await self.fail_step(nid, f"The workflow reached its limit of {max_jobs} agent steps.", "Stop the workflow")
            return
        agent_id = self.run["agents"].get(name)
        url, headers = callback(self.settings, self.tid, agent_id)
        try:
            job = await self.kap.submit_agent_job(self.tid, agent_id, user_prompt=prompt, callback_url=url, callback_headers=headers)
        except KapError as exc:
            if exc.status == 429 or exc.status >= 500:
                await self.set_step(nid, status="pending", note=f"KAP is busy; trying again ({exc.status}).")
                self.deferred.add(nid)  # not again in this tick
                return
            await self.fail_step(nid, f"KAP refused the job: {exc}")
            return
        self.run["agent_jobs"] += 1
        self.run_fields["agent_jobs"] = self.run["agent_jobs"]
        seq = self.steps[nid].seq if self.steps[nid].seq is not None else self.next_seq()
        await self.set_step(nid, status="running", job_id=job["id"], input=prompt, attempts=self.steps[nid].attempts + 1, started_at=_now(), seq=seq, note=note)
        await self.st.mirror_job(self.tid, {"id": agent_id, "name": name}, job)

    async def evidence(self) -> str:
        """Results of the finished agent steps, and what their tools returned (from the traces)."""
        parts = []
        for s in self.steps.values():
            if s.kind in ("agent", "aggregate") and s.status == "done" and s.output:
                parts.append(f"[{s.label}] {s.output}")
                if s.job_id:
                    try:
                        ev = evidence_of(await self.st.trace(s.job_id))
                    except Exception:  # traces are a bonus here
                        ev = ""
                    if ev:
                        parts.append(ev)
        return "\n\n".join(parts)

    async def start(self, nid: str) -> None:
        s = self.steps[nid]
        n = self.nodes[nid]
        p = self.p(nid)
        ins = graph.inputs_of(self.cfg, self.steps, nid)
        request = self.run["input"]
        if s.seq is None:
            await self.set_step(nid, seq=self.next_seq(), started_at=_now())
        kind = n[1]
        if kind == "output":
            answer = graph.combine(ins, self.entry_id)
            if answer:
                await self.finish_step(nid, answer)
            else:
                await self.fail_step(nid, "No step produced an answer.", "Stop the workflow")
        elif kind == "agent":
            await self.submit(nid, graph.agent_prompt(request, ins, self.entry_id))
        elif kind == "aggregate":
            if p.get("merge") == "First successful result":
                first = next(((label, text) for src, label, text in ins if src != self.entry_id), None)
                if first:
                    await self.finish_step(nid, first[1], f"Took the first result ({first[0]})")
                else:
                    await self.fail_step(nid, "No earlier step produced a result.")
            else:
                await self.submit(nid, graph.aggregate_prompt(request, ins, self.entry_id))
        elif kind == "router":
            await self.route(nid, request, ins)
        elif kind == "review" and p.get("rt") == "human":
            await self.set_step(nid, status="waiting", input=graph.combine(ins, self.entry_id), note="Waiting for a reviewer")
        elif kind == "review":
            await self.validate(nid, request, ins)
        elif kind == "fallback":
            await self.fallback(nid, request, ins)
        elif kind == "tool":
            await self.fail_step(nid, "Tool steps cannot run on their own; KAP runs tools inside agents.", "Stop the workflow")
        else:
            await self.fail_step(nid, f"Unknown step type {kind}.", "Stop the workflow")

    async def route(self, nid: str, request: str, ins: list[tuple[str, str, str]]) -> None:
        p = self.p(nid)
        options = graph.route_options(self.cfg, nid)
        by = p.get("by") or "Rule-based conditions"
        passthrough = graph.combine(ins, self.entry_id) or request
        if by == "Rule-based conditions":
            target, why = graph.route_by_rule(self.cfg, nid, request)
        else:
            try:
                target, why = await decide.route_by_model(await self.model(), request, options)
                why = f"Decided by the model: {why}" if why else "Decided by the model"
            except Exception as exc:
                target = graph.default_route(self.cfg, nid)
                why = f"The model could not decide ({exc}); took the default path."
        if target is None:
            await self.fail_step(nid, "The router has no path to take.")
            return
        label = graph.label_of(self.nodes[target]) if target in self.nodes else target
        await self.finish_step(nid, passthrough, f"Routed to {label}. {why}", chosen=[target])

    async def validate(self, nid: str, request: str, ins: list[tuple[str, str, str]]) -> None:
        p = self.p(nid)
        check = p.get("check") or "Numbers match the source data"
        answer = graph.combine(ins, self.entry_id)
        policy = "Stop and return an explanation" if p.get("onFail") == "Stop and return an explanation" else "Use the failure path"
        if not answer:
            await self.fail_step(nid, "Nothing to check: the earlier steps produced no answer.", policy)
            return
        try:
            ok, why = await decide.check_answer(await self.model(), check, request, answer, await self.evidence())
        except Exception as exc:
            await self.fail_step(nid, f"The check could not run: {exc}", policy)
            return
        if ok:
            await self.finish_step(nid, answer, f"{check}: passed. {why}".strip())
        else:
            await self.fail_step(nid, f"{check}: failed. {why}".strip(), policy)

    async def fallback(self, nid: str, request: str, ins: list[tuple[str, str, str]]) -> None:
        p = self.p(nid)
        action = p.get("action") or "Return a template answer"
        failed = [s for s in self.steps.values() if s.status == "failed"]
        reason = "; ".join(f"{s.label}: {s.note}" for s in failed) or "an earlier step failed"
        self.run_fields["fallback_used"] = True
        if action == "Return a template answer":
            await self.finish_step(nid, (p.get("text") or "").strip() or TEMPLATE, f"Template answer returned because {reason}")
        elif action == "Hand over to a human reviewer":
            await self.set_step(nid, status="waiting", input=graph.combine(ins, self.entry_id), note=f"Waiting for a person to answer, because {reason}")
        else:
            note = "Stopped" + (" (owner notification is not available yet)" if action == "Stop and notify the owner" else "")
            await self.finish_step(nid, None, note)
            raise Stop(f"Stopped by {self.steps[nid].label}: {reason}", f"The request could not be completed: {reason}")

    # ------------------------------------------------------------ the tick
    async def go(self) -> dict[str, Any]:
        run = self.run
        if run["status"] == "queued":
            self.run_fields.update(status="running", started_at=_now())
            run["started_at"] = self.run_fields["started_at"]
        e = self.steps.get(self.entry_id)
        if e and e.status == "pending":
            await self.set_step(self.entry_id, status="done", output=run["input"], seq=0, started_at=_now(), finished_at=_now())
        try:
            for nid, s in list(self.steps.items()):
                if s.status == "running" and s.job_id:
                    await self.follow(nid)
                elif s.status == "waiting":
                    await self.check_waiting(nid)
            self.check_duration()
            for _ in range(len(self.steps) * 2 + 2):
                ready, skipped = graph.advance(self.cfg, self.steps)
                ready = [nid for nid in ready if nid not in self.deferred]
                for nid in skipped:
                    await self.set_step(nid, status="skipped")
                for nid in ready:
                    await self.start(nid)
                if not ready and not skipped:
                    break
        except Stop as stop:
            return await self.end("failed", answer=stop.answer, error=stop.error)

        out = next((s for s in self.steps.values() if s.kind == "output"), None)
        if out and out.status == "done":
            return await self.end("succeeded", answer=out.output or "")
        if any(s.status == "running" for s in self.steps.values()):
            return await self.keep("running", self.settings.eval_poll_s)
        if any(s.status == "waiting" for s in self.steps.values()):
            return await self.keep("waiting", WAITING_POLL_S)
        if any(s.status == "pending" and s.note and s.note.startswith("KAP is busy") for s in self.steps.values()):
            return await self.keep("running", self.settings.eval_poll_s)
        failed = [s for s in self.steps.values() if s.status == "failed"]
        error = "; ".join(f"{s.label}: {s.note}" for s in failed) or "The workflow could not reach its output (check the connections)."
        return await self.end("failed", answer=self.explanation(error), error=error)

    def check_duration(self) -> None:
        """limits.dur covers the automatic steps; it is not applied while a person reviews."""
        human = any(graph.is_human(self.cfg, n) or (n[1] == "fallback" and self.p(n[0]).get("action") == "Hand over to a human reviewer") for n in self.nodes.values())
        if human:
            return
        limit = graph.seconds((self.cfg.get("limits") or {}).get("dur"), 600)
        started = _ts(self.run.get("started_at"))
        if started and _now() - started > timedelta(seconds=limit):
            dur = (self.cfg.get("limits") or {}).get("dur") or "600 s"
            raise Stop(f"The workflow ran longer than its time limit ({dur}).", self.explanation(f"The workflow ran longer than its time limit ({dur})."))

    async def end(self, status: str, answer: str | None, error: str | None = None) -> dict[str, Any]:
        for nid, s in self.steps.items():
            if s.status == "pending":
                await self.set_step(nid, status="skipped")
            elif s.status in ("running", "waiting"):
                await self.set_step(nid, status="skipped", note=f"{s.note} · stopped: the workflow ended" if s.note else "Stopped: the workflow ended")
        self.run_fields.update(status=status, answer=answer, error=error, finished_at=_now())
        await self.st.update_run(self.run["id"], {**self.run_fields, "polls": int(self.run.get("polls") or 0) + 1})
        return {"status": status}

    async def keep(self, status: str, delay: float) -> dict[str, Any]:
        polls = int(self.run.get("polls") or 0) + 1
        self.run_fields.update(status=status, polls=polls)
        await self.st.update_run(self.run["id"], self.run_fields)
        return {"status": status, "next_in_s": delay, "polls": polls}


async def tick(pool: Any, kap: KapClient, settings: Settings, run_id: str) -> dict[str, Any]:
    st = SERVICES.store(pool)
    run = await st.get_run(None, run_id)
    if run is None or run["status"] not in ACTIVE:
        return {"skipped": "run is not active"}
    if not await st.try_lease(run_id, settings.lease_s):
        return {"skipped": "another tick is running"}
    t = Tick(st, kap, settings, run, await st.steps(run_id))
    try:
        out = await t.go()
    finally:
        await t.close()
        await st.release(run_id)
    if out.get("status") in ("running", "waiting"):
        await enqueue_tick(pool, run, str(out["polls"]), out["next_in_s"])
    return out


async def handle_wf_tick(pool: Any, kap: KapClient, settings: Settings, job: queue.Job) -> dict[str, Any]:
    run_id = job.payload["run_id"]
    try:
        return await tick(pool, kap, settings, run_id)
    except Exception as exc:
        final = job.attempts >= job.max_attempts or (isinstance(exc, KapError) and exc.status < 500 and exc.status != 429)
        if final:
            log.exception("workflow run %s failed", run_id)
            st = SERVICES.store(pool)
            await st.update_run(run_id, {"status": "failed", "error": f"{type(exc).__name__}: {exc}"[:2000], "finished_at": _now()})
            await st.release(run_id)
        raise
