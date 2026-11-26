"""Evaluation runs on the job queue.

  eval_watch  {run_id}  one tick of a run (a new delayed job each time, no sleeping):
                        1. follow the submitted KAP jobs; a finished one gets its answer
                        2. score answered questions: fact checks, then the judge with the
                           trace's tool results as evidence (waits up to eval_trace_wait_s
                           for the trace to be linked)
                        3. submit more questions, keeping `parallel` jobs in flight
                        4. update the summary; finish when every question is scored

Each question is a real KAP agent job submitted with the platform's callback,
so it is mirrored in kap_job and its trace shows in Monitoring like any other.
Run status: queued → running → succeeded (questions may still have failed) |
failed (the run itself broke) | cancelled.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping

from .. import queue
from ..config import Settings
from ..kap.client import KapClient, KapError
from ..traces.build import Trace
from . import score
from .judge import Judge
from .store import DONE, EvalStore

log = logging.getLogger(__name__)

TERMINAL = ("succeeded", "failed", "cancelled")
EVIDENCE_PER_TOOL = 2500


class Services:
    def __init__(self, store_factory: Callable[[Any], Any] = EvalStore, judge_factory: Callable[[Settings], Any] = Judge):
        self.store_factory, self.judge_factory = store_factory, judge_factory

    def store(self, pool: Any) -> Any:
        return self.store_factory(pool)

    def judge(self, settings: Settings) -> Any:
        return self.judge_factory(settings)


SERVICES = Services()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _ts(v: Any) -> datetime | None:
    if v is None or isinstance(v, datetime):
        return v
    return datetime.fromisoformat(str(v).replace("Z", "+00:00"))


def _ms(a: Any, b: Any) -> int | None:
    a, b = _ts(a), _ts(b)
    return max(0, int((b - a).total_seconds() * 1000)) if a and b else None


async def enqueue_tick(pool: Any, run: Mapping[str, Any], polls: int, delay_s: float = 0) -> None:
    await queue.enqueue(
        pool, "eval_watch", {"run_id": run["id"]}, tenant_id=run["tenant_id"],
        run_after=_now() + timedelta(seconds=delay_s), idempotency_key=f"evalwatch:{run['id']}:{polls}", max_attempts=3,
    )


def _text(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    return json.dumps(v, ensure_ascii=False, default=str)


def evidence_of(trace: Trace | None) -> str:
    """What the agent's tools returned, for the judge: one block per tool call."""
    if trace is None:
        return ""
    blocks = []
    for s in trace.steps:
        if s.kind != "tool":
            continue
        out = _text(s.output)
        if len(out) > EVIDENCE_PER_TOOL:
            out = out[:EVIDENCE_PER_TOOL] + " …"
        blocks.append(f"[{s.name}] {_text(s.input)}\n→ {out if out else '(no result)'}")
    return "\n\n".join(blocks)


def metrics_of(job: Mapping[str, Any], trace: Trace | None) -> dict[str, Any]:
    m: dict[str, Any] = {"latency_ms": _ms(job.get("running_at"), job.get("finished_at")), "queue_ms": _ms(job.get("created_at"), job.get("running_at"))}
    if trace is not None and trace.totals.get("model_calls"):
        t = trace.totals
        m.update({
            "model_calls": t.get("model_calls"),
            "tool_calls": t.get("tool_calls"),
            "tool_errors": sum(1 for s in trace.steps if s.kind == "tool" and s.status == "error"),
            "tokens": (t.get("prompt_tokens") or 0) + (t.get("completion_tokens") or 0),
            "tools": [s.name for s in trace.steps if s.kind == "tool"],
        })
    return m


def callback(settings: Settings, tenant_id: str, agent_id: str) -> tuple[str, dict[str, str] | None]:
    url = f"{settings.public_base_url.rstrip('/')}/hooks/kap/t/{tenant_id}/agents/{agent_id}/jobs"
    return url, ({"X-Platform-Hook-Token": settings.hook_token} if settings.hook_token else None)


async def _follow(st: Any, kap: KapClient, settings: Settings, run: Mapping[str, Any], r: dict[str, Any], agent: dict[str, Any]) -> None:
    """A submitted question: read its KAP job; store the answer once it is finished."""
    try:
        job = await kap.get_agent_job(run["tenant_id"], run["agent_id"], r["job_id"])
    except KapError as exc:
        if exc.status in (404, 403):
            await st.update_result(r["id"], {"status": "error", "error": f"KAP no longer returns job {r['job_id']}: {exc}", "passed": False, "scored_at": _now()})
            return
        raise
    await st.mirror_job(run["tenant_id"], agent, job)
    status = job.get("status")
    if status in TERMINAL:
        await st.update_result(r["id"], {
            "status": "answered", "job_status": status, "answer": score.answer_text(job.get("result")) or None,
            "error": job.get("error"), "answered_at": _now(), "metrics": metrics_of(job, None),
        })
        return
    if r.get("job_status") != status:
        await st.update_result(r["id"], {"job_status": status})
    if r.get("submitted_at") and _now() - _ts(r["submitted_at"]) > timedelta(seconds=settings.eval_job_wait_s):
        await st.update_result(r["id"], {
            "status": "error", "job_status": status, "passed": False, "scored_at": _now(),
            "error": f"The KAP job was still {status} after {settings.eval_job_wait_s // 60} minutes.",
            "reason": "Timed out waiting for the agent.",
        })


async def _score(st: Any, judge: Any, settings: Settings, run: Mapping[str, Any], r: dict[str, Any], options: Mapping[str, Any]) -> bool:
    """An answered question: checks and judge. False when it should wait for its trace."""
    trace_status = await st.trace_status(r["job_id"])
    waited = _now() - (_ts(r.get("answered_at")) or _now())
    # the judge needs the tools' results; a failed job fails anyway and is scored at once
    if trace_status == "pending" and r.get("job_status") == "succeeded" and waited < timedelta(seconds=settings.eval_trace_wait_s):
        return False
    trace = await st.trace(r["job_id"]) if trace_status in ("linked", "ambiguous", "pending") else None
    answer = r.get("answer") or ""
    if not answer and trace is not None and trace.answer:
        answer = trace.answer
    checks = score.run_checks(answer, r.get("must_include") or [], r.get("must_not_include") or [])
    judged = None
    if options.get("judge") and r.get("job_status") == "succeeded" and answer.strip():
        judged = await judge.grade(r["question"], r.get("expected") or "", r.get("must_include") or [], answer, evidence_of(trace) or None)
    passed, reason = score.decide(r.get("job_status"), answer, checks, judged, options)
    if r.get("job_status") != "succeeded" and r.get("error"):
        reason = f"{reason} {r['error']}"[:2000]
    metrics = dict(r.get("metrics") or {})  # latency and queue time, from the job
    metrics.update({k: v for k, v in metrics_of({}, trace).items() if v is not None})  # tokens and tools, from the trace
    await st.update_result(r["id"], {
        "status": "scored", "answer": answer or None, "checks": checks, "judge": judged, "passed": passed,
        "reason": reason or None, "metrics": metrics, "scored_at": _now(),
    })
    return True


async def _submit(st: Any, kap: KapClient, settings: Settings, run: Mapping[str, Any], r: dict[str, Any], agent: dict[str, Any]) -> bool:
    """Send one question to the agent. False when KAP is busy (try again next tick)."""
    url, headers = callback(settings, run["tenant_id"], run["agent_id"])
    try:
        job = await kap.submit_agent_job(run["tenant_id"], run["agent_id"], user_prompt=r["question"], callback_url=url, callback_headers=headers)
    except KapError as exc:
        if exc.status == 429 or exc.status >= 500:
            return False
        await st.update_result(r["id"], {"status": "error", "error": str(exc), "passed": False, "reason": "KAP refused the job.", "scored_at": _now()})
        return True
    await st.update_result(r["id"], {"status": "submitted", "job_id": job["id"], "job_status": job.get("status"), "submitted_at": _now()})
    await st.mirror_job(run["tenant_id"], agent, job)
    return True


async def tick(pool: Any, kap: KapClient, settings: Settings, run_id: str) -> dict[str, Any]:
    st = SERVICES.store(pool)
    run = await st.get_run(None, run_id)
    if run is None or run["status"] not in ("queued", "running"):
        return {"skipped": "run is not active"}
    if run["status"] == "queued":
        await st.update_run(run["id"], {"status": "running", "started_at": _now()})
    options = score.options_with_defaults(run.get("options"))
    agent = {"id": run["agent_id"], "name": run.get("agent_name")}

    results = await st.results(run["id"])
    for r in results:
        if r["status"] == "submitted":
            await _follow(st, kap, settings, run, r, agent)

    results = await st.results(run["id"])
    to_score = [r for r in results if r["status"] == "answered"]
    if to_score:
        judge = SERVICES.judge(settings) if options.get("judge") else None
        try:
            for r in to_score:
                await _score(st, judge, settings, run, r, options)
        finally:
            if judge is not None:
                await judge.aclose()

    results = await st.results(run["id"])
    parallel = max(1, min(int(options.get("parallel") or 1), settings.eval_max_parallel))
    in_flight = sum(1 for r in results if r["status"] == "submitted")
    for r in results:
        if in_flight >= parallel:
            break
        if r["status"] == "pending":
            if not await _submit(st, kap, settings, run, r, agent):
                break
            in_flight += 1

    results = await st.results(run["id"])
    summary = score.summarize(results)
    polls = int(run.get("polls") or 0) + 1
    latest = await st.get_run(None, run["id"])
    if latest is None or latest["status"] == "cancelled":
        return {"skipped": "cancelled"}
    if all(r["status"] in DONE for r in results):
        await st.update_run(run["id"], {"status": "succeeded", "summary": summary, "polls": polls, "finished_at": _now()})
        return {"status": "succeeded", "summary": summary}
    await st.update_run(run["id"], {"summary": summary, "polls": polls})
    await enqueue_tick(pool, run, polls, settings.eval_poll_s)
    return {"status": "running", "done": summary["done"], "total": summary["total"]}


async def handle_eval_watch(pool: Any, kap: KapClient, settings: Settings, job: queue.Job) -> dict[str, Any]:
    run_id = job.payload["run_id"]
    try:
        return await tick(pool, kap, settings, run_id)
    except Exception as exc:
        final = job.attempts >= job.max_attempts or (isinstance(exc, KapError) and exc.status < 500 and exc.status != 429)
        if final:
            # the queue gives up on this tick: say so on the run instead of leaving it "running"
            log.exception("evaluation run %s failed", run_id)
            st = SERVICES.store(pool)
            await st.update_run(run_id, {"status": "failed", "error": f"{type(exc).__name__}: {exc}"[:2000], "finished_at": _now()})
        raise
