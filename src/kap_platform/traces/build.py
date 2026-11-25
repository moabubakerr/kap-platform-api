"""Turn a KAP job and its linked model calls into trace steps for the console.

Steps, in time order:
  queue   — created_at → running_at (waiting for a worker)
  model   — one per model call: "plan" when the reply calls tools, "answer" otherwise
  tool    — one per tool call in a reply: from the end of that model call to the
            start of the next one (KAP runs tools in between), with arguments
            and the result read from the next request's `tool` messages
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any

import re

from .extract import parse_reply, tool_results, try_json

# KAP hands tool errors back to the model as ordinary tool text, e.g.
# "1 validation error: ... Field required" (hybrid_search without entity_id,
# seen on the stand 25 Nov 2026). Those are failures, not data.
_TOOL_ERROR = re.compile(r"^\s*(\d+ validation errors?\b|error\b|traceback\b|exception\b|\w+(error|exception)\b:)", re.IGNORECASE)


def tool_failed(result: Any) -> bool:
    """Whether a tool result is an error message rather than data."""
    if isinstance(result, str):
        return bool(_TOOL_ERROR.match(result))
    if isinstance(result, dict):
        return set(result) <= {"error", "detail", "message", "type", "code"} and ("error" in result or "detail" in result)
    return False


@dataclass
class Call:
    id: str
    started_at: datetime
    finished_at: datetime
    duration_ms: int
    model: str | None
    status: int | None
    request: Any
    response: Any
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None


@dataclass
class Step:
    seq: int
    kind: str  # queue | model | tool
    name: str
    started_at: datetime | None
    finished_at: datetime | None
    duration_ms: int | None
    source: str = "model"  # model | local | documents | oxford | queue
    input: Any = None
    output: Any = None
    tokens: dict[str, int | None] | None = None
    status: str = "ok"
    model_call_id: str | None = None
    tool_call_id: str | None = None


@dataclass
class Trace:
    job_id: str
    steps: list[Step] = field(default_factory=list)
    answer: str | None = None
    totals: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        d = asdict(self)
        for s in d["steps"]:
            for k in ("started_at", "finished_at"):
                if isinstance(s[k], datetime):
                    s[k] = s[k].isoformat()
        return d


# Tool names → data-source colour coding used by the console.
DOCUMENT_TOOLS = {"hybrid_search"}
# KAP registers MCP tools under their own names (test stand: oxford_economic_data
# → EconomicData). Until the tool registry exists, these are listed here.
EXTERNAL_TOOLS = {"EconomicData", "EconomicAnalysis", "oxford_economic_data", "oxford_economic_analysis"}


def tool_source(name: str) -> str:
    if name in DOCUMENT_TOOLS:
        return "documents"
    if name in EXTERNAL_TOOLS:
        return "oxford"
    return "local"


def _ms(a: datetime | None, b: datetime | None) -> int | None:
    if a is None or b is None:
        return None
    return max(0, int((b - a).total_seconds() * 1000))


def build_trace(
    job_id: str,
    created_at: datetime | None,
    running_at: datetime | None,
    finished_at: datetime | None,
    calls: list[Call],
) -> Trace:
    trace = Trace(job_id=job_id)
    seq = 0
    if created_at and running_at:
        trace.steps.append(Step(seq, "queue", "queued", created_at, running_at, _ms(created_at, running_at), source="queue"))
        seq += 1

    calls = sorted(calls, key=lambda c: c.started_at)
    tokens_in = tokens_out = 0
    for i, call in enumerate(calls):
        reply = parse_reply(call.response)
        nxt = calls[i + 1] if i + 1 < len(calls) else None
        usage = reply.usage or {}
        p = call.prompt_tokens if call.prompt_tokens is not None else usage.get("prompt_tokens")
        c = call.completion_tokens if call.completion_tokens is not None else usage.get("completion_tokens")
        t = call.total_tokens if call.total_tokens is not None else usage.get("total_tokens")
        tokens_in += p or 0
        tokens_out += c or 0
        trace.steps.append(
            Step(
                seq,
                "model",
                "plan" if reply.tool_calls else "answer",
                call.started_at,
                call.finished_at,
                call.duration_ms,
                source="model",
                output=reply.text or None,
                tokens={"prompt": p, "completion": c, "total": t},
                status="ok" if (call.status or 200) < 400 else "error",
                model_call_id=call.id,
            )
        )
        seq += 1
        if reply.tool_calls:
            results = tool_results(nxt.request) if nxt else {}
            t_end = nxt.started_at if nxt else finished_at
            for tc in reply.tool_calls:
                has_result = tc.id in results if tc.id else False
                status = ("error" if tool_failed(results[tc.id]) else "ok") if has_result else "no_result"
                trace.steps.append(
                    Step(
                        seq,
                        "tool",
                        tc.name,
                        call.finished_at,
                        t_end,
                        _ms(call.finished_at, t_end),
                        source=tool_source(tc.name),
                        input=try_json(tc.arguments),
                        output=try_json(results[tc.id]) if has_result else None,
                        status=status,
                        tool_call_id=tc.id,
                    )
                )
                seq += 1
        elif reply.text:
            trace.answer = reply.text

    trace.totals = {
        "model_calls": len(calls),
        "tool_calls": sum(1 for s in trace.steps if s.kind == "tool"),
        "prompt_tokens": tokens_in,
        "completion_tokens": tokens_out,
        "queue_ms": _ms(created_at, running_at),
        "run_ms": _ms(running_at, finished_at),
        "model_ms": sum(c.duration_ms for c in calls),
    }
    return trace
