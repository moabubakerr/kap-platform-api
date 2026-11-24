"""Shared fixtures: a realistic KAP planner run, shaped like the one captured on
the test stand (24 Nov 2026): find_indicator → get_indicator_value →
actual_vs_target → answer, four non-streamed model calls."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

PROMPT = "Local section: use only collection_name='sandbox_indicators'. Report actual value and target. (probe reference: probe-104658)"
SYSTEM = "Answer only from the configured data tools and explicitly assigned collections."
T0 = datetime(2026, 11, 24, 10, 46, 59, 18000, tzinfo=timezone.utc)

TOOLS = [{"type": "function", "function": {"name": n}} for n in ("find_indicator", "get_indicator_value", "actual_vs_target")]


def _tc(cid: str, name: str, args: str) -> dict:
    return {"id": cid, "type": "function", "function": {"name": name, "arguments": args}}


def _reply_tool(cid: str, name: str, args: str, p: int, c: int) -> dict:
    return {
        "choices": [{"message": {"role": "assistant", "content": None, "tool_calls": [_tc(cid, name, args)]}}],
        "usage": {"prompt_tokens": p, "completion_tokens": c, "total_tokens": p + c},
    }


IND = '{"indicator_id": "5456bc3f", "frequency": "A", "period_start": "2019-01-01", "dimension": ""}'
FIND_RESULT = '[{"indicator_id": "5456bc3f", "name_en": "Non-Hydrocarbon Exports (share of total exports)"}]'
VALUE_RESULT = '{"value": "32.04", "unit": "%", "target_value": "50"}'
AVT_RESULT = '{"actual": {"value": "32.04"}, "target": {"value": "50"}}'
ANSWER = "The actual value for Non-Hydrocarbon Exports in 2019 was 32.04%, with a target value of 50%."


@pytest.fixture
def planner_run():
    """(job dict, list of model-call dicts) for one KAP planner job."""
    base = [{"role": "system", "content": SYSTEM}, {"role": "user", "content": PROMPT}]
    a1 = {"role": "assistant", "content": None, "tool_calls": [_tc("t1", "find_indicator", '{"text": "cms.8f22"}')]}
    r1 = {"role": "tool", "tool_call_id": "t1", "content": FIND_RESULT}
    a2 = {"role": "assistant", "content": None, "tool_calls": [_tc("t2", "get_indicator_value", IND)]}
    r2 = {"role": "tool", "tool_call_id": "t2", "content": VALUE_RESULT}
    a3 = {"role": "assistant", "content": None, "tool_calls": [_tc("t3", "actual_vs_target", IND)]}
    r3 = {"role": "tool", "tool_call_id": "t3", "content": AVT_RESULT}
    timings = [(0.0, 1.232), (1.5, 3.484), (5.2, 2.028), (7.4, 2.624)]
    requests = [base, base + [a1, r1], base + [a1, r1, a2, r2], base + [a1, r1, a2, r2, a3, r3]]
    responses = [
        _reply_tool("t1", "find_indicator", '{"text": "cms.8f22"}', 8891, 63),
        _reply_tool("t2", "get_indicator_value", IND, 9101, 162),
        _reply_tool("t3", "actual_vs_target", IND, 9484, 81),
        {"choices": [{"message": {"role": "assistant", "content": ANSWER}}], "usage": {"prompt_tokens": 9922, "completion_tokens": 91, "total_tokens": 10013}},
    ]
    calls = []
    for i, ((start, dur), msgs, resp) in enumerate(zip(timings, requests, responses)):
        s = T0 + timedelta(seconds=start)
        calls.append(
            {
                "id": f"call-{i + 1}",
                "started_at": s,
                "finished_at": s + timedelta(seconds=dur),
                "duration_ms": int(dur * 1000),
                "model": "qwen72b",
                "status": 200,
                "request": {"model": "qwen72b", "stream": False, "messages": msgs, "tools": TOOLS},
                "response": resp,
            }
        )
    job = {
        "id": "ced68c80-d0a1-4e16-8dce-f764fb9bd6a2",
        "tenant_id": "71b6c98a-7f7a-4a8b-ab31-132c85bdc88b",
        "user_prompt": PROMPT,
        "created_at": T0 - timedelta(milliseconds=220),
        "running_at": T0,
        "finished_at": T0 + timedelta(seconds=10.229),
    }
    return job, calls
