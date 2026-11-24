from datetime import timedelta

from kap_platform.traces.build import Call, build_trace, tool_source
from kap_platform.traces.correlate import CallKey, JobWindow, link_job
from kap_platform.traces.extract import parse_reply, parse_sse, summarize_request, tool_results

from conftest import ANSWER, AVT_RESULT, FIND_RESULT, PROMPT, SYSTEM, VALUE_RESULT


# ---------------------------------------------------------------- extract
def test_summarize_request_takes_first_user_message_and_hashes_prompt(planner_run):
    _, calls = planner_run
    first, last = summarize_request(calls[0]["request"]), summarize_request(calls[3]["request"])
    assert first.first_user_text == PROMPT and first.model == "qwen72b" and first.stream is False
    assert first.n_messages == 2 and last.n_messages == 8
    assert first.prompt_hash == last.prompt_hash  # same system + first user message across the run


def test_summarize_request_handles_content_parts_and_garbage():
    s = summarize_request({"messages": [{"role": "user", "content": [{"type": "text", "text": "a"}, {"type": "text", "text": "b"}]}]})
    assert s.first_user_text == "ab"
    assert summarize_request("not json").n_messages == 0


def test_parse_reply_tool_call_and_text(planner_run):
    _, calls = planner_run
    r1 = parse_reply(calls[0]["response"])
    assert [t.name for t in r1.tool_calls] == ["find_indicator"] and r1.tool_calls[0].id == "t1"
    assert r1.usage["prompt_tokens"] == 8891
    assert parse_reply(calls[3]["response"]).text == ANSWER


def test_parse_sse_rebuilds_text_tools_and_usage():
    raw = "\n\n".join(
        [
            'data: {"choices":[{"delta":{"content":"Exports were "}}]}',
            'data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"c9","function":{"name":"find_","arguments":"{\\"t\\""}}]}}]}',
            'data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"name":"indicator","arguments":": 1}"}}]}}]}',
            'data: {"choices":[{"delta":{"content":"32.04%."}}]}',
            'data: {"choices":[],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7}}',
            "data: [DONE]",
        ]
    )
    r = parse_sse(raw)
    assert r.text == "Exports were 32.04%."
    assert r.tool_calls[0].name == "find_indicator" and r.tool_calls[0].arguments == '{"t": 1}' and r.tool_calls[0].id == "c9"
    assert r.usage["total_tokens"] == 7
    assert parse_reply({"sse": raw}).text == "Exports were 32.04%."


def test_tool_results_by_call_id(planner_run):
    _, calls = planner_run
    assert tool_results(calls[3]["request"]) == {"t1": FIND_RESULT, "t2": VALUE_RESULT, "t3": AVT_RESULT}


# ---------------------------------------------------------------- build
def _calls(raw):
    return [Call(**c) for c in raw]


def test_build_trace_reconstructs_the_run(planner_run):
    job, raw = planner_run
    t = build_trace(job["id"], job["created_at"], job["running_at"], job["finished_at"], _calls(raw))
    kinds = [(s.kind, s.name) for s in t.steps]
    assert kinds == [
        ("queue", "queued"),
        ("model", "plan"),
        ("tool", "find_indicator"),
        ("model", "plan"),
        ("tool", "get_indicator_value"),
        ("model", "plan"),
        ("tool", "actual_vs_target"),
        ("model", "answer"),
    ]
    tools = [s for s in t.steps if s.kind == "tool"]
    assert tools[1].input["period_start"] == "2019-01-01"
    assert tools[1].output == {"value": "32.04", "unit": "%", "target_value": "50"}
    assert all(s.status == "ok" and s.source == "local" for s in tools)
    # tool time = gap between the model call that asked and the next one
    assert tools[0].duration_ms == int((raw[1]["started_at"] - raw[0]["finished_at"]).total_seconds() * 1000)
    assert t.answer == ANSWER
    assert t.totals["model_calls"] == 4 and t.totals["tool_calls"] == 3
    assert t.totals["prompt_tokens"] == 8891 + 9101 + 9484 + 9922
    assert t.totals["queue_ms"] == 220


def test_build_trace_marks_tool_without_result_and_serialises(planner_run):
    job, raw = planner_run
    t = build_trace(job["id"], None, job["running_at"], job["finished_at"], _calls(raw[:1]))  # job died after the first call
    assert [s.kind for s in t.steps] == ["model", "tool"]
    assert t.steps[1].status == "no_result" and t.steps[1].finished_at == job["finished_at"]
    d = t.as_dict()
    assert isinstance(d["steps"][0]["started_at"], str)


def test_tool_source_colour_coding():
    assert tool_source("find_indicator") == "local"
    assert tool_source("hybrid_search") == "documents"
    assert tool_source("oxford_economic_data") == "oxford"


# ---------------------------------------------------------------- correlate
def _window(job, **kw):
    return JobWindow(**{**{k: job[k] for k in ("id", "tenant_id", "user_prompt", "running_at", "finished_at")}, **kw})


def _keys(raw, tenant):
    return [CallKey(c["id"], tenant, c["started_at"], c["request"]["messages"][1]["content"]) for c in raw]


def test_link_job_links_calls_with_same_prompt_inside_window(planner_run):
    job, raw = planner_run
    link = link_job(_window(job), _keys(raw, job["tenant_id"]), [])
    assert link.status == "linked" and link.call_ids == ["call-1", "call-2", "call-3", "call-4"]


def test_link_job_ignores_other_prompts_tenants_and_times(planner_run):
    job, raw = planner_run
    keys = _keys(raw, job["tenant_id"])
    stray = [
        keys[0].__class__("other-prompt", job["tenant_id"], keys[0].started_at, "different question"),
        keys[0].__class__("other-tenant", "a81e92d4-9541-433a-b295-c869b5dbaf17", keys[0].started_at, PROMPT),
        keys[0].__class__("too-late", job["tenant_id"], job["finished_at"] + timedelta(seconds=30), PROMPT),
    ]
    link = link_job(_window(job), keys + stray, [])
    assert link.call_ids == ["call-1", "call-2", "call-3", "call-4"]


def test_link_job_is_ambiguous_when_identical_prompts_overlap(planner_run):
    job, raw = planner_run
    twin = _window(job, id="twin", running_at=job["running_at"] + timedelta(seconds=3), finished_at=job["finished_at"] + timedelta(seconds=3))
    assert link_job(_window(job), _keys(raw, job["tenant_id"]), [twin]).status == "ambiguous"
    later = _window(job, id="later", running_at=job["finished_at"] + timedelta(minutes=5), finished_at=job["finished_at"] + timedelta(minutes=6))
    assert link_job(_window(job), _keys(raw, job["tenant_id"]), [later]).status == "linked"


def test_link_job_none_without_prompt_or_window(planner_run):
    job, raw = planner_run
    assert link_job(_window(job, user_prompt=None), _keys(raw, job["tenant_id"]), []).status == "none"
    assert link_job(_window(job, finished_at=None), _keys(raw, job["tenant_id"]), []).status == "none"
    assert SYSTEM  # fixture sanity
