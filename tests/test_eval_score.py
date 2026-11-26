"""Fact checks, pass rule and run summary (evals/score.py)."""
import asyncio
import json

import httpx
import pytest

from kap_platform.config import Settings
from kap_platform.evals import score
from kap_platform.evals.judge import Judge, build_messages, parse_verdict


@pytest.mark.parametrize(
    "term,answer,ok",
    [
        ("2.62%", "Inflation was 2.62 % in April 2026.", True),
        ("2.62", "Inflation was 2.6% in April 2026.", False),  # less precise than the term
        ("2.6", "Inflation was 2.62%.", True),  # more precise than the term
        ("7,984", "It loaded 7984 observations.", True),
        ("7984", "It loaded 7,984 observations.", True),
        ("4.17", "Qatar ٤٫١٧٪ مقابل الكويت", True),  # Arabic-Indic digits
        ("50", "The target was 50%.", True),
        ("50", "The target was 150%.", False),
        ("-0.5", "Prices fell by 0.5% year on year.", True),
        ("Kuwait", "Qatar 4.17% vs Kuwait 2.06%", True),
        ("kuwait", "KUWAIT: 2.06%", True),
        ("Copper Harbour", "The project is in copper  harbour.", True),
        ("non-hydrocarbon", "Non hydrocarbon exports were 32.04%", True),
        ("rate", "inflation rates", False),  # whole words
        ("Q1 2026", "In Q1 2026 inflation averaged 2.98%.", True),
        ("Kuwait | الكويت", "الكويت 2.06%", True),
        ("Oman | Bahrain", "Qatar only", False),
    ],
)
def test_term_found(term, answer, ok):
    assert score.term_found(term, answer) is ok


def test_answer_text_from_job_results():
    assert score.answer_text("  plain  ") == "plain"
    assert score.answer_text('{"answer": "2.62%"}') == "2.62%"
    assert score.answer_text({"result": {"output": "nested"}}) == "nested"
    assert score.answer_text(None) == ""
    assert score.answer_text("{not json") == "{not json"
    assert score.answer_text({"rows": [1, 2]}) == '{"rows": [1, 2]}'


def test_checks_and_decide():
    checks = score.run_checks("Qatar 4.17% vs Kuwait 2.06%", ["4.17", "2.06", "Oman"], ["I don't know"])
    assert [c["ok"] for c in checks] == [True, True, False, True]
    opts = score.options_with_defaults({})
    passed, reason = score.decide("succeeded", "Qatar 4.17% vs Kuwait 2.06%", checks, None, opts)
    assert not passed and reason == "Missing: Oman"

    ok = score.run_checks("Qatar 4.17%", ["4.17"], [])
    assert score.decide("succeeded", "Qatar 4.17%", ok, None, opts) == (True, "")
    assert score.decide("succeeded", "Qatar 4.17%", ok, {"correctness": 5, "grounding": 4, "completeness": 5, "reason": "Correct."}, opts) == (True, "Correct.")
    passed, reason = score.decide("succeeded", "Qatar 4.17%", ok, {"correctness": 2, "grounding": 4, "completeness": 5, "reason": "Wrong period."}, opts)
    assert not passed and reason == "Judge: correctness 2/5 (needs 4) · Wrong period."
    # grounding is only required when asked for
    strict = score.options_with_defaults({"min_grounding": 4})
    assert score.decide("succeeded", "x", [], {"correctness": 5, "grounding": 2, "completeness": 5}, strict)[0] is False
    # a judge error does not decide: the checks do
    assert score.decide("succeeded", "Qatar 4.17%", ok, {"error": "timeout"}, opts) == (True, "")
    assert score.decide("failed", "", [], None, opts) == (False, "The agent job failed.")
    assert score.decide("succeeded", "  ", [], None, opts) == (False, "The agent returned an empty answer.")


def test_summary():
    rs = [
        {"status": "scored", "passed": True, "checks": [{"ok": True}], "judge": {"correctness": 5, "grounding": 4, "completeness": 5}, "metrics": {"latency_ms": 1000, "tokens": 100, "tool_errors": 0}},
        {"status": "scored", "passed": False, "checks": [{"ok": False}], "judge": {"correctness": 2, "grounding": None, "completeness": 3}, "metrics": {"latency_ms": 3000, "tokens": 300, "tool_errors": 1}},
        {"status": "error", "passed": False, "metrics": {}},
        {"status": "submitted"},
    ]
    s = score.summarize(rs)
    assert (s["total"], s["done"], s["passed"], s["failed"], s["errors"]) == (4, 3, 1, 1, 1)
    assert s["pass_rate"] == round(1 / 3, 4)
    assert (s["correctness"], s["grounding"], s["completeness"], s["judged"]) == (3.5, 4.0, 4.0, 2)
    assert (s["latency_ms_avg"], s["latency_ms_p95"], s["tokens_total"], s["tool_errors"]) == (2000, 3000, 400, 1)
    assert (s["checks_passed"], s["checks_total"]) == (1, 2)
    assert score.summarize([])["pass_rate"] is None


def test_judge_against_a_model_server():
    seen = []

    def handler(req: httpx.Request) -> httpx.Response:
        seen.append(req)
        if req.url.path == "/v1/models":
            return httpx.Response(200, json={"data": [{"id": "qwen72b"}]})
        body = json.loads(req.content)
        assert body["model"] == "qwen72b" and body["temperature"] == 0
        return httpx.Response(200, json={"choices": [{"message": {"content": '{"correctness": 5, "grounding": 4, "completeness": 5, "reason": "Matches."}'}}]})

    settings = Settings(gateway_default_upstream="http://vllm:8000/", tenants_file="none.yaml")
    j = Judge(settings, transport=httpx.MockTransport(handler))
    v = asyncio.run(j.grade("q", "2.62%", ["2.62"], "It was 2.62%.", "[tool] {} → 2.62"))
    assert v == {"correctness": 5, "grounding": 4, "completeness": 5, "reason": "Matches.", "model": "qwen72b"}
    assert str(seen[0].url) == "http://vllm:8000/v1/models"
    # no evidence: grounding is not scored; a second call reuses the model name
    v = asyncio.run(j.grade("q", "", [], "a", None))
    assert v["grounding"] is None and [r.url.path for r in seen].count("/v1/models") == 1

    bad = Judge(settings, transport=httpx.MockTransport(lambda req: httpx.Response(503, text="overloaded")))
    bad.model = "m"
    assert asyncio.run(bad.grade("q", "", [], "a", None))["error"].startswith("judge model server answered 503")


def test_judge_messages_and_parsing():
    msgs = build_messages("Latest inflation?", "2.62% in April 2026", ["2.62"], "It was 2.62%.", "[get_indicator_value] {} → {\"value\": 2.62}")
    user = msgs[1]["content"]
    assert "REFERENCE ANSWER:\n2.62% in April 2026" in user and "- 2.62" in user and "EVIDENCE (what" in user and user.endswith("It was 2.62%.")
    assert "EVIDENCE:\n(none)" in build_messages("q", "", [], "a", None)[1]["content"]

    v = parse_verdict('<think>hmm {"x":1}</think>```json\n{"correctness": 4.6, "grounding": null, "completeness": "3", "reason": "Fine"}\n```')
    assert v == {"correctness": 5, "grounding": None, "completeness": 3, "reason": "Fine"}
    assert parse_verdict('{"correctness": 9, "grounding": 0}')["correctness"] == 5
    with pytest.raises(ValueError):
        parse_verdict("no json here")
    with pytest.raises(ValueError):
        parse_verdict('{"grounding": 3}')
