"""Scoring of one answer, and the summary of a run. Pure functions.

Fact checks (`must_include`): every term must appear in the answer.
- A numeric term ("2.62%", "7,984", "-0.5") matches any number in the answer
  equal to it at the term's precision: "2.6" matches 2.62 (the answer is more
  precise), "2.62" does not match 2.6. Thousands separators, % and Arabic-Indic
  digits are accepted.
- Any other term matches as text, ignoring case, spacing and punctuation
  around words.
- "a | b" passes when any alternative matches (e.g. English | Arabic).
`must_not_include` terms fail the case when they appear (same matching).

Pass rule: the KAP job succeeded with a non-empty answer, every check passed,
and — when the judge ran — correctness ≥ min_correctness and grounding ≥
min_grounding (0 = not required).
"""
from __future__ import annotations

import json
import math
import re
import statistics
import unicodedata
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping

_ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹٫٬", "0123456789" * 2 + ".,")
_NUM_TERM = re.compile(r"^\s*([-+−]?)\s*((?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?|\.\d+)\s*%?\s*$")
_NUM_IN_TEXT = re.compile(r"(?<![\w.])([-+−]?)((?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?)(?![\d])")
_ANSWER_KEYS = ("answer", "final_answer", "output", "response", "result", "text", "content", "message")


def _digits(s: str) -> str:
    return s.translate(_ARABIC_DIGITS)


def answer_text(result: Any) -> str:
    """The agent's answer from a KAP job result: plain text, or the text field of a JSON result."""
    if result is None:
        return ""
    value: Any = result
    if isinstance(value, str):
        s = value.strip()
        if s[:1] in "{[":
            try:
                value = json.loads(s)
            except ValueError:
                return s
        else:
            return s
    for _ in range(3):  # {"result": {"answer": "..."}}
        if isinstance(value, Mapping):
            inner = next((value[k] for k in _ANSWER_KEYS if k in value and value[k] not in (None, "")), None)
            if inner is None:
                break
            value = inner
        else:
            break
    if isinstance(value, str):
        return value.strip()
    return json.dumps(value, ensure_ascii=False)


def _norm_text(s: str) -> str:
    s = unicodedata.normalize("NFKC", _digits(s)).casefold()
    s = re.sub(r"[^\w%.\-+]+", " ", s)  # punctuation → space, keep word chars, %, ., -, +
    s = re.sub(r"(?<!\d)\.|\.(?!\d)", " ", s)  # dots that are not decimal points
    return " ".join(s.split())


def parse_number(term: str) -> tuple[Decimal, int] | None:
    """(value, decimals) when the term is a number (optionally with % and thousands separators)."""
    m = _NUM_TERM.match(_digits(term))
    if not m:
        return None
    sign, body = m.group(1), m.group(2).replace(",", "")
    try:
        value = Decimal(body)
    except InvalidOperation:
        return None
    if sign in ("-", "−"):
        value = -value
    decimals = len(body.split(".", 1)[1]) if "." in body else 0
    return value, decimals


def numbers_in(text: str) -> list[Decimal]:
    out = []
    for m in _NUM_IN_TEXT.finditer(_digits(text)):
        try:
            v = Decimal(m.group(2).replace(",", ""))
        except InvalidOperation:
            continue
        out.append(-v if m.group(1) in ("-", "−") else v)
    return out


def _number_matches(target: Decimal, decimals: int, found: Iterable[Decimal]) -> bool:
    """Equal at the term's precision. A negative term also matches its size ("-0.5" ↔ "fell by 0.5")."""
    tol = Decimal(5) * Decimal(10) ** (-(decimals + 1))  # half a unit of the term's last digit
    for v in found:
        if abs(v - target) <= tol:
            return True
        if target < 0 and abs(abs(v) - abs(target)) <= tol:
            return True
    return False


def term_found(term: str, answer: str) -> bool:
    alternatives = [t.strip() for t in str(term).split("|")]
    nums: list[Decimal] | None = None
    words: str | None = None
    for alt in alternatives:
        if not alt:
            continue
        n = parse_number(alt)
        if n is not None:
            if nums is None:
                nums = numbers_in(answer)
            if _number_matches(n[0], n[1], nums):
                return True
            continue
        if words is None:
            words = f" {_norm_text(answer).replace('-', ' ')} "
        needle = _norm_text(alt).replace("-", " ")
        # whole words only: "rate" does not match "rates"
        if needle and f" {needle} " in words:
            return True
    return False


def run_checks(answer: str, must_include: Iterable[str], must_not_include: Iterable[str]) -> list[dict[str, Any]]:
    checks = [{"kind": "includes", "term": t, "ok": term_found(t, answer)} for t in must_include if str(t).strip()]
    checks += [{"kind": "excludes", "term": t, "ok": not term_found(t, answer)} for t in must_not_include if str(t).strip()]
    return checks


DEFAULT_OPTIONS = {"judge": True, "min_correctness": 4, "min_grounding": 0, "parallel": 2}


def options_with_defaults(options: Mapping[str, Any] | None) -> dict[str, Any]:
    out = dict(DEFAULT_OPTIONS)
    out.update({k: v for k, v in (options or {}).items() if v is not None})
    return out


def decide(job_status: str | None, answer: str, checks: list[dict[str, Any]], judge: Mapping[str, Any] | None, options: Mapping[str, Any]) -> tuple[bool, str]:
    """(passed, reason). The reason names the first problems, or the judge's note on a pass."""
    if job_status != "succeeded":
        return False, f"The agent job {job_status or 'did not finish'}."
    if not answer.strip():
        return False, "The agent returned an empty answer."
    reasons = []
    missing = [c["term"] for c in checks if c["kind"] == "includes" and not c["ok"]]
    present = [c["term"] for c in checks if c["kind"] == "excludes" and not c["ok"]]
    if missing:
        reasons.append("Missing: " + ", ".join(missing))
    if present:
        reasons.append("Should not contain: " + ", ".join(present))
    if judge and not judge.get("error"):
        mc, mg = int(options.get("min_correctness") or 0), int(options.get("min_grounding") or 0)
        c, g = judge.get("correctness"), judge.get("grounding")
        judge_failed = False
        if mc and isinstance(c, (int, float)) and c < mc:
            reasons.append(f"Judge: correctness {c}/5 (needs {mc})")
            judge_failed = True
        if mg and isinstance(g, (int, float)) and g < mg:
            reasons.append(f"Judge: grounding {g}/5 (needs {mg})")
            judge_failed = True
        # the judge's note explains its own verdict: only next to a judge failure, or on a pass
        if judge_failed and judge.get("reason"):
            reasons.append(str(judge["reason"]))
        if not reasons:
            return True, str(judge.get("reason") or "")
    if reasons:
        return False, " · ".join(reasons)
    return True, ""


def _avg(xs: list[float]) -> float | None:
    return round(sum(xs) / len(xs), 2) if xs else None


def _p95(xs: list[int]) -> int | None:
    if not xs:
        return None
    s = sorted(xs)
    return s[min(len(s) - 1, math.ceil(0.95 * len(s)) - 1)]


def summarize(results: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    rs = list(results)
    done = [r for r in rs if r.get("status") in ("scored", "error")]
    passed = sum(1 for r in done if r.get("passed"))
    errors = sum(1 for r in done if r.get("status") == "error")
    judged = [r["judge"] for r in done if isinstance(r.get("judge"), Mapping) and not r["judge"].get("error")]

    def scores(k: str) -> list[float]:
        return [float(j[k]) for j in judged if isinstance(j.get(k), (int, float))]

    metrics = [r.get("metrics") or {} for r in done]
    lat = [int(m["latency_ms"]) for m in metrics if isinstance(m.get("latency_ms"), (int, float))]
    tokens = [int(m["tokens"]) for m in metrics if isinstance(m.get("tokens"), (int, float))]
    checks = [c for r in done for c in (r.get("checks") or [])]
    return {
        "total": len(rs),
        "done": len(done),
        "passed": passed,
        "failed": len(done) - passed - errors,
        "errors": errors,
        "pass_rate": round(passed / len(done), 4) if done else None,
        "checks_passed": sum(1 for c in checks if c.get("ok")),
        "checks_total": len(checks),
        "correctness": _avg(scores("correctness")),
        "grounding": _avg(scores("grounding")),
        "completeness": _avg(scores("completeness")),
        "judged": len(judged),
        "latency_ms_avg": int(statistics.mean(lat)) if lat else None,
        "latency_ms_p95": _p95(lat),
        "tokens_total": sum(tokens) if tokens else None,
        "tool_errors": sum(int(m.get("tool_errors") or 0) for m in metrics),
    }


def _pct(x: float) -> str:
    return f"{round(x * 100)}%"


def regression(prev_summary: Mapping[str, Any] | None, prev_results: list[Mapping[str, Any]] | None, summary: Mapping[str, Any],
               results: list[Mapping[str, Any]], min_pass_rate: float | None = None) -> tuple[bool | None, str | None]:
    """(regressed, reason) of a finished run against the previous run of the same test set and target.

    Regressed when the pass rate fell, when a question that passed before fails now, or when the
    pass rate is below `min_pass_rate`. None when there is nothing to compare with and no minimum.
    """
    if prev_results is None and min_pass_rate is None:
        return None, None
    rate = summary.get("pass_rate")
    reasons: list[str] = []
    if prev_results is not None:
        key = lambda r: r.get("case_id") or str(r.get("question") or "").strip().lower()  # noqa: E731
        before = {key(r): bool(r.get("passed")) for r in prev_results if r.get("status") in ("scored", "error")}
        broken = [str(r.get("question")) for r in results if before.get(key(r)) is True and not r.get("passed")]
        prev_rate = (prev_summary or {}).get("pass_rate")
        if prev_rate is not None and rate is not None and rate < prev_rate:
            reasons.append(f"Pass rate {_pct(prev_rate)} → {_pct(rate)}")
        if broken:
            shown = "; ".join(q if len(q) <= 80 else q[:79] + "…" for q in broken[:5])
            reasons.append(f"Now failing: {shown}" + (f" and {len(broken) - 5} more" if len(broken) > 5 else ""))
    if min_pass_rate is not None and rate is not None and rate < float(min_pass_rate):
        reasons.append(f"Pass rate {_pct(rate)} is below the required {_pct(float(min_pass_rate))}")
    return bool(reasons), " · ".join(reasons) or None
