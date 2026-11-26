"""AI judge: one chat completion per answer on an OpenAI-compatible model server.

The judge reads the question, the reference answer and required facts (when the
case has them), the agent's answer and the evidence the agent's tools returned
(from the trace), and scores 1–5:

  correctness   agrees with the reference answer (or, without one, with the evidence)
  grounding     every fact in the answer is supported by the evidence; null without evidence
  completeness  answers every part of the question

The model server is the one the gateway forwards to (`PLATFORM_EVAL_JUDGE_PROVIDER`
picks the upstream; empty = the default upstream). The model is
`PLATFORM_EVAL_JUDGE_MODEL`, or the first model the server lists.
A judge failure never fails the case: the result keeps `{"error": ...}` and the
fact checks still decide.
"""
from __future__ import annotations

import json
import re
from typing import Any, Iterable

import httpx

from ..config import Settings

EVIDENCE_MAX = 8000
ANSWER_MAX = 6000

SYSTEM = (
    "You grade answers of an AI assistant. Be strict and literal. "
    "Reply with one JSON object only, no other text:\n"
    '{"correctness": 1-5, "grounding": 1-5 or null, "completeness": 1-5, "reason": "one or two sentences"}\n'
    "correctness: 5 = says the same as the reference answer (numbers, periods, names); 3 = partly right; 1 = wrong or a refusal. "
    "Without a reference answer, judge it against the evidence. Extra correct detail is fine. "
    "A different number, period or unit than the reference is wrong.\n"
    "grounding: 5 = every fact in the answer is supported by the evidence; 1 = facts that are not in the evidence (made up). "
    "null when there is no evidence.\n"
    "completeness: 5 = answers every part of the question; 1 = answers none of it.\n"
    "reason: what is wrong or missing, in plain words; for a fully correct answer, say so briefly."
)

_THINK = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)
_OBJECT = re.compile(r"\{.*\}", re.DOTALL)


def _clip(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[:limit] + f"\n… ({len(text) - limit} more characters cut)"


def build_messages(question: str, expected: str, must_include: Iterable[str], answer: str, evidence: str | None) -> list[dict[str, str]]:
    facts = [t for t in must_include if str(t).strip()]
    parts = [f"QUESTION:\n{question}"]
    parts.append(f"REFERENCE ANSWER:\n{expected}" if expected.strip() else "REFERENCE ANSWER:\n(none)")
    if facts:
        parts.append("REQUIRED FACTS (alternatives separated by |):\n" + "\n".join(f"- {t}" for t in facts))
    parts.append(f"EVIDENCE (what the assistant's tools returned):\n{_clip(evidence, EVIDENCE_MAX)}" if evidence and evidence.strip() else "EVIDENCE:\n(none)")
    parts.append(f"ANSWER TO GRADE:\n{_clip(answer, ANSWER_MAX)}")
    return [{"role": "system", "content": SYSTEM}, {"role": "user", "content": "\n\n".join(parts)}]


def _score(v: Any) -> int | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        n = round(float(v))
    except (TypeError, ValueError):
        return None
    return max(1, min(5, n))


def parse_verdict(content: str) -> dict[str, Any]:
    """The judge's JSON object from its reply (tolerates think blocks and code fences)."""
    text = _THINK.sub("", content or "").strip()
    m = _OBJECT.search(text)
    if not m:
        raise ValueError("the judge did not reply with JSON")
    data = json.loads(m.group(0))
    if not isinstance(data, dict):
        raise ValueError("the judge did not reply with a JSON object")
    out = {k: _score(data.get(k)) for k in ("correctness", "grounding", "completeness")}
    if out["correctness"] is None:
        raise ValueError("the judge gave no correctness score")
    out["reason"] = str(data.get("reason") or "").strip()[:1000]
    return out


class Judge:
    def __init__(self, settings: Settings, transport: httpx.AsyncBaseTransport | None = None):
        self.base = settings.upstream_for(settings.eval_judge_provider) if settings.eval_judge_provider else settings.gateway_default_upstream.rstrip("/")
        self.model = settings.eval_judge_model
        headers = {"Authorization": f"Bearer {settings.eval_judge_api_key}"} if settings.eval_judge_api_key else {}
        self._http = httpx.AsyncClient(base_url=self.base, timeout=settings.eval_judge_timeout_s, transport=transport, headers=headers)

    async def aclose(self) -> None:
        await self._http.aclose()

    async def _model(self) -> str:
        if not self.model:
            r = await self._http.get("/v1/models")
            r.raise_for_status()
            items = r.json().get("data") or []
            if not items:
                raise RuntimeError("the model server lists no models")
            self.model = items[0]["id"]
        return self.model

    async def grade(self, question: str, expected: str, must_include: Iterable[str], answer: str, evidence: str | None) -> dict[str, Any]:
        try:
            model = await self._model()
            r = await self._http.post(
                "/v1/chat/completions",
                json={"model": model, "messages": build_messages(question, expected, must_include, answer, evidence), "temperature": 0, "max_tokens": 600},
            )
            if r.status_code >= 400:
                return {"error": f"judge model server answered {r.status_code}: {r.text[:200]}"}
            content = ((r.json().get("choices") or [{}])[0].get("message") or {}).get("content") or ""
            verdict = parse_verdict(content)
        except (httpx.HTTPError, ValueError, RuntimeError, KeyError) as exc:
            return {"error": f"{type(exc).__name__}: {exc}"[:500]}
        if verdict.get("grounding") is not None and not (evidence and evidence.strip()):
            verdict["grounding"] = None  # nothing to be grounded in
        verdict["model"] = model
        return verdict
