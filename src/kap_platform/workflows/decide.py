"""Steps the platform decides with the model server (no KAP agent): a router whose
paths are chosen by the model, and a validator check. Each returns a result the
runner stores; a model failure is a step failure with the reason.
"""
from __future__ import annotations

import json
import re
from typing import Any

from .graph import CHECKS

_OBJECT = re.compile(r"\{.*\}", re.DOTALL)
EVIDENCE_MAX = 8000


def _json(text: str) -> dict[str, Any]:
    m = _OBJECT.search(text or "")
    if not m:
        raise ValueError("the model did not reply with JSON")
    data = json.loads(m.group(0))
    if not isinstance(data, dict):
        raise ValueError("the model did not reply with a JSON object")
    return data


async def route_by_model(model: Any, request: str, options: list[tuple[str, str]]) -> tuple[str, str]:
    """(target node id, reason). Options: (target, condition)."""
    listing = "\n".join(f"{i + 1}. {cond}" for i, (_, cond) in enumerate(options))
    messages = [
        {"role": "system", "content": 'You route a request to one path. Reply with one JSON object only: {"route": <number>, "reason": "short"}.'},
        {"role": "user", "content": f"REQUEST:\n{request}\n\nPATHS (when to take each):\n{listing}"},
    ]
    data = _json(await model.complete(messages, 200))
    try:
        i = int(data.get("route")) - 1
    except (TypeError, ValueError):
        raise ValueError("the model gave no route number") from None
    if not 0 <= i < len(options):
        raise ValueError(f"the model chose route {i + 1}, which does not exist")
    return options[i][0], str(data.get("reason") or "").strip()[:300]


async def check_answer(model: Any, check: str, request: str, answer: str, evidence: str) -> tuple[bool, str]:
    """(passed, reason) for a validator step."""
    rule = CHECKS.get(check, check)
    messages = [
        {"role": "system", "content": 'You check an answer against one rule. Be strict. Reply with one JSON object only: {"pass": true or false, "reason": "short, plain words"}.'},
        {"role": "user", "content": f"RULE:\n{rule}\n\nREQUEST:\n{request}\n\nRESULTS OF EARLIER STEPS (evidence):\n{(evidence or '(none)')[:EVIDENCE_MAX]}\n\nANSWER TO CHECK:\n{answer}"},
    ]
    data = _json(await model.complete(messages, 300))
    ok = data.get("pass")
    if not isinstance(ok, bool):
        ok = str(ok).strip().lower() in ("true", "yes", "pass", "1")
    return ok, str(data.get("reason") or "").strip()[:500]
