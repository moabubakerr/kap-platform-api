"""Parse OpenAI-compatible chat requests and responses as KAP sends them.

Verified on the test stand (24 Nov 2026): KAP calls `/v1/chat/completions`
through the OpenAI Python SDK with `stream=false`; the job's `user_prompt` is
the first `user` message verbatim; tool calls and results use the OpenAI
format, linked by `tool_call_id`; `usage` carries token counts.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any


def content_text(content: Any) -> str:
    """Message content may be a string or a list of content parts."""
    if isinstance(content, list):
        return "".join(p.get("text", "") for p in content if isinstance(p, dict))
    return content or ""


@dataclass
class RequestSummary:
    model: str | None = None
    stream: bool = False
    n_messages: int = 0
    first_user_text: str | None = None
    prompt_hash: str | None = None


def summarize_request(body: Any) -> RequestSummary:
    if not isinstance(body, dict):
        return RequestSummary()
    msgs = body.get("messages") or []
    system = next((content_text(m.get("content")) for m in msgs if m.get("role") == "system"), "")
    first_user = next((content_text(m.get("content")) for m in msgs if m.get("role") == "user"), None)
    digest = None
    if first_user is not None:
        digest = hashlib.sha256((system + "\x00" + first_user).encode("utf-8")).hexdigest()
    return RequestSummary(
        model=body.get("model"),
        stream=bool(body.get("stream")),
        n_messages=len(msgs),
        first_user_text=first_user,
        prompt_hash=digest,
    )


@dataclass
class ToolCall:
    id: str | None
    name: str
    arguments: str


@dataclass
class Reply:
    text: str = ""
    tool_calls: list[ToolCall] = field(default_factory=list)
    usage: dict[str, int] | None = None


def parse_sse(raw: str) -> Reply:
    """Rebuild text, tool calls and usage from a streamed (SSE) response."""
    text: list[str] = []
    tools: dict[int, dict[str, str]] = {}
    usage = None
    for line in raw.splitlines():
        line = line.strip()
        if not line.startswith("data:"):
            continue
        data = line[5:].strip()
        if data == "[DONE]":
            break
        try:
            ev = json.loads(data)
        except ValueError:
            continue
        usage = ev.get("usage") or usage
        for ch in ev.get("choices") or []:
            delta = ch.get("delta") or {}
            if delta.get("content"):
                text.append(delta["content"])
            for tc in delta.get("tool_calls") or []:
                t = tools.setdefault(tc.get("index", 0), {"id": "", "name": "", "arguments": ""})
                t["id"] = t["id"] or (tc.get("id") or "")
                fn = tc.get("function") or {}
                t["name"] += fn.get("name") or ""
                t["arguments"] += fn.get("arguments") or ""
    return Reply(
        text="".join(text),
        tool_calls=[ToolCall(t["id"] or None, t["name"], t["arguments"]) for _, t in sorted(tools.items())],
        usage=usage,
    )


def parse_reply(response: Any) -> Reply:
    """Parse a stored response: a chat completion JSON, or {"sse": "<raw stream>"}."""
    if isinstance(response, dict) and "sse" in response:
        return parse_sse(response["sse"] or "")
    if not isinstance(response, dict):
        return Reply()
    msg = ((response.get("choices") or [{}])[0] or {}).get("message") or {}
    calls = [
        ToolCall(t.get("id"), (t.get("function") or {}).get("name") or "", (t.get("function") or {}).get("arguments") or "")
        for t in msg.get("tool_calls") or []
    ]
    return Reply(text=content_text(msg.get("content")), tool_calls=calls, usage=response.get("usage"))


def tool_results(request: Any) -> dict[str, str]:
    """tool_call_id → tool result text, from the `tool` messages of a request."""
    out: dict[str, str] = {}
    if not isinstance(request, dict):
        return out
    for m in request.get("messages") or []:
        if m.get("role") == "tool" and m.get("tool_call_id"):
            out[m["tool_call_id"]] = content_text(m.get("content"))
    return out


def try_json(text: str) -> Any:
    try:
        return json.loads(text)
    except (TypeError, ValueError):
        return text
