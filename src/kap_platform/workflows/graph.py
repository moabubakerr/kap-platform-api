"""Workflow graph semantics. Pure: no I/O.

The console's designer stores a version as
  nodes  [id, kind, label, sub]           kind: entry | agent | router | aggregate | review | fallback | output | tool
  edges  [from, to, kind, condition, data] kind: seq | par | cond | delegate | fallback
  np     {node id: {property: value}}     per-step settings from the designer's panel
  limits {dur: "60 s", steps: "12"}

How a run moves (one rule for every step):
- An edge is *active* once its source finished and the edge applies:
    source done    → seq and par edges; cond edges only to the routes the source chose
    source failed  → fallback and delegate edges (the failure path)
    source skipped → none
- A pending step becomes *ready* when every incoming edge is decided and at least
  one is active; *skipped* when every incoming edge is inactive. An aggregator
  set to wait for the "First completed step" is ready at the first active edge.
- A step's input is the request plus the outputs of the steps on its active
  incoming edges.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

FINISHED = ("done", "failed", "skipped")
OPEN = ("pending",)
BUSY = ("running", "waiting")

CHECKS = {
    "Numbers match the source data": "Every number in the answer appears in, or follows directly from, the results of the earlier steps.",
    "Every statement has a citation": "Every factual statement in the answer names its source.",
    "Answer language matches the question": "The answer is written in the same language as the request.",
    "No personal data in the answer": "The answer contains no personal data (names of private individuals, ID numbers, phone numbers, e-mail or home addresses).",
}


@dataclass
class Step:
    node_id: str
    kind: str
    label: str = ""
    status: str = "pending"
    output: str | None = None
    note: str | None = None
    chosen: list[str] | None = None
    seq: int | None = None
    job_id: str | None = None
    attempts: int = 0
    input: str | None = None
    started_at: Any = None
    finished_at: Any = None
    extra: dict[str, Any] = field(default_factory=dict)


def nodes_of(cfg: Mapping[str, Any]) -> list[list[Any]]:
    return [list(n) for n in (cfg.get("nodes") or [])]


def edges_of(cfg: Mapping[str, Any]) -> list[list[Any]]:
    return [list(e) + [""] * (5 - len(e)) for e in (cfg.get("edges") or [])]


def props(cfg: Mapping[str, Any], node_id: str) -> dict[str, str]:
    return dict((cfg.get("np") or {}).get(node_id) or {})


def label_of(node: list[Any]) -> str:
    kind, label = node[1], node[2] if len(node) > 2 else ""
    if label:
        return str(label)
    return {"entry": "Request", "output": "Answer", "router": "Router", "aggregate": "Aggregator", "review": "Check", "fallback": "Fallback", "tool": "Tool"}.get(kind, kind)


def is_human(cfg: Mapping[str, Any], node: list[Any]) -> bool:
    return node[1] == "review" and props(cfg, node[0]).get("rt") == "human"


# ---------------------------------------------------------------- checks before a run
def problems(cfg: Mapping[str, Any], known_agents: Iterable[str]) -> list[str]:
    """Why this graph cannot run (empty = it can). Mirrors the designer's errors that matter at run time."""
    known = set(known_agents)
    N, E = nodes_of(cfg), edges_of(cfg)
    ids = {n[0] for n in N}
    out: list[str] = []
    entries = [n for n in N if n[1] == "entry"]
    outputs = [n for n in N if n[1] == "output"]
    if len(entries) != 1:
        out.append("The workflow needs exactly one entry point.")
    if len(outputs) != 1:
        out.append("The workflow needs exactly one output.")
    for e in E:
        if e[0] not in ids or e[1] not in ids:
            out.append(f"A connection points to a step that does not exist ({e[0]} → {e[1]}).")
    for n in N:
        if n[1] == "tool":
            out.append(f"{label_of(n)}: tool steps cannot run on their own; KAP runs tools inside agents. Give the tool to an agent instead.")
        if n[1] in ("agent", "aggregate"):
            merge_first = n[1] == "aggregate" and props(cfg, n[0]).get("merge") == "First successful result"
            if not n[2] and not merge_first:
                out.append(f"Choose the agent for step {n[0]}.")
            elif n[2] and n[2] not in known:
                out.append(f"{n[2]}: no such agent in KAP.")
        if n[1] == "router" and sum(1 for e in E if e[0] == n[0] and e[2] == "cond") < 2:
            out.append(f"{label_of(n)} needs at least two conditional paths.")
    for e in E:
        if e[2] == "cond" and not str(e[3] or "").strip():
            out.append(f"Set the condition for the path {e[0]} → {e[1]}.")
    if entries and outputs and outputs[0][0] not in reachable(cfg, entries[0][0]):
        out.append("There is no path from the entry point to the output.")
    return out


def reachable(cfg: Mapping[str, Any], start: str) -> set[str]:
    R, E = {start}, edges_of(cfg)
    changed = True
    while changed:
        changed = False
        for e in E:
            if e[0] in R and e[1] not in R:
                R.add(e[1])
                changed = True
    return R


# ---------------------------------------------------------------- moving the run
def edge_active(edge: list[Any], src: Step) -> bool | None:
    """True / False once decided, None while the source has not finished."""
    if src.status not in FINISHED:
        return None
    if src.status == "skipped":
        return False
    kind = edge[2]
    if src.status == "done":
        if kind in ("seq", "par"):
            return True
        if kind == "cond":
            return edge[1] in (src.chosen or [])
        return False  # delegate, fallback: only on failure
    return kind in ("fallback", "delegate")


def advance(cfg: Mapping[str, Any], steps: Mapping[str, Step]) -> tuple[list[str], list[str]]:
    """(ready, skipped): pending steps whose incoming edges are decided."""
    E = edges_of(cfg)
    ready, skipped = [], []
    for nid, s in steps.items():
        if s.status != "pending" or s.kind == "entry":
            continue
        ins = [edge_active(e, steps[e[0]]) for e in E if e[1] == nid and e[0] in steps]
        first = s.kind == "aggregate" and props(cfg, nid).get("wait") == "First completed step"
        if any(x is True for x in ins) and (first or all(x is not None for x in ins)):
            ready.append(nid)
        elif all(x is False for x in ins):
            skipped.append(nid)  # also: no incoming edges at all
    return ready, skipped


def inputs_of(cfg: Mapping[str, Any], steps: Mapping[str, Step], nid: str) -> list[tuple[str, str, str]]:
    """(source id, label, output) of the finished steps on the active edges into `nid`, in edge order."""
    out = []
    for e in edges_of(cfg):
        if e[1] != nid or e[0] not in steps:
            continue
        src = steps[e[0]]
        if edge_active(e, src) and src.output:
            out.append((src.node_id, src.label, src.output))
    return out


def failure_handled(cfg: Mapping[str, Any], nid: str) -> bool:
    """Whether a failed step has a failure path (fallback or delegate edge) to follow."""
    return any(e[0] == nid and e[2] in ("fallback", "delegate") for e in edges_of(cfg))


# ---------------------------------------------------------------- routes
_WORD = re.compile(r"[a-z0-9؀-ۿ]+")


def _tokens(s: str) -> set[str]:
    return {t for t in _WORD.findall(s.lower()) if len(t) > 2}


def route_options(cfg: Mapping[str, Any], nid: str) -> list[tuple[str, str]]:
    """(target node id, condition) of the conditional paths out of `nid`."""
    return [(e[1], str(e[3] or "")) for e in edges_of(cfg) if e[0] == nid and e[2] == "cond"]


def default_route(cfg: Mapping[str, Any], nid: str) -> str | None:
    """The designer's "Default path" (a step label), or the last conditional path."""
    opts = route_options(cfg, nid)
    if not opts:
        return None
    want = props(cfg, nid).get("def")
    by_label = {label_of(n): n[0] for n in nodes_of(cfg)}
    if want and by_label.get(want) in {t for t, _ in opts}:
        return by_label[want]
    return opts[-1][0]


def route_by_rule(cfg: Mapping[str, Any], nid: str, text: str) -> tuple[str | None, str]:
    """Rule-based routing: the path whose condition shares the most words with the request.
    Conditions are words or phrases ("numbers, indicators, statistics"); "a | b" lists alternatives."""
    words = _tokens(text)
    best, score = None, 0
    for target, cond in route_options(cfg, nid):
        terms = [t.strip().lower() for t in re.split(r"[|,;]", cond) if t.strip()]
        s = sum(2 for t in terms if len(t) > 2 and t in text.lower()) + len(_tokens(cond) & words)
        if s > score:
            best, score = target, s
    if best is None:
        d = default_route(cfg, nid)
        return d, "No condition matched the request; took the default path."
    return best, "Matched the condition words in the request."


# ---------------------------------------------------------------- limits and text
_DUR = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(s|sec|secs|seconds?|m|min|mins|minutes?|h|hours?|days?)?\s*$", re.IGNORECASE)


def seconds(value: Any, default: float) -> float:
    """"60 s", "2 min", "24 h", "3 days" → seconds."""
    m = _DUR.match(str(value or ""))
    if not m:
        return default
    n, unit = float(m.group(1)), (m.group(2) or "s").lower()
    return n * (86400 if unit.startswith("day") else 3600 if unit.startswith("h") else 60 if unit.startswith("m") else 1)


def int_or(value: Any, default: int) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def agent_prompt(request: str, inputs: list[tuple[str, str, str]], entry_id: str) -> str:
    earlier = [(label, text) for nid, label, text in inputs if nid != entry_id]
    if not earlier:
        return request
    blocks = "\n\n".join(f"### {label}\n{text}" for label, text in earlier)
    return f"{request}\n\nUse these results from earlier steps:\n\n{blocks}"


def aggregate_prompt(request: str, inputs: list[tuple[str, str, str]], entry_id: str) -> str:
    blocks = "\n\n".join(f"### {label}\n{text}" for nid, label, text in inputs if nid != entry_id)
    return (
        f"Request: {request}\n\nCombine the results below into one answer to the request. "
        "Keep every number, period and source exactly as given; say where the results disagree; do not add facts.\n\n" + blocks
    )


def combine(inputs: list[tuple[str, str, str]], entry_id: str) -> str:
    texts = [(label, text) for nid, label, text in inputs if nid != entry_id]
    if not texts:
        return ""
    if len(texts) == 1:
        return texts[0][1]
    return "\n\n".join(f"### {label}\n{text}" for label, text in texts)
