"""KAP passthrough for the console's hubs.

`/api/t/{tenant_id}/kap/{path}` forwards to KAP `/api/{path}` with the tenant's
credentials (token or M2M) and `X-Tenant-ID`. KAP's own permission checks
apply to the platform's KAP account.

Only tenant resources the console uses are allowed (`ALLOWED`). Not allowed:
- tenants and queue policies (superuser, cross-tenant);
- submitting agent jobs here: use `POST /api/t/{tenant}/agents/{agent}/jobs`,
  which adds the platform's callback so traces link.

Until the platform has its own sign-in (a later slice), every console user acts
as that KAP account. Writes are logged.
"""
from __future__ import annotations

import logging
import re

from fastapi import Request
from fastapi.responses import Response

from ..kap.client import KapClient

log = logging.getLogger(__name__)

ALLOWED = frozenset(
    {
        "agents",
        "agent-schemas",
        "connectors",
        "pipelines",
        "collections",
        "entity-types",
        "mcp-servers",
        "model-providers",
        "users",
        "groups",
        "roles",
        "permissions",
        "audit",
        "config-versions",
    }
)
METHODS = frozenset({"GET", "POST", "PATCH", "PUT", "DELETE"})
_SUBMIT_JOB = re.compile(r"^agents/[^/]+/jobs/?$")
_SEGMENT = re.compile(r"^[A-Za-z0-9._~:@-]+$")
# Response headers worth passing on (the rest are hop-by-hop or KAP internals).
_PASS_HEADERS = ("content-type", "content-disposition", "x-request-id", "retry-after")


class ProxyRefused(Exception):
    def __init__(self, status: int, kind: str, message: str):
        super().__init__(message)
        self.status, self.kind, self.message = status, kind, message


def check(method: str, path: str) -> str:
    """Validate a passthrough call; returns the KAP path (`/api/...`)."""
    if method not in METHODS:
        raise ProxyRefused(405, "method_not_allowed", f"{method} is not forwarded to KAP.")
    segments = path.strip("/").split("/")
    if not segments or not all(_SEGMENT.match(s) and s not in (".", "..") for s in segments):
        raise ProxyRefused(400, "bad_path", "Invalid KAP path.")
    if segments[0] not in ALLOWED:
        raise ProxyRefused(403, "not_forwarded", f"KAP resource '{segments[0]}' is not available through the platform.")
    clean = "/".join(segments)
    if method == "POST" and _SUBMIT_JOB.match(clean):
        raise ProxyRefused(409, "use_platform_submit", "Submit agent jobs with POST /api/t/{tenant}/agents/{agent}/jobs so the platform can trace them.")
    return f"/api/{clean}"


async def forward(k: KapClient, tenant_id: str, method: str, path: str, request: Request) -> Response:
    kap_path = check(method, path)
    body = None
    if method in ("POST", "PATCH", "PUT"):
        raw = await request.body()
        body = await request.json() if raw else None
    params = list(request.query_params.multi_items())
    r = await k.request_raw(tenant_id, method, kap_path, params=params or None, json=body)
    if method != "GET":
        log.info("kap write %s %s tenant=%s -> %s", method, kap_path, tenant_id, r.status_code)
    headers = {h: r.headers[h] for h in _PASS_HEADERS if h in r.headers}
    status = r.status_code if r.status_code < 500 else 502
    return Response(content=r.content, status_code=status, headers=headers)
