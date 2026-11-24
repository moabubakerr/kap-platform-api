"""Client for the KAP REST API (v1.1).

Conventions from the KAP Developer Guide: Bearer auth on every call except
health; `X-Tenant-ID` (optional for tenant accounts, required for the
superuser); pagination `limit` ≤ 200 with `items/total/has_next`; errors as
`{type, message, data}`; 429 with `Retry-After`.

Auth per tenant: a static legacy API token, or M2M client credentials
(`POST /oauth/token`, HTTP Basic, grant_type=client_credentials) with the
token cached until shortly before it expires.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, AsyncIterator

import httpx

from ..config import M2MAuth, Tenant, TokenAuth


class KapError(Exception):
    def __init__(self, status: int, body: Any, request_id: str | None = None):
        self.status = status
        self.body = body
        self.request_id = request_id
        kind = body.get("type") if isinstance(body, dict) else None
        msg = body.get("message") if isinstance(body, dict) else str(body)[:200]
        super().__init__(f"KAP {status} {kind or ''}: {msg} (x-request-id {request_id})")


@dataclass
class _CachedToken:
    value: str
    expires_at: float


class KapClient:
    def __init__(self, base_url: str, tenants: list[Tenant], timeout_s: float = 30.0, transport: httpx.AsyncBaseTransport | None = None):
        self._http = httpx.AsyncClient(base_url=base_url.rstrip("/"), timeout=timeout_s, transport=transport)
        self._tenants = {t.id: t for t in tenants}
        self._tokens: dict[str, _CachedToken] = {}
        self._token_locks: dict[str, asyncio.Lock] = {}

    async def aclose(self) -> None:
        await self._http.aclose()

    def tenant(self, tenant_id: str) -> Tenant:
        try:
            return self._tenants[tenant_id]
        except KeyError:
            raise KeyError(f"tenant {tenant_id} is not configured in the tenants file") from None

    # ------------------------------------------------------------------ auth
    async def _bearer(self, tenant: Tenant) -> str:
        if isinstance(tenant.auth, TokenAuth):
            return tenant.auth.token()
        assert isinstance(tenant.auth, M2MAuth)
        cached = self._tokens.get(tenant.id)
        if cached and cached.expires_at > time.monotonic() + 60:
            return cached.value
        lock = self._token_locks.setdefault(tenant.id, asyncio.Lock())
        async with lock:
            cached = self._tokens.get(tenant.id)
            if cached and cached.expires_at > time.monotonic() + 60:
                return cached.value
            r = await self._http.post(
                "/oauth/token",
                data={"grant_type": "client_credentials"},
                auth=(tenant.auth.client_id, tenant.auth.client_secret()),
            )
            if r.status_code != 200:
                raise KapError(r.status_code, _body(r), r.headers.get("x-request-id"))
            body = r.json()
            self._tokens[tenant.id] = _CachedToken(body["access_token"], time.monotonic() + float(body.get("expires_in", 300)))
            return body["access_token"]

    # ------------------------------------------------------------------ requests
    async def request(self, tenant_id: str, method: str, path: str, *, params: Any = None, json: Any = None) -> Any:
        r = await self.request_raw(tenant_id, method, path, params=params, json=json)
        if r.status_code >= 400:
            raise KapError(r.status_code, _body(r), r.headers.get("x-request-id"))
        return r.json() if r.content else None

    async def request_raw(self, tenant_id: str, method: str, path: str, *, params: Any = None, json: Any = None) -> httpx.Response:
        """One KAP call with auth, the tenant header, one M2M re-auth and one 429 wait.
        Returns the response as is (errors included), for passthrough."""
        tenant = self.tenant(tenant_id)
        for attempt in range(2):
            headers = {"Authorization": f"Bearer {await self._bearer(tenant)}", "X-Tenant-ID": tenant.id}
            r = await self._http.request(method, path, params=params, json=json, headers=headers)
            if r.status_code == 401 and isinstance(tenant.auth, M2MAuth) and attempt == 0:
                self._tokens.pop(tenant.id, None)  # token revoked or expired early: fetch a new one once
                continue
            if r.status_code == 429 and attempt == 0:
                await asyncio.sleep(min(float(r.headers.get("Retry-After", "1")), 10.0))
                continue
            return r
        return r

    async def paginate(self, tenant_id: str, path: str, params: dict[str, Any] | None = None, page_size: int = 200, max_items: int | None = None) -> AsyncIterator[dict[str, Any]]:
        offset, seen = 0, 0
        while True:
            page = await self.request(tenant_id, "GET", path, params={**(params or {}), "limit": page_size, "offset": offset})
            for item in page.get("items", []):
                yield item
                seen += 1
                if max_items is not None and seen >= max_items:
                    return
            if not page.get("has_next"):
                return
            offset += page_size

    # ------------------------------------------------------------------ resources used in slice 1
    async def list_agents(self, tenant_id: str) -> list[dict[str, Any]]:
        return [a async for a in self.paginate(tenant_id, "/api/agents")]

    async def get_agent(self, tenant_id: str, agent_id: str) -> dict[str, Any]:
        return await self.request(tenant_id, "GET", f"/api/agents/{agent_id}")

    async def list_users(self, tenant_id: str) -> list[dict[str, Any]]:
        return [u async for u in self.paginate(tenant_id, "/api/users")]

    async def recent_agent_jobs(self, tenant_id: str, agent_id: str, limit: int = 50) -> list[dict[str, Any]]:
        return [j async for j in self.paginate(tenant_id, f"/api/agents/{agent_id}/jobs", {"sort": "-created_at"}, page_size=min(limit, 200), max_items=limit)]

    async def get_agent_job(self, tenant_id: str, agent_id: str, job_id: str) -> dict[str, Any]:
        return await self.request(tenant_id, "GET", f"/api/agents/{agent_id}/jobs/{job_id}")

    async def submit_agent_job(self, tenant_id: str, agent_id: str, *, user_prompt: str | None = None, payload: dict[str, Any] | None = None, callback_url: str | None = None, callback_headers: dict[str, str] | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if user_prompt is not None:
            body["user_prompt"] = user_prompt
        if payload is not None:
            body["payload"] = payload
        if callback_url:
            body["callback_url"] = callback_url
        if callback_headers:
            body["callback_headers"] = callback_headers
        return await self.request(tenant_id, "POST", f"/api/agents/{agent_id}/jobs", json=body)


def _body(r: httpx.Response) -> Any:
    try:
        return r.json()
    except ValueError:
        return r.text
