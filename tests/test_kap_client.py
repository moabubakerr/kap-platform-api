import httpx
import pytest

from kap_platform.config import M2MAuth, Tenant, TokenAuth
from kap_platform.kap.client import KapClient, KapError

SBX = "71b6c98a-7f7a-4a8b-ab31-132c85bdc88b"
SCEAI = "a81e92d4-9541-433a-b295-c869b5dbaf17"


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("KAP_TOKEN_SBX", "legacy-token")
    monkeypatch.setenv("KAP_M2M_SECRET", "s3cret")


def tenants():
    return [
        Tenant(id=SBX, name="sandbox", auth=TokenAuth(type="token", token_env="KAP_TOKEN_SBX")),
        Tenant(id=SCEAI, name="SCEAI", auth=M2MAuth(type="m2m", client_id="cid", client_secret_env="KAP_M2M_SECRET")),
    ]


async def test_static_token_and_tenant_header(env):
    seen = []

    def handler(req: httpx.Request):
        seen.append(req)
        return httpx.Response(200, json={"items": [{"id": "a1", "name": "x"}], "has_next": False})

    k = KapClient("http://kap", tenants(), transport=httpx.MockTransport(handler))
    assert await k.list_agents(SBX) == [{"id": "a1", "name": "x"}]
    assert seen[0].headers["authorization"] == "Bearer legacy-token"
    assert seen[0].headers["x-tenant-id"] == SBX
    assert seen[0].url.params["limit"] == "200"
    await k.aclose()


async def test_m2m_token_is_fetched_once_and_cached(env):
    token_calls = []

    def handler(req: httpx.Request):
        if req.url.path == "/oauth/token":
            token_calls.append(req)
            return httpx.Response(200, json={"access_token": "m2m-1", "expires_in": 300})
        assert req.headers["authorization"] == "Bearer m2m-1"
        return httpx.Response(200, json={"id": "j"})

    k = KapClient("http://kap", tenants(), transport=httpx.MockTransport(handler))
    await k.get_agent_job(SCEAI, "a", "j")
    await k.get_agent_job(SCEAI, "a", "j")
    assert len(token_calls) == 1
    assert b"grant_type=client_credentials" in token_calls[0].content
    assert token_calls[0].headers["authorization"].startswith("Basic ")
    await k.aclose()


async def test_m2m_refetches_token_after_401(env):
    tokens = iter(["old", "new"])

    def handler(req: httpx.Request):
        if req.url.path == "/oauth/token":
            return httpx.Response(200, json={"access_token": next(tokens), "expires_in": 300})
        if req.headers["authorization"] == "Bearer old":
            return httpx.Response(401, json={"type": "unauthorized", "message": "revoked"})
        return httpx.Response(200, json={"ok": True})

    k = KapClient("http://kap", tenants(), transport=httpx.MockTransport(handler))
    assert await k.request(SCEAI, "GET", "/api/users/me") == {"ok": True}
    await k.aclose()


async def test_429_is_retried_once(env, monkeypatch):
    attempts = []

    def handler(req: httpx.Request):
        attempts.append(1)
        if len(attempts) == 1:
            return httpx.Response(429, headers={"Retry-After": "0"}, json={"type": "rate_limited", "message": "slow down"})
        return httpx.Response(200, json={"ok": True})

    k = KapClient("http://kap", tenants(), transport=httpx.MockTransport(handler))
    assert await k.request(SBX, "GET", "/api/agents") == {"ok": True}
    assert len(attempts) == 2
    await k.aclose()


async def test_errors_carry_type_and_request_id(env):
    def handler(req: httpx.Request):
        return httpx.Response(404, headers={"x-request-id": "rid-1"}, json={"type": "not_found", "message": "No agent", "data": {}})

    k = KapClient("http://kap", tenants(), transport=httpx.MockTransport(handler))
    with pytest.raises(KapError) as e:
        await k.get_agent_job(SBX, "a", "j")
    assert e.value.status == 404 and e.value.request_id == "rid-1" and "not_found" in str(e.value)
    await k.aclose()


async def test_paginates_and_respects_max_items(env):
    def handler(req: httpx.Request):
        offset = int(req.url.params["offset"])
        limit = int(req.url.params["limit"])
        items = [{"id": f"j{i}"} for i in range(offset, min(offset + limit, 7))]
        return httpx.Response(200, json={"items": items, "has_next": offset + limit < 7})

    k = KapClient("http://kap", tenants(), transport=httpx.MockTransport(handler))
    jobs = [j async for j in k.paginate(SBX, "/api/agents/a/jobs", page_size=3)]
    assert [j["id"] for j in jobs] == [f"j{i}" for i in range(7)]
    assert len(await k.recent_agent_jobs(SBX, "a", limit=2)) == 2
    await k.aclose()


async def test_unknown_tenant_is_rejected(env):
    k = KapClient("http://kap", tenants(), transport=httpx.MockTransport(lambda r: httpx.Response(200)))
    with pytest.raises(KeyError):
        await k.list_agents("00000000-0000-0000-0000-000000000000")
    await k.aclose()
