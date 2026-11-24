"""KAP passthrough: allowlist, forwarding and error mapping, against a mock KAP."""
import httpx
import pytest
from fastapi.testclient import TestClient

from kap_platform.api import kap_proxy
from kap_platform.api.app import create_api
from kap_platform.config import Settings, Tenant, TokenAuth
from kap_platform.kap.client import KapClient

SBX = "71b6c98a-7f7a-4a8b-ab31-132c85bdc88b"
AUTH = {"Authorization": "Bearer k1"}


class FakePool:
    async def fetchval(self, *_):
        return 1

    async def close(self):
        pass


@pytest.fixture
def kap_calls(monkeypatch):
    monkeypatch.setenv("KAP_TOKEN_SBX", "legacy-token")
    return []


def make_client(kap_calls, handler=None):
    def default(req: httpx.Request):
        kap_calls.append(req)
        if req.url.path == "/api/connectors/c1" and req.method == "DELETE":
            return httpx.Response(204)
        if req.url.path == "/api/collections/x/files/f/content":
            return httpx.Response(200, content=b"%PDF-1", headers={"content-type": "application/pdf"})
        if req.url.path == "/api/agents/missing":
            return httpx.Response(404, json={"type": "not_found", "message": "Agent not found", "data": {}})
        if req.url.path == "/api/pipelines":
            return httpx.Response(503, text="upstream down")
        return httpx.Response(200, json={"items": [{"id": "a1"}], "total": 1, "has_next": False})

    tenants = [Tenant(id=SBX, name="sandbox", auth=TokenAuth(type="token", token_env="KAP_TOKEN_SBX"))]
    kap = KapClient("http://kap", tenants, transport=httpx.MockTransport(handler or default))
    settings = Settings(api_keys=["k1"], hook_token="h1", tenants_file="none.yaml")

    async def pool_factory():
        return FakePool()

    return TestClient(create_api(settings, pool_factory=pool_factory, kap_factory=lambda: kap))


def test_get_is_forwarded_with_query_and_tenant_auth(kap_calls):
    with make_client(kap_calls) as c:
        r = c.get(f"/api/t/{SBX}/kap/agents", params=[("limit", "50"), ("type__in", "workflow"), ("type__in", "planner_and_executor")], headers=AUTH)
    assert r.status_code == 200 and r.json()["items"] == [{"id": "a1"}]
    req = kap_calls[0]
    assert req.url.path == "/api/agents"
    assert req.url.params.get_list("type__in") == ["workflow", "planner_and_executor"]
    assert req.headers["authorization"] == "Bearer legacy-token" and req.headers["x-tenant-id"] == SBX


def test_write_body_is_forwarded(kap_calls):
    with make_client(kap_calls) as c:
        r = c.patch(f"/api/t/{SBX}/kap/agents/a1", json={"description": "d"}, headers=AUTH)
    assert r.status_code == 200
    assert kap_calls[0].method == "PATCH" and kap_calls[0].content == b'{"description":"d"}'


def test_no_content_and_binary_responses_pass_through(kap_calls):
    with make_client(kap_calls) as c:
        assert c.delete(f"/api/t/{SBX}/kap/connectors/c1", headers=AUTH).status_code == 204
        r = c.get(f"/api/t/{SBX}/kap/collections/x/files/f/content", headers=AUTH)
    assert r.content == b"%PDF-1" and r.headers["content-type"] == "application/pdf"


def test_kap_errors_keep_their_status_and_body_but_5xx_becomes_502(kap_calls):
    with make_client(kap_calls) as c:
        r = c.get(f"/api/t/{SBX}/kap/agents/missing", headers=AUTH)
        assert r.status_code == 404 and r.json()["type"] == "not_found"
        assert c.get(f"/api/t/{SBX}/kap/pipelines", headers=AUTH).status_code == 502


@pytest.mark.parametrize(
    "method,path,status,kind",
    [
        ("GET", "tenants", 403, "not_forwarded"),
        ("GET", "oauth/token", 403, "not_forwarded"),
        ("POST", "agents/a1/jobs", 409, "use_platform_submit"),
    ],
)
def test_refused_paths_never_reach_kap(kap_calls, method, path, status, kind):
    with make_client(kap_calls) as c:
        r = c.request(method, f"/api/t/{SBX}/kap/{path}", headers=AUTH, json={} if method == "POST" else None)
    assert r.status_code == status and r.json()["type"] == kind
    assert kap_calls == []


def test_check_rejects_traversal_and_odd_segments():
    for bad in ("agents/../tenants", "agents/%2e%2e", "agents//x", "agents/a b"):
        with pytest.raises(kap_proxy.ProxyRefused):
            kap_proxy.check("GET", bad)
    assert kap_proxy.check("POST", "agents/a1/jobs/j1/cancel") == "/api/agents/a1/jobs/j1/cancel"
    assert kap_proxy.check("GET", "agents/a1/jobs") == "/api/agents/a1/jobs"


def test_unknown_tenant_and_missing_key(kap_calls):
    with make_client(kap_calls) as c:
        assert c.get("/api/t/nope/kap/agents", headers=AUTH).json()["type"] == "tenant_not_configured"
        assert c.get(f"/api/t/{SBX}/kap/agents").status_code == 401
    assert kap_calls == []
