"""API tests with a fake KAP client and a fake pool (no database)."""
from fastapi.testclient import TestClient

from kap_platform.api.app import create_api
from kap_platform.config import Settings

SBX = "71b6c98a-7f7a-4a8b-ab31-132c85bdc88b"


class FakePool:
    async def fetchval(self, *_):
        return 1

    async def close(self):
        pass


class FakeKap:
    def __init__(self):
        self.submitted = []

    async def list_agents(self, tenant_id):
        if tenant_id != SBX:
            raise KeyError(tenant_id)
        return [{"id": "a1", "name": "sandbox_indicators_agent", "type": "planner_and_executor", "model_ref": "agent_default", "collections": ["sandbox_indicators"], "allowed_tools": ["find_indicator"], "system_prompt_is_set": True, "config": {}}]

    async def aclose(self):
        pass


def client(fake=None):
    settings = Settings(api_keys=["k1"], hook_token="h1", tenants_file="none.yaml")
    fake = fake or FakeKap()

    async def pool_factory():
        return FakePool()

    app = create_api(settings, pool_factory=pool_factory, kap_factory=lambda: fake)
    return TestClient(app)


def test_list_agents_returns_a_page_without_internal_fields():
    with client() as c:
        r = c.get(f"/api/t/{SBX}/agents", headers={"Authorization": "Bearer k1"})
    assert r.status_code == 200
    body = r.json()
    assert body["total"] == 1 and body["has_next"] is False
    assert body["items"][0]["name"] == "sandbox_indicators_agent"
    assert "system_prompt_is_set" not in body["items"][0]


def test_list_agents_unknown_tenant_is_404():
    with client() as c:
        r = c.get("/api/t/00000000-0000-0000-0000-000000000000/agents", headers={"Authorization": "Bearer k1"})
    assert r.status_code == 404 and r.json()["type"] == "tenant_not_configured"


def test_api_requires_key():
    with client() as c:
        assert c.get(f"/api/t/{SBX}/agents").status_code == 401
        assert c.get(f"/api/t/{SBX}/agents", headers={"Authorization": "Bearer wrong"}).status_code == 401


def test_submit_rejects_empty_body():
    with client() as c:
        r = c.post(f"/api/t/{SBX}/agents/a1/jobs", json={}, headers={"Authorization": "Bearer k1"})
    assert r.status_code == 422


def test_callback_requires_hook_token():
    with client() as c:
        r = c.post(f"/hooks/kap/t/{SBX}/agents/a1/jobs", json={"job_id": "j1", "status": "succeeded"})
    assert r.status_code == 401
