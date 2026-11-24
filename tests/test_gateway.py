import json

import httpx

from kap_platform.gateway.app import create_gateway

SBX = "71b6c98a-7f7a-4a8b-ab31-132c85bdc88b"
COMPLETION = {"choices": [{"message": {"role": "assistant", "content": "32.04%"}}], "usage": {"prompt_tokens": 9, "completion_tokens": 2, "total_tokens": 11}}
SSE = 'data: {"choices":[{"delta":{"content":"32."}}]}\n\ndata: {"choices":[{"delta":{"content":"04%"}}]}\n\ndata: [DONE]\n\n'


def upstream(req: httpx.Request) -> httpx.Response:
    if req.url.path == "/v1/models":
        return httpx.Response(200, json={"data": [{"id": "qwen72b"}]})
    body = json.loads(req.content)
    assert req.headers["authorization"] == "Bearer vllm-key"  # forwarded unchanged
    if body.get("stream"):
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, content=SSE.encode())
    return httpx.Response(200, json=COMPLETION)


def make(records, up=upstream):
    async def record(rec):
        records.append(rec)

    app = create_gateway(lambda provider: f"http://vllm-{provider}:8000", record, transport=httpx.MockTransport(up))
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway")


async def test_forwards_json_and_records_call():
    records = []
    async with make(records) as c:
        r = await c.post(f"/t/{SBX}/p/agent_default/v1/chat/completions", json={"model": "qwen72b", "messages": [{"role": "user", "content": "q"}]}, headers={"Authorization": "Bearer vllm-key"})
    assert r.status_code == 200 and r.json() == COMPLETION
    (rec,) = records
    assert rec.tenant_id == SBX and rec.provider == "agent_default" and rec.path == "/v1/chat/completions"
    assert rec.request["messages"][0]["content"] == "q" and rec.response == COMPLETION and rec.status == 200
    assert rec.finished_at >= rec.started_at and rec.started_at.tzinfo is not None


async def test_forwards_stream_unchanged_and_records_raw_sse():
    records = []
    async with make(records) as c:
        r = await c.post(f"/t/{SBX}/p/agent_default/v1/chat/completions", json={"model": "qwen72b", "stream": True, "messages": []}, headers={"Authorization": "Bearer vllm-key"})
    assert r.status_code == 200 and r.text == SSE
    assert records[0].response == {"sse": SSE}


async def test_get_models_passes_through_and_tenant_placeholder():
    records = []
    async with make(records) as c:
        r = await c.get("/t/-/p/agent_default/v1/models")
    assert r.json()["data"][0]["id"] == "qwen72b"
    assert records[0].tenant_id is None and records[0].method == "GET"


async def test_upstream_down_returns_502_and_records_error():
    records = []

    def down(req):
        raise httpx.ConnectError("refused", request=req)

    async with make(records, down) as c:
        r = await c.post(f"/t/{SBX}/p/agent_default/v1/chat/completions", json={"messages": []})
    assert r.status_code == 502 and r.json()["error"]["type"] == "bad_gateway"
    # the record task runs in the background; give it a tick
    import asyncio

    for _ in range(20):
        if records:
            break
        await asyncio.sleep(0.01)
    assert records[0].status == 502 and "ConnectError" in records[0].error


async def test_recorder_failure_does_not_break_the_response():
    async def broken(rec):
        raise RuntimeError("db down")

    app = create_gateway(lambda p: "http://vllm:8000", broken, transport=httpx.MockTransport(upstream))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gateway") as c:
        r = await c.post(f"/t/{SBX}/p/agent_default/v1/chat/completions", json={"messages": []}, headers={"Authorization": "Bearer vllm-key"})
    assert r.status_code == 200
