"""Model gateway: a transparent proxy between KAP and the model servers.

KAP model providers point their `base_url` here:

    http://platform-gateway:8090/t/{tenant_id}/p/{provider}/v1

The gateway forwards `/v1/...` to the provider's upstream (vLLM, Ollama…)
unchanged — headers, body, streaming — and records each call (request,
response, status, millisecond timing) through the injected `record` function.
Recording never delays or breaks the response: it runs after the reply is
sent, and its errors are logged, not raised.

Deployment note (verified): the gateway's host name must be on KAP's
`OUTBOUND_PRIVATE_ALLOWLIST`, and it must run on KAP's Docker network.
"""
from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from starlette.background import BackgroundTask

from ..store import RecordedCall

log = logging.getLogger(__name__)

Recorder = Callable[[RecordedCall], Awaitable[None]]

HOP_BY_HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailers", "transfer-encoding", "upgrade", "content-length", "host", "content-encoding"}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _json_or_none(raw: bytes) -> Any:
    if not raw:
        return None
    try:
        return json.loads(raw)
    except ValueError:
        return {"raw": raw.decode("utf-8", errors="replace")[:100_000]}


def create_gateway(upstream_for: Callable[[str], str], record: Recorder, timeout_s: float = 600.0, transport: httpx.AsyncBaseTransport | None = None) -> FastAPI:
    client = httpx.AsyncClient(timeout=httpx.Timeout(timeout_s, connect=10.0), transport=transport)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        try:
            yield
        finally:
            await client.aclose()

    app = FastAPI(title="KAP platform model gateway", docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)

    async def _safe_record(rec: RecordedCall) -> None:
        try:
            await record(rec)
        except Exception:  # recording must never affect the model call
            log.exception("failed to record model call %s %s", rec.method, rec.path)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.api_route("/t/{tenant_id}/p/{provider}/{path:path}", methods=["GET", "POST"])
    async def proxy(tenant_id: str, provider: str, path: str, request: Request) -> Response:
        started = _now()
        body = await request.body()
        req_json = _json_or_none(body)
        target = f"{upstream_for(provider)}/{path}"
        if request.url.query:
            target += f"?{request.url.query}"
        headers = {k: v for k, v in request.headers.items() if k.lower() not in HOP_BY_HOP}
        tenant = None if tenant_id in ("-", "_") else tenant_id

        def rec(status: int | None, response: Any, error: str | None = None) -> RecordedCall:
            return RecordedCall(tenant, provider, started, _now(), request.method, "/" + path, status, req_json, response, error)

        try:
            upstream_req = client.build_request(request.method, target, headers=headers, content=body)
            upstream = await client.send(upstream_req, stream=True)
        except httpx.HTTPError as exc:
            asyncio.create_task(_safe_record(rec(502, None, f"{type(exc).__name__}: {exc}")))
            return JSONResponse({"error": {"message": "gateway could not reach the model server", "type": "bad_gateway"}}, status_code=502)

        resp_headers = {k: v for k, v in upstream.headers.items() if k.lower() not in HOP_BY_HOP}
        is_stream = "text/event-stream" in upstream.headers.get("content-type", "")

        if is_stream:
            chunks: list[bytes] = []

            async def relay():
                try:
                    # decoded bytes: content-encoding is dropped from the relayed headers
                    async for chunk in upstream.aiter_bytes():
                        chunks.append(chunk)
                        yield chunk
                finally:
                    await upstream.aclose()

            async def after() -> None:
                raw = b"".join(chunks).decode("utf-8", errors="replace")
                await _safe_record(rec(upstream.status_code, {"sse": raw}))

            return StreamingResponse(relay(), status_code=upstream.status_code, headers=resp_headers, background=BackgroundTask(after))

        raw = await upstream.aread()
        await upstream.aclose()

        async def after_plain() -> None:
            await _safe_record(rec(upstream.status_code, _json_or_none(raw)))

        return Response(content=raw, status_code=upstream.status_code, headers=resp_headers, background=BackgroundTask(after_plain))

    return app
