"""Export runs on the job queue.

  pg_export        {run_id}  read → write → upload (+ provision KAP once) → hand over to the watcher
  pg_export_watch  {run_id}  poll KAP every ~10 s (a new delayed job each time, no sleeping)
                             until the pipeline job created after the upload finishes, ≤ 15 min

Run status: queued → running → waiting_for_kap → succeeded | failed.
If the three files hash the same as the previous successful run, KAP would
see no new versions: kap_scan / kap_load are skipped and the run succeeds.
"""
from __future__ import annotations

import logging
from contextlib import AbstractAsyncContextManager
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping

import asyncpg

from .. import queue
from ..config import Settings
from ..kap.client import KapClient, KapError
from . import build, provision
from .build import ExportError
from .kapcsv import FILES
from .s3 import S3NotConfigured, S3Store, object_key, object_prefix
from .secrets import SecretBox, SecretKeyMissing
from .source import SourceParams, SourceSession, SourceUnavailable, open_source
from .store import Conflict, PgStore

log = logging.getLogger(__name__)

STEPS = (
    ("read", "Read from PostgreSQL"),
    ("write", "Write KAP CSV files"),
    ("upload", "Upload to S3"),
    ("kap_scan", "KAP picks up the files"),
    ("kap_load", "KAP loads the indicators"),
)
UNCHANGED = "unchanged — KAP keeps the current version"
KAP_RUNNING = ("waiting", "running")


def initial_steps() -> list[dict[str, Any]]:
    return [{"key": k, "title": t, "state": "pending", "detail": None, "started_at": None, "finished_at": None} for k, t in STEPS]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _ts(v: Any) -> datetime | None:
    if v is None:
        return None
    d = v if isinstance(v, datetime) else datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def _size(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n / 1024 / 1024:.1f} MB"


# ---------------------------------------------------------------- dependencies (swappable in tests)
class Services:
    def __init__(
        self,
        store_factory: Callable[[Any], Any] = PgStore,
        s3_factory: Callable[[Settings], Any] = S3Store.from_settings,
        opener: Callable[[SourceParams], AbstractAsyncContextManager[SourceSession]] = open_source,
    ):
        self.store_factory, self.s3_factory, self.opener = store_factory, s3_factory, opener
        self._s3: Any = None

    def store(self, pool: Any) -> Any:
        return self.store_factory(pool)

    def s3(self, settings: Settings) -> Any:
        if self._s3 is None:
            self._s3 = self.s3_factory(settings)
        return self._s3

    def open(self, params: SourceParams) -> AbstractAsyncContextManager[SourceSession]:
        return self.opener(params)


SERVICES = Services()


def source_params(source: Mapping[str, Any], settings: Settings) -> SourceParams:
    password = SecretBox(settings.secret_key).decrypt(source["password_enc"]) if source.get("password_enc") else None
    return SourceParams(source["host"], int(source["port"]), source["database"], source["username"], password, source["sslmode"])


def describe_error(exc: BaseException) -> str:
    if isinstance(exc, (ExportError, SecretKeyMissing, S3NotConfigured)):
        return str(exc)
    if isinstance(exc, SourceUnavailable):
        return f"Cannot connect to the source database: {exc}"
    if isinstance(exc, KapError):
        return f"KAP refused the request: {exc}"
    if isinstance(exc, asyncpg.PostgresError):
        return f"Source query failed: {exc}"
    mod = type(exc).__module__ or ""
    if mod.startswith(("botocore", "boto3", "s3transfer")):
        return f"S3 upload failed: {exc}"
    if isinstance(exc, KeyError) and "tenant" in str(exc):
        return f"KAP tenant is not configured: {exc}"
    return f"{type(exc).__name__}: {exc}"


class RunSteps:
    """A run's step list, persisted on every change."""

    def __init__(self, store: Any, run_id: str, steps: list[dict[str, Any]]):
        self.store, self.run_id = store, run_id
        self.steps = [dict(s) for s in steps]

    def get(self, key: str) -> dict[str, Any]:
        return next(s for s in self.steps if s["key"] == key)

    def current(self) -> str | None:
        return next((s["key"] for s in self.steps if s["state"] == "running"), None)

    async def set(self, key: str, state: str, detail: str | None = None, **run_fields: Any) -> None:
        s = self.get(key)
        now = _now().isoformat()
        if state == "running" and not s.get("started_at"):
            s["started_at"] = now
        if state in ("done", "failed", "skipped"):
            s["started_at"] = s.get("started_at") or now
            s["finished_at"] = now
        s["state"] = state
        if detail is not None:
            s["detail"] = detail
        await self.store.update_run(self.run_id, steps=self.steps, **run_fields)


async def start_run(pool: Any, tenant_id: str, dataset_id: str, trigger: str) -> dict[str, Any]:
    """Create a queued run and its pg_export job. Raises store.Conflict when one is already active."""
    st = SERVICES.store(pool)
    run = await st.create_run(tenant_id, dataset_id, trigger, initial_steps())
    await queue.enqueue(pool, "pg_export", {"run_id": run["id"]}, tenant_id=tenant_id, idempotency_key=f"pgexport:{run['id']}", max_attempts=2)
    return run


async def _enqueue_watch(pool: Any, run: Mapping[str, Any], polls: int, settings: Settings) -> None:
    await queue.enqueue(
        pool, "pg_export_watch", {"run_id": run["id"]}, tenant_id=run["tenant_id"],
        run_after=_now() + timedelta(seconds=settings.pg_export_poll_s), idempotency_key=f"pgwatch:{run['id']}:{polls}", max_attempts=3,
    )


# ---------------------------------------------------------------- pg_export
async def handle_pg_export(pool: Any, kap: KapClient, settings: Settings, job: queue.Job) -> dict[str, Any]:
    st = SERVICES.store(pool)
    run = await st.get_run(None, job.payload["run_id"])
    if run is None:
        return {"skipped": "unknown run"}
    if run["status"] not in ("queued", "running"):  # duplicate delivery of a finished run
        return {"skipped": f"run is {run['status']}"}
    tenant_id = run["tenant_id"]
    steps = RunSteps(st, run["id"], initial_steps())  # a retried run starts over
    await st.update_run(run["id"], status="running", started_at=_now(), finished_at=None, error=None, steps=steps.steps)
    outputs: dict[str, Any] = {}
    try:
        dataset = await st.get_dataset(tenant_id, run["dataset_id"])
        if dataset is None:
            raise ExportError("The dataset no longer exists.")
        source = await st.get_source(tenant_id, dataset["source_id"])
        if source is None:
            raise ExportError("The dataset's source no longer exists.")

        # read
        await steps.set("read", "running", f"Connecting to {source['host']}:{source['port']}/{source['database']}")
        async with SERVICES.open(source_params(source, settings)) as session:
            rr = await build.read(session, dataset)
        counts = rr.counts()
        detail = " · ".join(f"{n} {counts[n]:,} rows" for n in FILES)
        if rr.problems:
            detail += " — notes: " + "; ".join(rr.problems)
        await steps.set("read", "done", detail, counts=counts)

        # write
        await steps.set("write", "running")
        outputs = build.write(rr)
        files = [{"name": o.filename, "key": object_key(tenant_id, dataset["name"], o.filename), "bytes": o.bytes} for o in outputs.values()]
        hashes = {o.filename: o.sha256 for o in outputs.values()}
        total = sum(o.bytes for o in outputs.values())
        await steps.set("write", "done", f"{len(files)} files, {_size(total)} (UTF-8 with BOM)", files=files, file_hashes=hashes)

        # upload (+ KAP objects, once)
        await steps.set("upload", "running", f"s3://{settings.s3_bucket}/{object_prefix(tenant_id, dataset['name'])}")
        s3 = SERVICES.s3(settings)
        uploaded_at = _now()
        for name, o in outputs.items():
            await s3.put(object_key(tenant_id, dataset["name"], o.filename), o.fileobj())
        kap_info = await provision.ensure(kap, tenant_id, dataset, settings)
        if kap_info != (dataset.get("kap") or {}):
            await st.update_dataset(tenant_id, dataset["id"], {"kap": kap_info})
        kap_run = {"collection": kap_info["collection"], "pipeline_job_id": None, "state": None, "progress": None, "result": None}
        await steps.set(
            "upload", "done",
            f"{len(files)} objects in s3://{settings.s3_bucket}/{object_prefix(tenant_id, dataset['name'])}; KAP collection {kap_info['collection']}",
            uploaded_at=uploaded_at, kap=kap_run,
        )

        # unchanged?
        previous = await st.last_success_hashes(dataset["id"], run["id"])
        if previous == hashes:
            for key in ("kap_scan", "kap_load"):
                s = steps.get(key)
                s.update(state="skipped", detail=UNCHANGED, finished_at=_now().isoformat())
            await st.update_run(run["id"], steps=steps.steps, status="succeeded", finished_at=_now())
            return {"status": "succeeded", "unchanged": True, "counts": counts}

        await steps.set("kap_scan", "running", f"Waiting for KAP's connector {kap_info['connector']} to scan (every 1 min)", status="waiting_for_kap", polls=0)
        await _enqueue_watch(pool, run, 0, settings)
        return {"status": "waiting_for_kap", "counts": counts}
    except Exception as exc:
        message = describe_error(exc)
        if not isinstance(exc, (ExportError, SourceUnavailable, KapError)):
            log.warning("export run %s failed", run["id"], exc_info=True)
        key = steps.current() or next((s["key"] for s in steps.steps if s["state"] == "pending"), "read")
        await steps.set(key, "failed", message, status="failed", error=message, finished_at=_now())
        return {"status": "failed", "error": message}
    finally:
        for o in outputs.values():
            o.discard()


# ---------------------------------------------------------------- pg_export_watch
def _job_detail(j: Mapping[str, Any]) -> str:
    p = j.get("progress") or {}
    if p.get("total"):
        return f"{j.get('state')}: {p.get('completed', 0)}/{p['total']} files ({p.get('percent', 0)}%)"
    return str(j.get("state"))


async def _file_states(kap: KapClient, tenant_id: str, collection_id: str) -> str:
    try:
        page = await kap.request(tenant_id, "GET", f"/api/collections/{collection_id}/files", params={"limit": 200})
    except KapError:
        return ""
    wanted = {f"{n}.csv" for n in FILES}
    parts = []
    for f in (page or {}).get("items", []):
        name = str(f.get("path") or f.get("name") or f.get("key") or "")
        base = name.rsplit("/", 1)[-1]
        if base in wanted:
            parts.append(f"{base}: {f.get('state') or f.get('status')}")
    return "; ".join(sorted(parts))


async def _find_job(kap: KapClient, tenant_id: str, pipeline_id: str, collection_id: str, job_id: str | None, since: datetime) -> dict[str, Any] | None:
    if job_id:
        return await kap.request(tenant_id, "GET", f"/api/pipelines/{pipeline_id}/jobs/{job_id}")
    page = await kap.request(tenant_id, "GET", f"/api/pipelines/{pipeline_id}/jobs", params={"collection_id": collection_id, "sort": "-created_at", "limit": 20})
    newer = [j for j in (page or {}).get("items", []) if (_ts(j.get("created_at")) or since) >= since]
    return min(newer, key=lambda j: _ts(j["created_at"])) if newer else None


async def handle_pg_export_watch(pool: Any, kap: KapClient, settings: Settings, job: queue.Job) -> dict[str, Any]:
    st = SERVICES.store(pool)
    run = await st.get_run(None, job.payload["run_id"])
    if run is None or run["status"] != "waiting_for_kap":
        return {"skipped": "run is not waiting for KAP"}
    tenant_id = run["tenant_id"]
    steps = RunSteps(st, run["id"], run["steps"])
    polls = int(run.get("polls") or 0) + 1
    uploaded_at = _ts(run["uploaded_at"]) or _now()
    deadline = uploaded_at + timedelta(seconds=settings.pg_export_wait_s)
    kap_run = dict(run.get("kap") or {})
    late = _now() >= deadline
    minutes = settings.pg_export_wait_s // 60

    async def fail(key: str, message: str) -> dict[str, Any]:
        await steps.set(key, "failed", message, status="failed", error=message, finished_at=_now(), polls=polls, kap=kap_run)
        return {"status": "failed", "error": message}

    async def again(key: str, detail: str) -> dict[str, Any]:
        await steps.set(key, "running", detail, polls=polls, kap=kap_run)
        await _enqueue_watch(pool, run, polls, settings)
        return {"status": "waiting_for_kap", "polls": polls}

    try:
        dataset = await st.get_dataset(tenant_id, run["dataset_id"])
        info = (dataset or {}).get("kap") or {}
        if not info.get("pipeline_id") or not info.get("collection_id"):
            return await fail("kap_scan", "The dataset has no KAP pipeline/collection recorded.")
        kj = await _find_job(kap, tenant_id, info["pipeline_id"], info["collection_id"], kap_run.get("pipeline_job_id"), uploaded_at)
    except KapError as exc:
        key = steps.current() or "kap_scan"
        if (exc.status >= 500 or exc.status == 429) and not late:
            return await again(key, f"KAP is not answering ({exc.status}); retrying")
        return await fail(key, describe_error(exc))
    except Exception as exc:  # network trouble etc.: keep polling until the deadline
        key = steps.current() or "kap_scan"
        if not late:
            log.warning("watch of run %s: %s", run["id"], exc)
            return await again(key, f"Could not reach KAP ({type(exc).__name__}); retrying")
        return await fail(key, describe_error(exc))

    if kj is None:
        if late:
            return await fail("kap_scan", f"KAP did not start a pipeline job within {minutes} min of the upload. Check connector {info.get('connector')} in KAP (connectors → check).")
        waited = int((_now() - uploaded_at).total_seconds())
        return await again("kap_scan", f"Waiting for KAP's connector {info.get('connector')} to scan (every 1 min) — {waited} s since upload")

    kap_run.update(pipeline_job_id=kj.get("id"), state=kj.get("state"), progress=kj.get("progress"), result=kj.get("result"))
    if steps.get("kap_scan")["state"] != "done":
        steps.get("kap_scan").update(state="done", detail=f"KAP pipeline job {kj.get('id')} created {kj.get('created_at')}", finished_at=_now().isoformat())
    state = kj.get("state")
    if state in KAP_RUNNING:
        if late:
            return await fail("kap_load", f"KAP job {kj.get('id')} is still {state} after {minutes} min; it may still finish — check it in KAP.")
        return await again("kap_load", _job_detail(kj))

    files = await _file_states(kap, tenant_id, info["collection_id"])
    result = kj.get("result") or {}
    summary = f"{state}: succeeded {result.get('succeeded', 0)}, failed {result.get('failed', 0)}, cancelled {result.get('cancelled', 0)}"
    if files:
        summary += f" — {files}"
    if state == "succeeded" and not result.get("failed"):
        await steps.set("kap_load", "done", summary, status="succeeded", finished_at=_now(), polls=polls, kap=kap_run)
        return {"status": "succeeded", "kap_job": kj.get("id")}
    err = kj.get("error") or {}
    reason = err.get("message") or err.get("code") or ("some files failed" if result.get("failed") else state)
    steps.get("kap_load")["detail"] = summary
    return await fail("kap_load", f"KAP pipeline job {kj.get('id')} {state}: {reason}" + (f" ({files})" if files else ""))


# ---------------------------------------------------------------- schedule
async def schedule_tick(pool: Any, settings: Settings) -> int:
    """Start exports of datasets whose next_run_at has passed. Returns how many were started."""
    st = SERVICES.store(pool)
    started = 0
    for d in await st.due_datasets():
        try:
            await start_run(pool, d["tenant_id"], d["id"], "schedule")
            started += 1
        except Conflict:
            log.info("dataset %s: previous export still in progress; skipping this slot", d["id"])
    return started
