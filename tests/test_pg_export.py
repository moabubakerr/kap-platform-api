"""KAP provisioning bodies and export run step transitions, with in-memory fakes."""
from datetime import timedelta

import pytest
from cryptography.fernet import Fernet

from kap_platform import queue
from kap_platform.config import Settings
from kap_platform.pgsource import export, provision
from kap_platform.pgsource.secrets import SecretBox

from pg_fakes import SBX, FakeKap, FakeQueuePool, FakeS3, FakeSession, MemoryStore, now, opener_for

KEY = Fernet.generate_key().decode()


def settings(**kw):
    base = dict(tenants_file="none.yaml", secret_key=KEY, s3_access_key="ak", s3_secret_key="sk", s3_bucket="platform-exports", s3_url="http://s3:9000")
    return Settings(**{**base, **kw})


# ---------------------------------------------------------------- provisioning
def test_connector_body_uses_plain_keys_prefix_and_globs():
    body = provision.connector_body("scai_ind_pg", SBX, "scai", settings())
    assert body["name"] == "scai_ind_pg" and body["type"] == "s3"
    assert body["settings"]["object_selector"] == {"prefix": f"{SBX}/scai/", "include_globs": ["registry.csv", "observations.csv", "narratives.csv"], "exclude_globs": []}
    assert body["settings"]["sync"] == {"interval": "PT1M"}
    cfg = body["config"]
    assert cfg["bucket_name"] == "platform-exports" and cfg["endpoint_url"] == "http://s3:9000" and cfg["use_ssl"] is False
    assert cfg["auth"] == {"type": "static_keys", "access_key": "ak", "secret_key": "sk"}
    assert provision.connector_body("x_pg", SBX, "d", settings(s3_kap_url="https://s3.example:9000/"))["config"]["endpoint_url"] == "https://s3.example:9000"


def test_pipeline_and_collection_bodies():
    p = provision.pipeline_body("scai_ind_pipeline")
    assert p["name"] == "scai_ind_pipeline" and p["kind"] == "indicators" and p["mode"] == "snapshot" and p["embedding_model_ref"] == "embed_default"
    stage = p["stages"][0]
    assert stage["type"] == "csv_loader" and stage["config"]["files"] == {"**/registry.csv": "registry", "**/observations.csv": "observations", "**/narratives.csv": "narratives"}
    assert stage["config"]["encoding"] == "utf-8-sig" and stage["config"]["period_formats"] == ["%Y-%m", "%Y-Q%q", "%Y"]
    assert provision.PIPELINE_TEMPLATE["stages"][0]["name"] == "load_csv"  # template not mutated
    c = provision.collection_body("scai_ind", "scai_ind_pipeline", "scai_ind_pg")
    assert c == {"name": "scai_ind", "pipeline_ref": "scai_ind_pipeline", "connectors": ["scai_ind_pg"], "sync_strategy": "overwrite", "entity": {"enabled": False}}


async def test_ensure_creates_once_and_reuses_by_name():
    kap = FakeKap()
    ds = {"name": "scai", "collection_name": "scai_ind", "kap": None}
    info = await provision.ensure(kap, SBX, ds, settings())
    assert [c[:2] for c in kap.calls] == [("POST", "/api/connectors"), ("POST", "/api/pipelines"), ("POST", "/api/collections")]
    assert info["connector"] == "scai_ind_pg" and info["pipeline"] == "scai_ind_pipeline" and info["collection"] == "scai_ind"
    assert info["collection_id"] and info["pipeline_id"] and info["provisioned_at"]
    # recorded → no KAP calls at all
    kap.calls.clear()
    assert (await provision.ensure(kap, SBX, {**ds, "kap": info}, settings()))["collection_id"] == info["collection_id"]
    assert kap.calls == []
    # not recorded, but exists in KAP → reused, nothing created
    again = await provision.ensure(kap, SBX, ds, settings())
    assert kap.calls == [] and again["pipeline_id"] == info["pipeline_id"]


# ---------------------------------------------------------------- runs
@pytest.fixture
def env(monkeypatch):
    db: dict = {}
    s3 = FakeS3()
    session = FakeSession()
    monkeypatch.setattr(export, "SERVICES", export.Services(store_factory=lambda pool: MemoryStore(db), s3_factory=lambda st: s3, opener=opener_for(session)))
    st = MemoryStore(db)
    return {"db": db, "s3": s3, "session": session, "store": st, "pool": FakeQueuePool(), "kap": FakeKap()}


async def make_dataset(st, **kw):
    src = await st.create_source(SBX, {"name": "scai", "host": "scai-chatbot-postgres-1", "database": "scai_indicators", "username": "kap_platform_ro", "password_enc": SecretBox(KEY).encrypt("pw")})
    return await st.create_dataset(SBX, {"source_id": src["id"], "name": "scai", "preset": "scai_indicators", "collection_name": "scai_ind", **kw})


def job(kind, run_id):
    return queue.Job(id=None, tenant_id=SBX, kind=kind, payload={"run_id": run_id}, attempts=1, max_attempts=2)


def states(run):
    return {s["key"]: s["state"] for s in run["steps"]}


async def test_export_runs_steps_and_waits_for_kap_then_succeeds(env):
    st, pool, kap = env["store"], env["pool"], env["kap"]
    ds = await make_dataset(st)
    run = await export.start_run(pool, SBX, ds["id"], "manual")
    assert run["status"] == "queued" and pool.enqueued[-1]["kind"] == "pg_export"

    out = await export.handle_pg_export(pool, kap, settings(), job("pg_export", run["id"]))
    assert out["status"] == "waiting_for_kap"
    run = await st.get_run(SBX, run["id"])
    assert run["status"] == "waiting_for_kap"
    assert states(run) == {"read": "done", "write": "done", "upload": "done", "kap_scan": "running", "kap_load": "pending"}
    assert run["counts"] == {"registry": 3, "observations": 4, "narratives": 2}
    assert [f["name"] for f in run["files"]] == ["registry.csv", "observations.csv", "narratives.csv"]
    assert sorted(env["s3"].objects) == sorted(f"{SBX}/scai/{n}.csv" for n in ("registry", "observations", "narratives"))
    assert env["s3"].objects[f"{SBX}/scai/registry.csv"].startswith(b"\xef\xbb\xbfindicator_id,name_en")
    assert (await st.get_dataset(SBX, ds["id"]))["kap"]["collection"] == "scai_ind"
    watch = pool.enqueued[-1]
    assert watch["kind"] == "pg_export_watch" and watch["run_after"] > now() + timedelta(seconds=5)

    # first poll: no KAP job yet → keep waiting
    out = await export.handle_pg_export_watch(pool, kap, settings(), job("pg_export_watch", run["id"]))
    assert out["status"] == "waiting_for_kap"
    run = await st.get_run(SBX, run["id"])
    assert states(run)["kap_scan"] == "running" and run["polls"] == 1 and "since upload" in run["steps"][3]["detail"]

    # an older job must be ignored; the new one is followed
    kap.jobs.append({"id": "old", "state": "succeeded", "created_at": (run["uploaded_at"] - timedelta(minutes=5)).isoformat()})
    kap.jobs.append({"id": "j1", "state": "running", "created_at": (run["uploaded_at"] + timedelta(seconds=40)).isoformat(), "progress": {"total": 3, "completed": 1, "percent": 33}})
    await export.handle_pg_export_watch(pool, kap, settings(), job("pg_export_watch", run["id"]))
    run = await st.get_run(SBX, run["id"])
    assert states(run)["kap_scan"] == "done" and states(run)["kap_load"] == "running"
    assert run["kap"]["pipeline_job_id"] == "j1" and run["steps"][4]["detail"] == "running: 1/3 files (33%)"

    kap.jobs[-1].update(state="succeeded", progress={"total": 3, "completed": 3, "percent": 100}, result={"succeeded": 3, "failed": 0, "cancelled": 0})
    kap.files = [{"path": f"{SBX}/scai/registry.csv", "state": "processed"}]
    out = await export.handle_pg_export_watch(pool, kap, settings(), job("pg_export_watch", run["id"]))
    assert out["status"] == "succeeded"
    run = await st.get_run(SBX, run["id"])
    assert run["status"] == "succeeded" and run["finished_at"] and states(run)["kap_load"] == "done"
    assert "registry.csv: processed" in run["steps"][4]["detail"]

    # same data again → KAP steps skipped, succeeds without waiting
    run2 = await export.start_run(pool, SBX, ds["id"], "manual")
    n_jobs = len(pool.enqueued)
    out = await export.handle_pg_export(pool, kap, settings(), job("pg_export", run2["id"]))
    run2 = await st.get_run(SBX, run2["id"])
    assert out["unchanged"] is True and run2["status"] == "succeeded"
    assert states(run2) == {"read": "done", "write": "done", "upload": "done", "kap_scan": "skipped", "kap_load": "skipped"}
    assert run2["steps"][3]["detail"] == "unchanged — KAP keeps the current version"
    assert len(pool.enqueued) == n_jobs  # no watcher


async def test_kap_job_failure_fails_the_run(env):
    st, pool, kap = env["store"], env["pool"], env["kap"]
    ds = await make_dataset(st)
    run = await export.start_run(pool, SBX, ds["id"], "manual")
    await export.handle_pg_export(pool, kap, settings(), job("pg_export", run["id"]))
    run = await st.get_run(SBX, run["id"])
    kap.jobs.append({"id": "j1", "state": "failed", "created_at": run["uploaded_at"].isoformat(), "result": {"succeeded": 0, "failed": 3, "cancelled": 0}, "error": {"code": "csv_invalid", "message": "registry.csv: bad header"}})
    out = await export.handle_pg_export_watch(pool, kap, settings(), job("pg_export_watch", run["id"]))
    run = await st.get_run(SBX, run["id"])
    assert out["status"] == "failed" and run["status"] == "failed"
    assert states(run)["kap_load"] == "failed" and "bad header" in run["error"]


async def test_no_kap_job_within_deadline_fails(env):
    st, pool, kap = env["store"], env["pool"], env["kap"]
    ds = await make_dataset(st)
    run = await export.start_run(pool, SBX, ds["id"], "manual")
    await export.handle_pg_export(pool, kap, settings(), job("pg_export", run["id"]))
    await st.update_run(run["id"], uploaded_at=now() - timedelta(minutes=16))
    await export.handle_pg_export_watch(pool, kap, settings(), job("pg_export_watch", run["id"]))
    run = await st.get_run(SBX, run["id"])
    assert run["status"] == "failed" and states(run)["kap_scan"] == "failed" and "within 15 min" in run["error"]


async def test_read_failure_marks_read_step_failed(env, monkeypatch):
    st, pool, kap = env["store"], env["pool"], env["kap"]
    ds = await make_dataset(st, preset="custom", queries={"registry": "select bogus from t", "observations": "select 1"})
    env["session"].queries = {"select bogus from t": (["bogus"], [])}
    run = await export.start_run(pool, SBX, ds["id"], "manual")
    out = await export.handle_pg_export(pool, kap, settings(), job("pg_export", run["id"]))
    run = await st.get_run(SBX, run["id"])
    assert out["status"] == "failed" and run["status"] == "failed"
    assert states(run) == {"read": "failed", "write": "pending", "upload": "pending", "kap_scan": "pending", "kap_load": "pending"}
    assert "unknown columns bogus" in run["error"] and env["s3"].objects == {}


async def test_custom_queries_export(env):
    st, pool, kap = env["store"], env["pool"], env["kap"]
    reg_sql, obs_sql = "select indicator_id, name_en, frequency from r", "select indicator_id, period, value from o"
    env["session"].queries = {reg_sql: (["indicator_id", "name_en", "frequency"], [("x.a.overall", "X", "A")]), obs_sql: (["indicator_id", "period", "value"], [("x.a.overall", "2019", 1.25)])}
    ds = await make_dataset(st, preset="custom", queries={"registry": reg_sql, "observations": obs_sql})
    run = await export.start_run(pool, SBX, ds["id"], "manual")
    await export.handle_pg_export(pool, kap, settings(), job("pg_export", run["id"]))
    run = await st.get_run(SBX, run["id"])
    assert run["status"] == "waiting_for_kap" and run["counts"] == {"registry": 1, "observations": 1, "narratives": 0}
    obs = env["s3"].objects[f"{SBX}/scai/observations.csv"].decode("utf-8-sig").split("\r\n")
    assert obs[1] == "x.a.overall,2019,,,1.25,,,,,,,"
    assert env["s3"].objects[f"{SBX}/scai/narratives.csv"].decode("utf-8-sig").strip() == "indicator_id,language,period,summary,details,additional_info,benchmark,source,snapshot_date"


async def test_schedule_tick_starts_due_datasets_once(env):
    st, pool = env["store"], env["pool"]
    ds = await make_dataset(st, schedule_minutes=60, next_run_at=now() - timedelta(seconds=1))
    assert await export.schedule_tick(pool, settings()) == 1
    runs, _ = await st.list_runs(SBX, ds["id"], 10, 0)
    assert runs[0]["trigger"] == "schedule" and pool.enqueued[-1]["kind"] == "pg_export"
    assert (await st.get_dataset(SBX, ds["id"]))["next_run_at"] > now() + timedelta(minutes=59)
    assert await export.schedule_tick(pool, settings()) == 0  # not due again
