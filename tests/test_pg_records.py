"""`records` preset: spec validation, generated SQL, record cards (.docx), keys, upload diff,
provisioning of the document collection, export runs and API endpoints (in-memory fakes)."""
import io
import re
import zipfile
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest
from cryptography.fernet import Fernet
from docx import Document
from fastapi.testclient import TestClient

from kap_platform import queue
from kap_platform.api.app import create_api
from kap_platform.config import Settings
from kap_platform.pgsource import build, export, provision, recorddoc, records
from kap_platform.pgsource.secrets import SecretBox

from pg_fakes import SBX, FakeKap, FakeQueuePool, FakeS3, FakeSession, MemoryStore, now, opener_for

KEY = Fernet.generate_key().decode()
AUTH = {"Authorization": "Bearer k1"}
BASE = f"/api/t/{SBX}"

COLUMNS = [
    {"name": "ticket_id", "type": "integer", "nullable": False},
    {"name": "subject", "type": "text", "nullable": True},
    {"name": "status", "type": "text", "nullable": True},
    {"name": "opened", "type": "date", "nullable": True},
    {"name": "amount", "type": "numeric", "nullable": True},
    {"name": "description", "type": "text", "nullable": True},
    {"name": "resolution", "type": "text", "nullable": True},
    {"name": "is_public", "type": "boolean", "nullable": True},
    {"name": "blob", "type": "bytea", "nullable": True},
]

SPEC = {
    "table": "public.tickets",
    "id_column": "ticket_id",
    "title": "Ticket {ticket_id}: {subject}",
    "fields": [{"column": "status", "label": "Status"}, {"column": "opened", "label": "Opened"}, {"column": "amount", "label": "Amount"}],
    "sections": [{"column": "description", "label": "Description"}, {"column": "resolution", "label": "Resolution"}],
    "filters": [{"column": "is_public", "op": "is_true"}, {"column": "status", "op": "!=", "value": "spam"}],
    "order_by": {"column": "opened", "desc": True},
}

ROWS = [
    {"ticket_id": 42, "subject": "Printer jam", "status": "open", "opened": date(2026, 11, 3), "amount": Decimal("12.50"),
     "description": "Paper stuck in tray 2.\n\nHappens every morning.\nSince Monday.", "resolution": None},
    {"ticket_id": 43, "subject": "VPN", "status": "closed", "opened": date(2026, 11, 2), "amount": None,
     "description": "Cannot connect.", "resolution": "Reset the token."},
    {"ticket_id": 44, "subject": "  ", "status": "", "opened": None, "amount": Decimal("0"), "description": None, "resolution": None},
]


# ---------------------------------------------------------------- fakes
class RecordsSession(FakeSession):
    """Answers the generated records SQL from `rows` (filters/order are not simulated; the SQL text is tested separately)."""

    def __init__(self, rows=ROWS, table="public.tickets", columns=COLUMNS):
        super().__init__()
        self.rows = rows
        self.readable = [{"schema": table.split(".")[0], "name": table.split(".")[1], "rows_estimate": len(rows)}]
        self.columns_by_table = {table: columns}

    async def fetch(self, sql, *args, savepoint=False):
        self.fetched.append((sql, args))
        if "count(*)" in sql:
            return [{"n": len(self.rows)}]
        cols = {int(i): c for c, i in re.findall(r'"([^"]+)" as c(\d+)', sql)}
        limit = int(re.search(r"limit (\d+)$", sql).group(1))
        return [{f"c{i}": r.get(c) for i, c in cols.items()} for r in self.rows[:limit]]


def settings(**kw):
    base = dict(tenants_file="none.yaml", secret_key=KEY, s3_access_key="ak", s3_secret_key="sk", s3_bucket="platform-exports", s3_url="http://s3:9000")
    return Settings(**{**base, **kw})


def card(**kw):
    base = dict(id="42", slug="42", title="Ticket 42: Printer jam", fields=[("Status", "open"), ("Opened", "2026-11-03")],
                sections=[("Description", "Paper stuck in tray 2.\n\nHappens every morning.\nSince Monday.")], source="public.tickets")
    return records.Card(**{**base, **kw})


def docx_paragraphs(data: bytes):
    d = Document(io.BytesIO(data))
    return [(p.style.name, p.text) for p in d.paragraphs]


# ---------------------------------------------------------------- validation
def test_normalize_fills_defaults_and_reports_problems():
    r, problems = records.normalize(SPEC)
    assert problems == [] and r["max_records"] == 5000 and r["language"] == "en"
    assert r["fields"][0] == {"column": "status", "label": "Status"}
    assert r["filters"] == [{"column": "is_public", "op": "is_true"}, {"column": "status", "op": "!=", "value": "spam"}]

    r, _ = records.normalize({**SPEC, "fields": [{"column": "status"}], "sections": [], "max_records": 50000, "language": "ar"})
    assert r["fields"] == [{"column": "status", "label": "status"}] and r["max_records"] == 50000 and r["language"] == "ar"

    bad = {"table": "tickets", "id_column": "", "title": " ", "fields": [{"label": "x"}], "sections": "no", "order_by": {"column": "a", "desc": "yes"},
           "max_records": 50001, "language": "fr", "filters": [{"column": "a", "op": "like"}], "extra": 1}
    r, problems = records.normalize(bad)
    assert r is None
    text = "\n".join(problems)
    for needle in ('unknown field "extra"', "must be written schema.name", "id_column: choose a column", "title: enter a title",
                   "fields: item 1: choose a column", "sections: must be a list", "desc must be true or false",
                   "max_records: a whole number from 1 to 50000", 'language: "en" or "ar"', "unknown operator 'like'"):
        assert needle in text, needle
    assert records.normalize({**SPEC, "fields": [], "sections": []})[1] == ["fields / sections: choose at least one column to show in the document"]
    assert records.normalize({**SPEC, "max_records": True})[0] is None
    assert records.normalize("x") == (None, ["records: an object is required"])


def test_check_columns():
    r, _ = records.normalize(SPEC)
    assert records.check_columns(r, COLUMNS) == []
    r2, _ = records.normalize({**SPEC, "id_column": "blob", "title": "T {nope}", "sections": [{"column": "ghost"}], "filters": [{"column": "status", "op": "is_true"}]})
    problems = records.check_columns(r2, COLUMNS)
    assert 'id_column: column "blob" is bytea' in problems[0]
    assert 'title: column "nope" does not exist in public.tickets' in problems
    assert 'sections: column "ghost" does not exist in public.tickets' in problems
    assert any("is_true needs a boolean column" in p for p in problems)


def test_build_sql_quotes_identifiers_and_parameterises_filters():
    r, _ = records.normalize({**SPEC, "max_records": 100})
    sql = records.build_sql(r, "public", 'tick"ets')
    assert sql.columns == ["ticket_id", "subject", "status", "opened", "amount", "description", "resolution"]
    assert sql.select == (
        'select "ticket_id" as c0,\n       "subject" as c1,\n       "status" as c2,\n       "opened" as c3,\n       "amount" as c4,\n'
        '       "description" as c5,\n       "resolution" as c6\n  from "public"."tick""ets"\n where "is_public" is true\n'
        '   and "status"::text is distinct from $1\n order by "opened" desc nulls last, "ticket_id"\n limit 101'
    )
    assert sql.args == ["spam"]
    assert sql.count == 'select count(*) as n\n  from "public"."tick""ets"\n where "is_public" is true\n   and "status"::text is distinct from $1'
    assert sql.sample(5).endswith("\n limit 5")
    plain, _ = records.normalize({**SPEC, "filters": [], "order_by": None})
    s = records.build_sql(plain, "public", "tickets")
    assert "where" not in s.select and s.select.endswith('order by "ticket_id"\n limit 5001') and s.args == []
    # user text never reaches the SQL
    evil, _ = records.normalize({**SPEC, "filters": [{"column": "status", "op": "=", "value": "x'; drop table t; --"}]})
    s = records.build_sql(evil, "public", "tickets")
    assert "drop" not in s.select and s.args == ["x'; drop table t; --"]


# ---------------------------------------------------------------- cards
def test_values_titles_and_cards():
    assert records.format_value(date(2026, 1, 2)) == "2026-01-02"
    assert records.format_value(datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)) == "2026-01-02T03:04:00+00:00"
    assert records.format_value(Decimal("1.50")) == "1.50" and records.format_value(3) == "3" and records.format_value(0.25) == "0.25"
    assert records.format_value(True) == "Yes" and records.format_value(float("nan")) is None and records.format_value("  ") is None
    assert records.format_value(["a", None, "b"]) == "a, b" and records.format_value({"b": 1, "a": "x"}) == '{"a": "x", "b": 1}'
    assert records.format_value("bad\x00\x07char") == "badchar" and records.format_value(b"\x00") is None

    r, _ = records.normalize(SPEC)
    cols = records.used_columns(r)
    rows = [{f"c{i}": row.get(c) for i, c in enumerate(cols)} for row in ROWS]
    cards, problems = records.cards_from_rows(r, rows, cols)
    assert problems == [] and [c.id for c in cards] == ["42", "43", "44"]
    c42, c43, c44 = cards
    assert c42.title == "Ticket 42: Printer jam" and c42.fields == [("Status", "open"), ("Opened", "2026-11-03"), ("Amount", "12.50")]
    assert c42.sections == [("Description", "Paper stuck in tray 2.\n\nHappens every morning.\nSince Monday.")]  # empty resolution omitted
    assert c43.fields == [("Status", "closed"), ("Opened", "2026-11-02")] and [s[0] for s in c43.sections] == ["Description", "Resolution"]
    assert c44.title == "Ticket 44:" and c44.fields == [("Amount", "0")] and c44.sections == []
    assert c42.path == "42/record.docx"
    assert c42.text() == (
        "# Ticket 42: Printer jam\n\nStatus: open\nOpened: 2026-11-03\nAmount: 12.50\n\n## Description\n\n"
        "Paper stuck in tray 2.\n\nHappens every morning.\nSince Monday.\n\nSource: public.tickets · record 42\n"
    )

    # empty ids and repeated ids are left out
    dup = rows + [dict(rows[0]), {**rows[1], "c0": None}]
    cards, problems = records.cards_from_rows(r, dup, cols)
    assert len(cards) == 3 and any("1 rows with an empty ticket_id" in p for p in problems) and any("repeat an ticket_id" in p for p in problems)
    # title falls back to "Record <id>"
    r2, _ = records.normalize({**SPEC, "title": "{subject}"})
    cards, _ = records.cards_from_rows(r2, [rows[2]], cols)
    assert cards[0].title == "Record 44"


def test_record_slugs_are_safe_and_distinct():
    assert records.record_slug("42") == "42" and records.record_slug("abc_def") == "abc_def"
    up = records.record_slug("ABC")
    assert up.startswith("abc_") and len(up) == 12 and up != records.record_slug("abc")
    assert records.record_slug("a/b") != records.record_slug("a b") and records.record_slug("a/b").startswith("a_b_")
    assert re.fullmatch(r"id_[0-9a-f]{8}", records.record_slug("—"))
    long_a, long_b = "x" * 100 + "a", "x" * 100 + "b"
    assert len(records.record_slug(long_a)) == 64 and records.record_slug(long_a) != records.record_slug(long_b)
    assert records.record_slug("x" * 64) == "x" * 64
    ids = ["Ticket #1", "ticket 1", "TICKET-1", "ticket_1", "ticket__1", "../etc", "ü", "Ü"]
    slugs = [records.record_slug(i) for i in ids]
    assert len(set(slugs)) == len(ids) and all(re.fullmatch(r"[a-z0-9_]{1,64}", s) for s in slugs)
    # a crafted id equal to another id's hashed slug still gets its own folder
    r, _ = records.normalize({**SPEC, "fields": [{"column": "status"}], "sections": []})
    crafted = records.record_slug("A")
    cards, _ = records.cards_from_rows(r, [{"c0": "A", "c1": None, "c2": "x"}, {"c0": crafted, "c1": None, "c2": "y"}], ["ticket_id", "subject", "status"])
    assert len({c.slug for c in cards}) == 2


# ---------------------------------------------------------------- .docx
def test_docx_content():
    data = recorddoc.render(card())
    paras = docx_paragraphs(data)
    assert paras[0] == ("Heading 1", "Ticket 42: Printer jam")
    assert ("Normal", "Status: open") in paras and ("Normal", "Opened: 2026-11-03") in paras
    assert ("Heading 2", "Description") in paras
    i = paras.index(("Heading 2", "Description"))
    assert paras[i + 1] == ("Normal", "Paper stuck in tray 2.") and paras[i + 2] == ("Normal", "Happens every morning.\nSince Monday.")
    assert paras[-1] == ("Normal", "Source: public.tickets · record 42")
    d = Document(io.BytesIO(data))
    assert d.paragraphs[1].runs[0].bold is True and d.paragraphs[1].runs[0].text == "Status: "
    assert d.core_properties.title == "Ticket 42: Printer jam" and d.core_properties.revision == 1
    assert d.core_properties.modified == recorddoc.FIXED_TIME and d.core_properties.created == recorddoc.FIXED_TIME and d.core_properties.last_printed == recorddoc.FIXED_TIME
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        names = z.namelist()
        assert names[0] == "[Content_Types].xml" and names[1] == "_rels/.rels"
        assert all(i.date_time == (1980, 1, 1, 0, 0, 0) for i in z.infolist())
    assert "w:bidi" not in zipfile.ZipFile(io.BytesIO(data)).read("word/document.xml").decode()


def test_docx_is_deterministic_and_changes_with_content():
    a, b = recorddoc.render(card()), recorddoc.render(card())
    assert a == b
    assert recorddoc.digest(a) == recorddoc.digest(b)
    assert recorddoc.render(card(fields=[("Status", "closed")])) != a
    d = recorddoc.digest(a)
    assert len(d.sha256) == 64 and len(d.md5) == 32 and d.bytes == len(a)
    # change detection without rendering
    h = recorddoc.content_hash(card())
    assert h == recorddoc.content_hash(card()) and h != recorddoc.content_hash(card(language="ar"))
    assert h != recorddoc.content_hash(card(sections=[("Description", "other")]))
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(recorddoc, "RENDER_VERSION", recorddoc.RENDER_VERSION + 1)
        assert recorddoc.content_hash(card()) != h


def test_docx_arabic_is_rtl():
    data = recorddoc.render(card(title="تذكرة ٤٢", fields=[("الحالة", "مفتوحة")], sections=[], language="ar"))
    xml = zipfile.ZipFile(io.BytesIO(data)).read("word/document.xml").decode()
    assert xml.count("<w:bidi/>") == 3 and "<w:rtl/>" in xml
    assert docx_paragraphs(data)[0] == ("Heading 1", "تذكرة ٤٢")
    assert Document(io.BytesIO(data)).core_properties.language == "ar"


# ---------------------------------------------------------------- upload diff
def test_plan_upload_new_changed_removed_unchanged():
    prefix = f"{SBX}/tickets/"
    keys = [f"{prefix}a/record.docx", f"{prefix}b/record.docx", f"{prefix}c/record.docx", f"{prefix}d/record.docx", f"{prefix}e/record.docx"]
    paths = [k[len(prefix):] for k in keys]
    hashes = [f"h{i}" for i in range(5)]
    existing = {keys[1]: "x", keys[2]: "x", keys[3]: "md53", keys[4]: "zzz-2",
                f"{prefix}gone/record.docx": "x", f"{prefix}registry.csv": "x", f"{prefix}a/b/record.docx": "x", f"{prefix}x/other.docx": "x"}
    previous = {paths[1]: "h1", paths[2]: "old"}
    rendered: list[int] = []

    def md5_of(i):
        rendered.append(i)
        return f"md5{i}"

    plan = export.plan_upload(keys, paths, hashes, existing, previous, prefix, md5_of)
    assert rendered == [3, 4]  # only documents without a stored hash are rendered for the ETag check
    assert plan.new == [0]
    assert plan.unchanged == [1, 3]  # stored hash equal; no stored hash but ETag = MD5
    assert plan.changed == [2, 4]  # stored hash differs; multipart-style ETag never equals an MD5
    assert plan.removed == [f"{prefix}gone/record.docx"]  # other objects under the prefix are left alone
    assert plan.counts(5) == {"documents": 5, "new": 1, "changed": 2, "removed": 1, "unchanged": 2}
    assert not plan.nothing_to_do and plan.upload == [0, 2, 4]


# ---------------------------------------------------------------- provisioning
def test_document_pipeline_connector_and_collection_bodies():
    c = provision.connector_body("tickets_c_pg", SBX, "tickets", settings(), records=True)
    assert c["settings"]["object_selector"] == {"prefix": f"{SBX}/tickets/", "include_globs": ["**/*.docx"], "exclude_globs": []}
    assert c["settings"]["sync"] == {"interval": "PT1M"}
    p = provision.document_pipeline_body("tickets_c_pipeline", "chat_x", "embed_y")
    assert p["kind"] == "document" and [s["type"] for s in p["stages"]] == ["parser", "transformer", "embedding"]
    assert p["stages"][0]["routes"] == [{"match": {"extensions": ["docx"]}, "parser": {"provider": "docx", "output": "markdown", "config": {"preserve_headings": True, "preserve_lists": True, "preserve_tables": True}}}]
    tr = p["stages"][1]
    assert tr["name"] == "summarize_document" and tr["config"]["model_ref"] == "chat_x"
    task = tr["config"]["tasks"][0]
    assert task["name"] == "summary" and task["enabled"] is True and set(task["config"]) == {"initial_prompt", "refine_prompt", "merge_prompt"}
    assert task["config"]["initial_prompt"].startswith("Summarise this document in Markdown. Start with its reference and title.")
    assert p["stages"][2]["config"] == {"model_ref": "embed_y", "chunking": {"chunk_size": 800, "overlap": 120}}
    col = provision.records_collection_body("tickets_c", "tickets_c_pipeline", "tickets_c_pg", "indicator_document_context", "chat_x")
    assert col["pipeline_ref"] == "tickets_c_pipeline" and col["connectors"] == ["tickets_c_pg"] and col["sync_strategy"] == "overwrite"
    e = col["entity"]
    assert e["enabled"] is True and e["entity_type_ref"] == "indicator_document_context" and e["metadata"] is None
    assert e["boundary"] == {"strategy": "folder", "collection_root": "/", "folder_depth": 1, "recursive": False}
    assert e["identity"] == {"entity_id_template": "{collection_id}:{relative_folder_path}"}
    assert e["summary"]["model_ref"] == "chat_x" and e["summary"]["initial_prompt"].startswith("Combine the document summaries into one Markdown text")


async def test_ensure_records_creates_entity_type_and_document_objects():
    kap = FakeKap()
    ds = {"name": "tickets", "collection_name": "tickets_c", "preset": "records", "kap": None}
    info = await provision.ensure(kap, SBX, ds, settings())
    assert [c[:2] for c in kap.calls] == [("POST", "/api/connectors"), ("POST", "/api/pipelines"), ("POST", "/api/entity-types"), ("POST", "/api/collections")]
    assert kap.calls[2][2]["name"] == "document_group" and kap.calls[2][2]["schema"] == {"type": "object", "properties": {}}
    assert kap.calls[1][2]["kind"] == "document" and kap.calls[3][2]["entity"]["entity_type_ref"] == "document_group"
    assert info["kind"] == "document" and info["collection_id"]
    kap.calls.clear()
    assert (await provision.ensure(kap, SBX, {**ds, "kap": info}, settings()))["collection_id"] == info["collection_id"] and kap.calls == []

    # the preferred type is used when the tenant has it
    kap2 = FakeKap()
    kap2.objects["entity-types"] = [{"id": "t1", "name": "indicator_document_context"}]
    await provision.ensure(kap2, SBX, ds, settings())
    assert [c[1] for c in kap2.calls] == ["/api/connectors", "/api/pipelines", "/api/collections"]
    assert kap2.calls[2][2]["entity"]["entity_type_ref"] == "indicator_document_context"

    # an indicators pipeline under the same name is not reused for records
    kap3 = FakeKap()
    kap3.objects["pipelines"] = [{"id": "p", "name": "tickets_c_pipeline", "kind": "indicators"}]
    with pytest.raises(provision.ProvisionError, match="needs a document pipeline"):
        await provision.ensure(kap3, SBX, ds, settings())


# ---------------------------------------------------------------- export runs
@pytest.fixture
def env(monkeypatch):
    db: dict = {}
    s3 = FakeS3()
    session = RecordsSession()
    monkeypatch.setattr(export, "SERVICES", export.Services(store_factory=lambda pool: MemoryStore(db), s3_factory=lambda st: s3, opener=opener_for(session)))
    return {"db": db, "s3": s3, "session": session, "store": MemoryStore(db), "pool": FakeQueuePool(), "kap": FakeKap()}


async def make_records_dataset(st, spec=SPEC):
    src = await st.create_source(SBX, {"name": "helpdesk", "host": "db", "database": "hd", "username": "ro", "password_enc": SecretBox(KEY).encrypt("pw")})
    r, _ = records.normalize(spec)
    return await st.create_dataset(SBX, {"source_id": src["id"], "name": "tickets", "preset": "records", "records": r, "collection_name": "tickets_c"})


def job(kind, run_id):
    return queue.Job(id=None, tenant_id=SBX, kind=kind, payload={"run_id": run_id}, attempts=1, max_attempts=2)


def states(run):
    return {s["key"]: s["state"] for s in run["steps"]}


def prefix():
    return f"{SBX}/tickets/"


async def run_export(env, ds):
    run = await export.start_run(env["pool"], SBX, ds["id"], "manual")
    out = await export.handle_pg_export(env["pool"], env["kap"], settings(), job("pg_export", run["id"]))
    return out, await env["store"].get_run(SBX, run["id"])


async def finish_run(env, run):
    """Let a KAP job succeed and the watcher see it."""
    env["kap"].jobs.append({"id": f"j{len(env['kap'].jobs)}", "state": "succeeded", "created_at": (run["uploaded_at"] + timedelta(seconds=30)).isoformat(),
                            "progress": {"total": 1, "completed": 1, "percent": 100}, "result": {"succeeded": 1, "failed": 0, "cancelled": 0}})
    return await export.handle_pg_export_watch(env["pool"], env["kap"], settings(), job("pg_export_watch", run["id"]))


async def test_records_export_uploads_one_folder_per_record_then_diffs(env, monkeypatch):
    st, s3, pool = env["store"], env["s3"], env["pool"]
    ds = await make_records_dataset(st)
    run = await export.start_run(pool, SBX, ds["id"], "manual")
    assert [s["title"] for s in run["steps"]][1:] == ["Write record documents", "Upload to S3", "KAP picks up the documents", "KAP summarises and indexes the records"]

    out = await export.handle_pg_export(pool, env["kap"], settings(), job("pg_export", run["id"]))
    run = await st.get_run(SBX, run["id"])
    assert out["status"] == "waiting_for_kap" and run["status"] == "waiting_for_kap"
    assert states(run) == {"read": "done", "write": "done", "upload": "done", "kap_scan": "running", "kap_load": "pending"}
    assert run["counts"] == {"documents": 3, "new": 3, "changed": 0, "removed": 0, "unchanged": 0}
    assert sorted(s3.objects) == [f"{prefix()}{i}/record.docx" for i in (42, 43, 44)]
    assert set(s3.types.values()) == {records.DOCX_TYPE}
    assert [f["name"] for f in run["files"]] == ["42/record.docx", "43/record.docx", "44/record.docx"] and run["files"][0]["key"] == f"{prefix()}42/record.docx"
    assert docx_paragraphs(s3.objects[f"{prefix()}42/record.docx"])[0] == ("Heading 1", "Ticket 42: Printer jam")
    assert set(run["file_hashes"]) == {"42/record.docx", "43/record.docx", "44/record.docx"}
    kap_ds = (await st.get_dataset(SBX, ds["id"]))["kap"]
    assert kap_ds["kind"] == "document" and kap_ds["connector"] == "tickets_c_pg"
    assert run["kap"]["upload_started_at"]
    sql, args = env["session"].fetched[-1]
    assert '"is_public" is true' in sql and args == ("spam",)
    assert await finish_run(env, run) == {"status": "succeeded", "kap_job": "j0"}

    # same rows → nothing rendered or uploaded, KAP steps skipped
    s3.puts.clear()
    renders = []
    real_render = recorddoc.render
    monkeypatch.setattr(recorddoc, "render", lambda c: renders.append(c.id) or real_render(c))
    out, run2 = await run_export(env, ds)
    assert renders == []
    assert out["unchanged"] is True and run2["status"] == "succeeded" and s3.puts == []
    assert states(run2)["kap_scan"] == "skipped" and states(run2)["kap_load"] == "skipped"
    assert run2["counts"] == {"documents": 3, "new": 0, "changed": 0, "removed": 0, "unchanged": 3} and run2["files"] == []

    # one changed, one removed, one new
    env["session"].rows = [{**ROWS[0], "status": "closed"}, ROWS[1], {**ROWS[1], "ticket_id": 45}]
    out, run3 = await run_export(env, ds)
    assert run3["status"] == "waiting_for_kap"
    assert run3["counts"] == {"documents": 3, "new": 1, "changed": 1, "removed": 1, "unchanged": 1}
    assert sorted(s3.puts) == [f"{prefix()}42/record.docx", f"{prefix()}45/record.docx"] and s3.deletes == [f"{prefix()}44/record.docx"]
    assert sorted(s3.objects) == [f"{prefix()}{i}/record.docx" for i in (42, 43, 45)]
    assert "Status: closed" in [p[1] for p in docx_paragraphs(s3.objects[f"{prefix()}42/record.docx"])]
    assert "1 new, 1 changed, 1 removed, 1 unchanged" in run3["steps"][2]["detail"]


async def test_records_export_without_stored_hashes_uses_etags(env):
    """A failed previous run (no hashes stored): objects whose ETag is their MD5 are not uploaded again."""
    st, s3 = env["store"], env["s3"]
    ds = await make_records_dataset(st)
    _, run = await run_export(env, ds)
    env["db"]["runs"][run["id"]]["file_hashes"] = None
    env["db"]["runs"][run["id"]]["status"] = "failed"
    s3.etags[f"{prefix()}43/record.docx"] = "abc-2"  # multipart ETag: unknown content → uploaded again
    s3.puts.clear()
    _, run2 = await run_export(env, ds)
    assert run2["counts"]["unchanged"] == 2 and run2["counts"]["changed"] == 1 and s3.puts == [f"{prefix()}43/record.docx"]


async def test_records_watch_follows_all_jobs_and_backs_off(env, monkeypatch):
    st, pool, kap = env["store"], env["pool"], env["kap"]
    ds = await make_records_dataset(st)
    _, run = await run_export(env, ds)
    started = datetime.fromisoformat(run["kap"]["upload_started_at"])
    up = run["uploaded_at"]
    assert pool.enqueued[-1]["run_after"] < now() + timedelta(seconds=15)

    # a scan during the upload (job a) and one after it (job b)
    kap.jobs.append({"id": "old", "state": "succeeded", "created_at": (started - timedelta(minutes=5)).isoformat()})
    kap.jobs.append({"id": "a", "state": "succeeded", "created_at": started.isoformat(), "progress": {"total": 1, "completed": 1}, "result": {"succeeded": 1, "failed": 0, "cancelled": 0}})
    kap.jobs.append({"id": "b", "state": "running", "created_at": (up + timedelta(seconds=20)).isoformat(), "progress": {"total": 2, "completed": 0}})
    out = await export.handle_pg_export_watch(pool, kap, settings(), job("pg_export_watch", run["id"]))
    run = await st.get_run(SBX, run["id"])
    assert out["status"] == "waiting_for_kap" and states(run)["kap_scan"] == "done" and states(run)["kap_load"] == "running"
    assert run["kap"]["state"] == "running" and run["kap"]["progress"] == {"total": 3, "completed": 1, "percent": 33}
    assert "1/3 documents (33%) (2 KAP jobs)" in run["steps"][4]["detail"] and "summarises each new or changed record" in run["steps"][4]["detail"]

    kap.jobs[-1].update(state="succeeded", progress={"total": 2, "completed": 2}, result={"succeeded": 2, "failed": 0, "cancelled": 0})
    out = await export.handle_pg_export_watch(pool, kap, settings(), job("pg_export_watch", run["id"]))
    run = await st.get_run(SBX, run["id"])
    assert out == {"status": "succeeded", "kap_job": "b"} and run["status"] == "succeeded"
    assert run["kap"]["result"] == {"succeeded": 3, "failed": 0, "cancelled": 0} and "succeeded 3" in run["steps"][4]["detail"]

    # backoff: 10 s for the first minute of polls, up to 60 s
    s = settings()
    assert [export.watch_delay(s, p, True) for p in (0, 5, 6, 12, 100)] == [10, 10, 20, 30, 60]
    assert export.watch_delay(s, 100) == 10


async def test_records_watch_waits_for_a_scan_after_the_upload_and_reports_failures(env):
    st, pool, kap = env["store"], env["pool"], env["kap"]
    ds = await make_records_dataset(st)
    _, run = await run_export(env, ds)
    # the upload ran from t-100 s to t-10 s; the only job started during it and has finished
    t = now()
    await st.update_run(run["id"], uploaded_at=t - timedelta(seconds=10), kap={**run["kap"], "upload_started_at": (t - timedelta(seconds=100)).isoformat()})
    kap.jobs.append({"id": "a", "state": "succeeded", "created_at": (t - timedelta(seconds=50)).isoformat(), "result": {"succeeded": 1, "failed": 0}})
    out = await export.handle_pg_export_watch(pool, kap, settings(), job("pg_export_watch", run["id"]))
    run = await st.get_run(SBX, run["id"])
    assert out["status"] == "waiting_for_kap" and "next scan" in run["steps"][4]["detail"]
    # no newer job within the settle time (180 s): done
    await st.update_run(run["id"], uploaded_at=t - timedelta(seconds=200), kap={**run["kap"], "upload_started_at": (t - timedelta(seconds=300)).isoformat()})
    kap.jobs[0]["created_at"] = (t - timedelta(seconds=250)).isoformat()
    out = await export.handle_pg_export_watch(pool, kap, settings(), job("pg_export_watch", run["id"]))
    assert out["status"] == "succeeded"

    # a job with failed documents fails the run
    env["session"].rows = ROWS[:1]
    _, run = await run_export(env, ds)
    kap.jobs.append({"id": "f", "state": "succeeded", "created_at": (run["uploaded_at"] + timedelta(seconds=5)).isoformat(), "result": {"succeeded": 0, "failed": 1, "cancelled": 0}})
    out = await export.handle_pg_export_watch(pool, kap, settings(), job("pg_export_watch", run["id"]))
    run = await st.get_run(SBX, run["id"])
    assert out["status"] == "failed" and states(run)["kap_load"] == "failed" and "some documents failed" in run["error"]


async def test_records_watch_deadlines(env):
    st, pool, kap = env["store"], env["pool"], env["kap"]
    ds = await make_records_dataset(st)
    _, run = await run_export(env, ds)
    # no job 16 min after the upload → scan failed (15 min, like indicators)
    await st.update_run(run["id"], uploaded_at=now() - timedelta(minutes=16), kap={**run["kap"], "upload_started_at": (now() - timedelta(minutes=17)).isoformat()})
    await export.handle_pg_export_watch(pool, kap, settings(), job("pg_export_watch", run["id"]))
    run = await st.get_run(SBX, run["id"])
    assert run["status"] == "failed" and "within 15 min" in run["error"]

    # a running job is followed for up to 60 min
    env["session"].rows = ROWS[:2]
    _, run = await run_export(env, ds)
    kap.jobs.append({"id": "slow", "state": "running", "created_at": (now() - timedelta(minutes=30)).isoformat()})
    await st.update_run(run["id"], uploaded_at=now() - timedelta(minutes=31), kap={**run["kap"], "upload_started_at": (now() - timedelta(minutes=32)).isoformat()})
    out = await export.handle_pg_export_watch(pool, kap, settings(), job("pg_export_watch", run["id"]))
    assert out["status"] == "waiting_for_kap"
    await st.update_run(run["id"], uploaded_at=now() - timedelta(minutes=61))
    out = await export.handle_pg_export_watch(pool, kap, settings(), job("pg_export_watch", run["id"]))
    run = await st.get_run(SBX, run["id"])
    assert out["status"] == "failed" and "after 60 min" in run["error"]


async def test_records_export_with_bad_spec_fails_read(env):
    st = env["store"]
    ds = await make_records_dataset(st, {**SPEC, "sections": [{"column": "ghost"}]})
    out, run = await run_export(env, ds)
    assert out["status"] == "failed" and states(run)["read"] == "failed" and 'column "ghost" does not exist' in run["error"] and env["s3"].objects == {}


async def test_truncation_is_reported(env):
    env["session"].rows = ROWS
    r, _ = records.normalize({**SPEC, "max_records": 2})
    rr = await records.read(env["session"], r)
    assert len(rr.cards) == 2 and "only the first 2" in rr.problems[0]
    pv = await build.preview_records(env["session"], r, 1)
    assert pv["documents"]["count"] == 2 and len(pv["documents"]["sample"]) == 1 and "3 rows match but max_records is 2" in pv["problems"][0]


# ---------------------------------------------------------------- API
@pytest.fixture
def api(monkeypatch):
    db: dict = {}
    session = RecordsSession()
    monkeypatch.setattr(export, "SERVICES", export.Services(store_factory=lambda pool: MemoryStore(db), s3_factory=lambda st: FakeS3(), opener=opener_for(session)))
    pool = FakeQueuePool()

    async def pool_factory():
        return pool

    app = create_api(Settings(api_keys=["k1"], tenants_file="none.yaml", secret_key=KEY), pool_factory=pool_factory, kap_factory=lambda: FakeKap())
    with TestClient(app) as c:
        sid = c.post(f"{BASE}/pg-sources", json={"name": "helpdesk", "host": "db", "database": "hd", "username": "ro", "password": "pw"}, headers=AUTH).json()["id"]
        yield {"c": c, "sid": sid, "db": db, "session": session}


def test_records_preview_endpoint(api):
    c, sid = api["c"], api["sid"]
    r = c.post(f"{BASE}/pg-sources/{sid}/records-preview", json={"records": SPEC, "name": "tickets"}, headers=AUTH)
    assert r.status_code == 200
    body = r.json()
    assert set(body) == {"documents", "problems", "query"} and body["problems"] == []
    assert body["documents"]["count"] == 3 and len(body["documents"]["sample"]) == 3
    first = body["documents"]["sample"][0]
    assert set(first) == {"key", "title", "text"} and first["key"] == "42/record.docx" and first["title"] == "Ticket 42: Printer jam"
    assert first["text"].startswith("# Ticket 42: Printer jam\n\nStatus: open") and body["query"].startswith('select "ticket_id" as c0')

    r = c.post(f"{BASE}/pg-sources/{sid}/records-preview", json={"records": {**SPEC, "id_column": "nope"}}, headers=AUTH).json()
    assert r["documents"] == {"count": 0, "sample": []} and r["query"] is None and 'column "nope" does not exist' in r["problems"][0]
    assert c.post(f"{BASE}/pg-sources/{sid}/records-preview", json={"records": SPEC, "limit": 6}, headers=AUTH).status_code == 422
    assert len(c.post(f"{BASE}/pg-sources/{sid}/records-preview", json={"records": SPEC, "limit": 2}, headers=AUTH).json()["documents"]["sample"]) == 2


def test_records_dataset_lifecycle(api):
    c, sid = api["c"], api["sid"]
    body = {"source_id": sid, "name": "tickets", "preset": "records", "records": SPEC, "collection_name": "tickets_c"}
    assert c.post(f"{BASE}/pg-datasets", json={**body, "records": None}, headers=AUTH).json()["type"] == "validation_error"
    r = c.post(f"{BASE}/pg-datasets", json={**body, "records": {**SPEC, "max_records": 0}}, headers=AUTH)
    assert r.status_code == 422 and any("max_records" in p for p in r.json()["data"]["problems"])
    r = c.post(f"{BASE}/pg-datasets", json=body, headers=AUTH)
    assert r.status_code == 201
    ds = r.json()
    assert ds["preset"] == "records" and ds["mapping"] is None and ds["queries"] == {}
    assert ds["records"]["max_records"] == 5000 and ds["records"]["language"] == "en" and ds["records"]["table"] == "public.tickets"

    pv = c.post(f"{BASE}/pg-datasets/{ds['id']}/preview", headers=AUTH).json()
    assert set(pv) == {"documents", "problems", "query"} and pv["documents"]["count"] == 3 and len(pv["documents"]["sample"]) == 3

    run = c.post(f"{BASE}/pg-datasets/{ds['id']}/run", headers=AUTH).json()
    assert run["counts"] == {"documents": None, "new": None, "changed": None, "removed": None, "unchanged": None}
    assert run["steps"][4]["title"] == "KAP summarises and indexes the records" and run["files"] == []

    # patch the spec; switching to an indicator preset drops it and resets the KAP objects
    api["db"]["datasets"][ds["id"]]["kap"] = {"collection": "tickets_c", "kind": "document"}
    r = c.patch(f"{BASE}/pg-datasets/{ds['id']}", json={"records": {**SPEC, "language": "ar"}}, headers=AUTH).json()
    assert r["records"]["language"] == "ar" and r["kap"] is not None
    r = c.patch(f"{BASE}/pg-datasets/{ds['id']}", json={"preset": "scai_indicators"}, headers=AUTH).json()
    assert r["preset"] == "scai_indicators" and r["records"] is None and r["kap"] is None
    assert c.patch(f"{BASE}/pg-datasets/{ds['id']}", json={"preset": "records"}, headers=AUTH).status_code == 422  # records spec required again
