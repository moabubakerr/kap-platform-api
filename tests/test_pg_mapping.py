"""`mapped` preset: mapping validation, generated SQL, ids, row mapping, and its API endpoints (fakes)."""
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

from kap_platform.api.app import create_api
from kap_platform.config import Settings
from kap_platform.pgsource import build, export, mapping
from kap_platform.pgsource.kapcsv import HEADERS

from pg_fakes import SBX, FakeKap, FakeQueuePool, FakeS3, FakeSession, MemoryStore, opener_for

COLUMNS = [
    {"name": "indicator", "type": "text", "nullable": False},
    {"name": "country", "type": "character varying", "nullable": True},
    {"name": "period_date", "type": "date", "nullable": True},
    {"name": "month", "type": "text", "nullable": True},
    {"name": "year", "type": "integer", "nullable": True},
    {"name": "freq", "type": "text", "nullable": True},
    {"name": "val", "type": "numeric", "nullable": True},
    {"name": "tgt", "type": "double precision", "nullable": True},
    {"name": "name", "type": "text", "nullable": True},
    {"name": "unit", "type": "text", "nullable": True},
    {"name": "is_published", "type": "boolean", "nullable": True},
    {"name": "status", "type": "text", "nullable": True},
]
BASE_MAPPING = {
    "table": "stats.kpi",
    "series_key": ["indicator"],
    "period": {"column": "period_date", "kind": "date"},
    "frequency": {"value": "A"},
    "value": "val",
}


def norm(raw, prefix="kpi"):
    m, problems = mapping.normalize(raw, prefix)
    assert problems == [], problems
    return m


def as_dicts(name, rows):
    return [dict(zip(HEADERS[name], r)) for r in rows]


# ---------------------------------------------------------------- shape validation
def test_normalize_fills_defaults_and_keeps_given_fields():
    m = norm({**BASE_MAPPING, "unit": {"value": " % "}, "dimension": "country", "domestic_values": ["Qatar", " "], "filters": [{"column": "year", "op": "=", "value": 2024}]})
    assert m["id_prefix"] == "kpi" and m["domestic_values"] == ["Qatar"] and m["unit"] == {"value": "%"}
    assert m["filters"] == [{"column": "year", "op": "=", "value": "2024"}]
    assert "target" not in m and m["frequency"] == {"value": "A"}
    assert mapping.normalize(m, "other")[0] == m  # idempotent (stored mappings are normalized again on read)
    assert mapping.default_prefix("SCAI-Stats.v2") == "scai_stats_v2" and mapping.default_prefix("") == "dataset"


@pytest.mark.parametrize("change, expected", [
    ({"id_prefix": "Bad-Prefix"}, 'id_prefix: "Bad-Prefix" must be 1–24 characters a–z, 0–9 or _'),
    ({"id_prefix": "a" * 25}, "id_prefix:"),
    ({"frequency": {"column": "freq", "map": {"yearly": "Y"}}}, "frequency: map values must be"),
    ({"frequency": {"column": "freq", "map": {}}}, "frequency: a frequency column needs a map"),
    ({"frequency": {"column": "freq", "value": "A", "map": {"y": "A"}}}, "frequency: give either"),
    ({"frequency": {"value": "W"}}, 'frequency: value must be "A", "Q" or "M"'),
    ({"series_key": []}, "series_key: choose at least one column"),
    ({"period": {"column": "period_date", "kind": "week"}}, "period: kind must be"),
    ({"table": "kpi"}, "table: \"kpi\" must be written schema.name"),
    ({"value": None}, "value: choose a column"),
    ({"filters": [{"column": "status", "op": "="}]}, "filters: filter 1 (=) needs a value"),
    ({"filters": [{"column": "status", "op": "like", "value": "%"}]}, "filters: filter 1 has an unknown operator"),
    ({"unit": {"value": "%", "column": "unit"}}, "unit: give either a fixed value or a column"),
    ({"sql": "select 1"}, 'mapping: unknown field "sql"'),
])
def test_normalize_rejects_bad_shapes(change, expected):
    m, problems = mapping.normalize({**BASE_MAPPING, **change}, "kpi")
    assert m is None and any(p.startswith(expected) for p in problems), problems


# ---------------------------------------------------------------- column validation
def test_check_columns_unknown_and_wrong_types():
    m = norm({**BASE_MAPPING, "series_key": ["indicator", "nope"], "value": "name", "filters": [{"column": "status", "op": "is_true"}]})
    problems = mapping.check_columns(m, COLUMNS)
    assert 'series_key: column "nope" does not exist in stats.kpi' in problems
    assert 'value: column "name" is text; choose a numeric column' in problems
    assert 'filters: column "status" is text; is_true needs a boolean column' in problems

    m = norm({**BASE_MAPPING, "period": {"column": "month", "kind": "date"}})
    assert mapping.check_columns(m, COLUMNS) == ['period: column "month" is text; choose kind text or a date column']
    m = norm({**BASE_MAPPING, "period": {"column": "period_date", "kind": "text"}})
    assert mapping.check_columns(m, COLUMNS) == ['period: column "period_date" is date; choose kind date']
    m = norm({**BASE_MAPPING, "period": {"column": "tgt", "kind": "text"}})
    assert mapping.check_columns(m, COLUMNS) == ['period: column "tgt" is double precision; choose a text, integer or date column']
    m = norm({**BASE_MAPPING, "period": {"column": "year", "kind": "text"}, "target": "tgt", "unit": {"column": "unit"}})
    assert mapping.check_columns(m, COLUMNS) == []


# ---------------------------------------------------------------- generated SQL
def test_sql_quotes_identifiers_and_parameterises_filter_values():
    evil = "x'; drop table t; --"
    m = norm({
        **BASE_MAPPING, "table": "My Schema.kpi", "series_key": ["indicator", 'we"ird'], "name_en": "name",
        "filters": [{"column": "status", "op": "=", "value": evil}, {"column": "is_published", "op": "is_true"},
                    {"column": "region", "op": "!=", "value": "Gulf"}, {"column": "val", "op": "is_not_null"}],
    })
    sql = mapping.build_sql(m, "My Schema", "kpi")
    where = (
        '\n where "status"::text = $1'
        '\n   and "is_published" is true'
        '\n   and "region"::text is distinct from $2'
        '\n   and "val" is not null'
    )
    assert sql.observations == (
        'select "indicator"::text as k0,\n'
        '       "we""ird"::text as k1,\n'
        '       "period_date" as period,\n'
        '       "val" as value\n'
        '  from "My Schema"."kpi"' + where
    )
    assert sql.registry == (
        'select "indicator"::text as k0,\n'
        '       "we""ird"::text as k1,\n'
        "       (array_agg(btrim(\"name\"::text) order by \"period_date\" desc) filter (where nullif(btrim(\"name\"::text), '') is not null))[1] as name_en\n"
        '  from "My Schema"."kpi"' + where + "\n group by 1, 2"
    )
    assert sql.args == [evil, "Gulf"]
    assert evil not in sql.observations and "Gulf" not in sql.registry


def test_sql_with_frequency_column_dimension_target_and_text_period():
    m = norm({**BASE_MAPPING, "period": {"column": "month", "kind": "text"}, "frequency": {"column": "freq", "map": {"monthly": "M"}},
              "dimension": "country", "target": "tgt", "unit": {"column": "unit"}})
    sql = mapping.build_sql(m, "stats", "kpi")
    assert sql.observations == (
        'select "indicator"::text as k0,\n       "freq"::text as freq,\n       "country"::text as dim,\n'
        '       "month"::text as period,\n       "val" as value,\n       "tgt" as target\n  from "stats"."kpi"'
    )
    assert sql.registry.endswith('as unit\n  from "stats"."kpi"\n group by 1, 2, 3') and sql.args == []
    assert mapping.quote_ident('a"b') == '"a""b"'
    with pytest.raises(ValueError):
        mapping.quote_ident("a\x00b")


# ---------------------------------------------------------------- ids
def test_ids_are_stable_slugs_with_hash_when_long():
    assert mapping.series_id("kpi", ["GDP Growth", "Real"], "A", None) == "kpi.gdp_growth_real.a.overall"
    assert mapping.series_id("kpi", ["GDP Growth"], "Q", "United Arab Emirates") == "kpi.gdp_growth.q.united_arab_emirates"
    assert mapping.id_slug("  --  ") == "unknown"
    long_a = "Share of non-hydrocarbon exports in total merchandise exports, by destination"
    long_b = long_a + " (revised)"
    sa, sb = mapping.id_slug(long_a), mapping.id_slug(long_b)
    assert len(sa) <= 48 and len(sb) <= 48 and sa != sb
    assert sa == mapping.id_slug(long_a) and sa.startswith("share_of_non_hydrocarbon_exports_in_tot_")
    assert sa[-9] == "_" and all(c in "0123456789abcdef" for c in sa[-8:])
    assert mapping.id_slug("x" * 48) == "x" * 48


# ---------------------------------------------------------------- rows → files
def obs_row(k, period, value, target=None, dim=None, freq=None):
    return {"k0": k, "period": period, "value": value, "target": target, "dim": dim, "freq": freq}


def test_periods_from_dates_by_frequency():
    for freq, d, expected in (("A", date(2024, 5, 3), "2024"), ("Q", date(2024, 5, 3), "2024-Q2"), ("M", datetime(2024, 11, 30, tzinfo=timezone.utc), "2024-11")):
        m = norm({**BASE_MAPPING, "frequency": {"value": freq}})
        res = mapping.map_rows(m, [], [obs_row("GDP", d, Decimal("1.5"))])
        assert res.observations[0][:2] == [f"kpi.gdp.{freq.lower()}.overall", expected]
    m = norm(BASE_MAPPING)
    res = mapping.map_rows(m, [], [obs_row("GDP", None, 1)])
    assert res.observations == [] and any("1 rows without a date period" in p for p in res.problems)


def test_text_periods_are_validated_against_frequency():
    m = norm({**BASE_MAPPING, "period": {"column": "month", "kind": "text"}, "frequency": {"column": "freq", "map": {"Monthly": "M", "Q": "Q", "annual": "A"}}})
    rows = [
        obs_row("GDP", "2024-03", 1, freq="Monthly"),
        obs_row("GDP", "2024-13", 1, freq="monthly"),   # bad month (map matched case-insensitively)
        obs_row("GDP", "2024-Q2", 2, freq="Q"),
        obs_row("GDP", "2024", 3, freq=" annual "),
        obs_row("GDP", "2024", 3, freq="weekly"),       # not in the map
        obs_row("GDP", "2024-Q1", 3, freq="annual"),    # quarter text for an annual series
    ]
    res = mapping.map_rows(m, [], rows)
    assert [(r[0], r[1]) for r in res.observations] == [("kpi.gdp.a.overall", "2024"), ("kpi.gdp.m.overall", "2024-03"), ("kpi.gdp.q.overall", "2024-Q2")]
    assert any(p.startswith("observations: 2 rows with a period not in the format") and '"2024-13 (M)"' in p for p in res.problems)
    assert any(p.startswith("observations: 1 rows whose frequency is not in the frequency map") and '"weekly"' in p for p in res.problems)


def test_domestic_dimension_names_units_and_registry():
    m = norm({**BASE_MAPPING, "dimension": "country", "domestic_values": ["National / Overall", "Qatar"], "target": "tgt",
              "name_en": "name", "unit": {"value": "%"}, "source": {"column": "src"}, "definition": "def"})
    rows = [
        obs_row("GDP", date(2023, 1, 1), 1, 2, dim="national / overall"),
        obs_row("GDP", date(2024, 1, 1), 3, None, dim=None),
        obs_row("GDP", date(2024, 1, 1), 4, None, dim=" QATAR "),   # same series + period → duplicate, last kept
        obs_row("GDP", date(2024, 1, 1), 5, None, dim="Saudi Arabia"),
        obs_row("GDP", date(2022, 1, 1), None, None, dim=None),       # empty
        obs_row("", date(2022, 1, 1), 1, None, dim=None),             # no key
        obs_row("Jobs", date(2024, 1, 1), 7, None, dim=None),
    ]
    reg_rows = [
        {"k0": "GDP", "dim": "Qatar", "name_en": None, "name_ar": None, "source": None, "definition": None},
        {"k0": "GDP", "dim": None, "name_en": "Real GDP growth", "source": "PSA", "definition": "Growth of real GDP."},
        {"k0": "GDP", "dim": "Saudi Arabia", "name_en": "Real GDP growth", "source": "GASTAT", "definition": None},
    ]
    res = mapping.map_rows(m, reg_rows, rows)
    obs = {(r["indicator_id"], r["period"]): r for r in as_dicts("observations", res.observations)}
    assert set(obs) == {("kpi.gdp.a.overall", "2023"), ("kpi.gdp.a.overall", "2024"), ("kpi.gdp.a.saudi_arabia", "2024"), ("kpi.jobs.a.overall", "2024")}
    assert obs[("kpi.gdp.a.overall", "2024")]["value"] == 4 and obs[("kpi.gdp.a.overall", "2023")]["target_value"] == 2
    assert obs[("kpi.gdp.a.overall", "2023")]["dimension"] == "" and obs[("kpi.gdp.a.saudi_arabia", "2024")]["dimension"] == "Saudi Arabia"
    assert all(r["publication_status"] == "published" for r in obs.values())
    assert "observations: 1 duplicate (indicator, period) rows; the last one was kept" in res.problems
    assert "observations: 1 rows with neither value nor target were left out" in res.problems
    assert "observations: 1 rows with an empty series key were left out" in res.problems
    assert "narratives: not part of a column mapping; narratives.csv contains the header only" in res.problems
    assert res.narratives == []

    reg = {r["indicator_id"]: r for r in as_dicts("registry", res.registry)}
    assert sorted(reg) == ["kpi.gdp.a.overall", "kpi.gdp.a.saudi_arabia", "kpi.jobs.a.overall"]
    g = reg["kpi.gdp.a.overall"]
    assert (g["name_en"], g["frequency"], g["unit"], g["source"], g["definition"], g["dimension"]) == ("Real GDP growth", "A", "%", "PSA", "Growth of real GDP.", "")
    assert g["cms_detail"] == "{}" and g["cms_intervals"] == "[]" and g["name_ar"] == ""
    s = reg["kpi.gdp.a.saudi_arabia"]
    assert s["name_en"] == "Real GDP growth (Saudi Arabia)" and s["dimension"] == s["geography"] == "Saudi Arabia" and s["source"] == "GASTAT"
    assert reg["kpi.jobs.a.overall"]["name_en"] == "Jobs"  # default name: the series key values
    assert all(len(r) == len(HEADERS["registry"]) for r in res.registry)


def test_default_name_joins_series_key_values():
    m = norm({**BASE_MAPPING, "series_key": ["indicator", "measure"]})
    res = mapping.map_rows(m, [], [{"k0": "GDP", "k1": "Real", "period": date(2024, 1, 1), "value": 1}])
    assert res.registry[0][:2] == ["kpi.gdp_real.a.overall", "GDP · Real"]


# ---------------------------------------------------------------- against a (fake) session
def fake_source(m, obs_rows, reg_rows=()):
    s = FakeSession()
    s.readable = [{"schema": "stats", "name": "kpi", "rows_estimate": 10}, {"schema": "public", "name": "indicators", "rows_estimate": 470}]
    s.columns_by_table = {"stats.kpi": COLUMNS}
    sql = mapping.build_sql(m, "stats", "kpi")
    obs_cols = list(obs_rows[0]) if obs_rows else ["k0", "period", "value"]
    reg_cols = list(reg_rows[0]) if reg_rows else ["k0"]
    s.queries = {sql.observations: (obs_cols, [tuple(r.values()) for r in obs_rows]), sql.registry: (reg_cols, [tuple(r.values()) for r in reg_rows])}
    return s, sql


MAPPED = {**BASE_MAPPING, "name_en": "name", "filters": [{"column": "status", "op": "=", "value": "final"}]}
OBS = [{"k0": "GDP", "period": date(2024, 1, 1), "value": Decimal("2.5")}, {"k0": "Jobs", "period": date(2023, 6, 1), "value": 7}]
REG = [{"k0": "GDP", "name_en": "Real GDP growth"}]


async def test_build_read_and_preview_for_mapped_dataset():
    m = norm(MAPPED, "kpi")
    s, sql = fake_source(m, OBS, REG)
    ds = {"preset": "mapped", "name": "kpi", "mapping": m}
    rr = await build.read(s, ds)
    assert rr.counts() == {"registry": 2, "observations": 2, "narratives": 0}
    assert [a for _, a in s.fetched] == [("final",), ("final",)]
    pv = await build.preview(s, ds, limit=1)
    assert pv["queries"] == {"registry": sql.registry, "observations": sql.observations}
    assert pv["files"]["observations"]["count"] == 2 and len(pv["files"]["observations"]["rows"]) == 1
    assert pv["files"]["registry"]["rows"][0][:4] == ["kpi.gdp.a.overall", "Real GDP growth", "", "A"]

    bad = {**ds, "mapping": {**m, "value": "name"}}
    pv = await build.preview(s, bad)
    assert pv["queries"] == {"registry": None, "observations": None} and pv["problems"] == ['value: column "name" is text; choose a numeric column']
    with pytest.raises(build.ExportError, match="cannot be used"):
        await build.read(s, bad)
    pv = await build.preview(s, {**ds, "mapping": {**m, "table": "stats.secret"}})
    assert pv["problems"] == ['table: "stats.secret" is not a table or view this login can read']


# ---------------------------------------------------------------- API
AUTH = {"Authorization": "Bearer k1"}
BASE = f"/api/t/{SBX}"
SOURCE = {"name": "stats", "host": "db", "database": "stats", "username": "ro", "password": "pw", "sslmode": "disable"}


@pytest.fixture
def env(monkeypatch):
    db: dict = {}
    m = norm(MAPPED, "kpi_ds")
    session, sql = fake_source(m, OBS, REG)
    # the dataset's stored mapping gets id_prefix kpi_ds (its name); mapping-preview without a name gets "kpi" (table)
    session.queries.update(fake_source(norm(MAPPED, "kpi"), OBS, REG)[0].queries)
    monkeypatch.setattr(export, "SERVICES", export.Services(store_factory=lambda pool: MemoryStore(db), s3_factory=lambda st: FakeS3(), opener=opener_for(session)))

    async def pool_factory():
        return FakeQueuePool()

    app = create_api(Settings(api_keys=["k1"], tenants_file="none.yaml", secret_key=Fernet.generate_key().decode()), pool_factory=pool_factory, kap_factory=lambda: FakeKap())
    with TestClient(app) as c:
        sid = c.post(f"{BASE}/pg-sources", json=SOURCE, headers=AUTH).json()["id"]
        yield {"c": c, "sid": sid, "session": session, "sql": sql, "db": db}


def test_columns_endpoint(env):
    c, sid = env["c"], env["sid"]
    r = c.get(f"{BASE}/pg-sources/{sid}/columns", params={"table": "stats.kpi"}, headers=AUTH)
    assert r.status_code == 200
    body = r.json()
    assert body["table"] == "stats.kpi" and body["columns"][0] == {"name": "indicator", "type": "text", "nullable": False}
    assert len(body["columns"]) == len(COLUMNS)
    for table in ("stats.nope", "pg_catalog.pg_authid", "kpi"):
        r = c.get(f"{BASE}/pg-sources/{sid}/columns", params={"table": table}, headers=AUTH)
        assert r.status_code == 404 and r.json()["type"] == "not_found"
    assert c.get(f"{BASE}/pg-sources/{sid}/columns", headers=AUTH).status_code == 422


def test_mapping_preview_endpoint(env):
    c, sid = env["c"], env["sid"]
    r = c.post(f"{BASE}/pg-sources/{sid}/mapping-preview", json={"mapping": MAPPED, "limit": 5}, headers=AUTH)
    assert r.status_code == 200
    pv = r.json()
    assert set(pv) == {"files", "problems", "queries"} and set(pv["queries"]) == {"registry", "observations"}
    assert '"status"::text = $1' in pv["queries"]["observations"] and "final" not in pv["queries"]["observations"]
    assert pv["files"]["observations"]["rows"][0][:5] == ["kpi.gdp.a.overall", "2024", "", "", "2.5"]
    assert pv["files"]["narratives"]["count"] == 0 and any(p.startswith("narratives:") for p in pv["problems"])
    assert env["db"]["datasets"] == {}  # nothing created

    r = c.post(f"{BASE}/pg-sources/{sid}/mapping-preview", json={"mapping": {**MAPPED, "id_prefix": "Bad!"}}, headers=AUTH)
    assert r.status_code == 200 and r.json()["queries"] == {"registry": None, "observations": None}
    assert r.json()["problems"] == ['id_prefix: "Bad!" must be 1–24 characters a–z, 0–9 or _']
    r = c.post(f"{BASE}/pg-sources/{sid}/mapping-preview", json={"mapping": {**MAPPED, "period": {"column": "month", "kind": "date"}}}, headers=AUTH)
    assert r.json()["problems"] == ['period: column "month" is text; choose kind text or a date column']


def test_mapped_dataset_create_patch_round_trip_and_preview(env):
    c, sid = env["c"], env["sid"]
    body = {"source_id": sid, "name": "kpi_ds", "preset": "mapped", "mapping": MAPPED, "queries": {"registry": "drop table x"}, "collection_name": "kpi_coll"}
    r = c.post(f"{BASE}/pg-datasets", json=body, headers=AUTH)
    assert r.status_code == 201, r.text
    ds = r.json()
    assert ds["preset"] == "mapped" and ds["queries"] == {}  # queries ignored for mapped
    assert ds["mapping"] == {**MAPPED, "id_prefix": "kpi_ds", "domestic_values": []}
    assert c.get(f"{BASE}/pg-datasets/{ds['id']}", headers=AUTH).json()["mapping"] == ds["mapping"]

    pv = c.post(f"{BASE}/pg-datasets/{ds['id']}/preview", headers=AUTH).json()
    assert pv["queries"]["observations"] == env["sql"].observations and pv["files"]["registry"]["rows"][0][0] == "kpi_ds.gdp.a.overall"

    # renaming keeps the stored id_prefix (ids stay stable)
    r = c.patch(f"{BASE}/pg-datasets/{ds['id']}", json={"name": "renamed"}, headers=AUTH)
    assert r.json()["mapping"]["id_prefix"] == "kpi_ds"
    r = c.patch(f"{BASE}/pg-datasets/{ds['id']}", json={"mapping": {**MAPPED, "id_prefix": "stats", "target": "tgt"}}, headers=AUTH)
    assert r.status_code == 200 and r.json()["mapping"]["id_prefix"] == "stats" and r.json()["mapping"]["target"] == "tgt"
    r = c.patch(f"{BASE}/pg-datasets/{ds['id']}", json={"mapping": {**MAPPED, "frequency": {"column": "freq", "map": {"x": "W"}}}}, headers=AUTH)
    assert r.status_code == 422 and r.json()["type"] == "validation_error" and r.json()["data"]["problems"][0].startswith("frequency:")

    # switching preset drops the mapping
    r = c.patch(f"{BASE}/pg-datasets/{ds['id']}", json={"preset": "scai_indicators"}, headers=AUTH)
    assert r.json()["preset"] == "scai_indicators" and r.json()["mapping"] is None and env["db"]["datasets"][ds["id"]]["mapping"] is None

    r = c.post(f"{BASE}/pg-datasets", json={**body, "name": "nomap", "collection_name": "nomap_c", "mapping": None}, headers=AUTH)
    assert r.status_code == 422 and r.json()["type"] == "validation_error"
    r = c.post(f"{BASE}/pg-datasets", json={**body, "name": "badmap", "collection_name": "badmap_c", "mapping": {**MAPPED, "id_prefix": "UPPER"}}, headers=AUTH)
    assert r.status_code == 422 and "id_prefix" in r.json()["message"]
