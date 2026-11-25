"""scai_indicators preset: row mapping on fixture rows shaped like the stand's schema."""
from kap_platform.pgsource import presets
from kap_platform.pgsource.kapcsv import HEADERS

from pg_fakes import ANALYSIS, DETAILS, PUB_ID, VALUES

A = f"cms.{PUB_ID}.a.overall"
Q = f"cms.{PUB_ID}.q.overall"
UAE = f"cms.{PUB_ID}.a.united_arab_emirates"


def as_dicts(name, rows):
    return [dict(zip(HEADERS[name], r)) for r in rows]


def test_registry_one_series_per_detail_granularity_and_geography():
    res = presets.map_scai(DETAILS, VALUES, ANALYSIS, published_only=True)
    reg = {r["indicator_id"]: r for r in as_dicts("registry", res.registry)}
    assert sorted(reg) == sorted([A, Q, UAE])  # d2 (unpublished) skipped
    a = reg[A]
    assert a["name_en"] == "Non-Hydrocarbon Exports" and a["frequency"] == "A" and a["unit"] == "%"
    assert a["dimension"] == "" and a["geography"] == "Qatar" and a["source"] == "National Planning Council"
    assert a["definition"] == "Share of non-hydrocarbon exports.\n\nحصة الصادرات غير الهيدروكربونية."
    assert a["cms_benchmark_countries"] == "[]" and a["cms_detail"] == "{}" and a["cms_country"] == ""
    assert reg[Q]["frequency"] == "Q"
    assert reg[UAE]["dimension"] == "United Arab Emirates" and reg[UAE]["name_en"] == "Non-Hydrocarbon Exports (United Arab Emirates)"
    assert all(len(r) == len(HEADERS["registry"]) for r in res.registry)


def test_observations_periods_values_baseline():
    res = presets.map_scai(DETAILS, VALUES, ANALYSIS, published_only=True)
    obs = {(r["indicator_id"], r["period"]): r for r in as_dicts("observations", res.observations)}
    assert set(obs) == {(A, "2019"), (A, "2020"), (Q, "2024-Q1"), (UAE, "2019")}  # Q3 has no actual/target
    r = obs[(A, "2019")]
    assert str(r["value"]) == "32.04" and str(r["target_value"]) == "50" and r["publication_status"] == "published"
    assert str(r["baseline_value"]) == "25.5" and r["baseline_year"] == 2018 and r["dimension"] == ""
    assert obs[(UAE, "2019")]["dimension"] == "United Arab Emirates" and obs[(UAE, "2019")]["baseline_value"] is None
    assert any("neither actual nor target" in p for p in res.problems)
    assert any("unpublished" in p for p in res.problems)


def test_unpublished_details_included_when_not_published_only():
    res = presets.map_scai(DETAILS, VALUES, ANALYSIS, published_only=False)
    ids = [r[0] for r in res.registry]
    assert "cms.d2.a.overall" in ids


def test_duplicates_keep_last():
    values = VALUES[:1] + [dict(VALUES[0], actual=33)]
    res = presets.map_scai(DETAILS, values, [], published_only=True)
    assert len(res.observations) == 1 and res.observations[0][4] == 33
    assert any("duplicate" in p for p in res.problems)


def test_narratives_prefer_published_and_split_languages():
    res = presets.map_scai(DETAILS, VALUES, ANALYSIS, published_only=True)
    narr = as_dicts("narratives", res.narratives)
    assert [(n["indicator_id"], n["language"], n["period"]) for n in narr] == [(A, "ar", "2019"), (A, "en", "2019")]
    en = narr[1]
    assert en["summary"] == "Exports, rose" and en["details"] == "Line 1\nLine 2" and en["additional_info"] == "NPC" and en["benchmark"] == "Above GCC"
    assert en["source"] == "National Planning Council"
    assert any("analysis rows do not match" in p for p in res.problems)


def test_no_analysis_table_gives_header_only():
    res = presets.map_scai(DETAILS, VALUES, None)
    assert res.narratives == [] and any("header only" in p for p in res.problems)


def test_national_overall_is_qatar():
    # scai_indicators labels Qatar's own values "National / Overall" (6,569 of 9,497 on the stand)
    for v in ("National / Overall", " national / overall ", "", None, "Qatar"):
        assert presets.country_of(v) is None
    assert presets.country_of("KSA") == "KSA"
    values = [dict(r, country_en="National / Overall") if not r.get("country_en") else r for r in VALUES]
    res = presets.map_scai(DETAILS, values, ANALYSIS, published_only=True)
    ids = {r[0] for r in res.registry}
    assert A in ids and not any(i.endswith("national_overall") for i in ids)
