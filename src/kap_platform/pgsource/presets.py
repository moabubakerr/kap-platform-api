"""Built-in export for the `scai_indicators` schema (scai-chatbot etl/schema.sql).

Mapping (ids follow the CMS scheme the KAP sandbox uses):

- One indicator series per indicator detail × granularity × geography found in
  `indicator_values`. The id is
  `cms.<published_detail_id, lower-case, no dashes>.<a|q|m>.<overall|country>`:
  the published id is P02.PublishedIndicatorDetailId, the same id the CMS and
  the sandbox files use. Qatar (country_en NULL/blank/'Qatar', the ETL's
  "domestic" rows) is `overall` with a blank dimension; benchmark-country rows
  become their own series `….<country slug>` with dimension and geography =
  the country name, so a lookup with dimension='' always gets Qatar.
- A detail without a published id is skipped when `published_only`; otherwise
  its internal indicator_detail_id stands in for the CMS id.
- registry: names from the detail (falling back to the indicator), frequency
  from the granularity, unit_en, definition EN + blank line + AR,
  source = data_source_en, cms_* JSON placeholders.
- observations: period from period_date per granularity, value = actual,
  target_value = target, baseline from the detail (Qatar series only),
  publication_status = published. Rows with neither actual nor target are
  dropped; duplicates of (indicator, period, dimension) keep the last row.
- narratives: `indicator_analysis` rows matched to their data point —
  `item` rows via indicator_values.value_id, `published` rows via
  published_data_points — one row per language that has text (summary,
  detailed analysis, NPC analysis → additional_info, benchmark). Where both an
  item and a published analysis exist for the same point, published wins.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

from .kapcsv import FREQUENCY, format_period, indicator_id

DETAILS_SQL = """
select d.indicator_detail_id, d.published_detail_id,
       coalesce(nullif(btrim(d.name_en), ''), i.name_en) as name_en,
       coalesce(nullif(btrim(d.name_ar), ''), i.name_ar) as name_ar,
       d.unit_en, d.definition_en, d.definition_ar, d.data_source_en,
       d.baseline_value, d.baseline_year
  from indicator_details d
  left join indicators i on i.indicator_id = d.indicator_id
 order by d.indicator_detail_id
"""

VALUES_SQL = """
select v.indicator_detail_id, v.period_date, lower(v.granularity) as granularity,
       nullif(btrim(v.country_en), '') as country_en, v.actual, v.target
  from indicator_values v
 where v.period_date is not null and lower(v.granularity) in ('yearly', 'quarterly', 'monthly')
 order by v.indicator_detail_id, v.period_date, v.value_id
"""

NARRATIVES_SQL = """
select a.analysis_id, a.source as origin,
       coalesce(v.indicator_detail_id, d.indicator_detail_id) as indicator_detail_id,
       coalesce(v.period_date, p.period_date) as period_date,
       lower(coalesce(v.granularity, p.granularity)) as granularity,
       nullif(btrim(coalesce(v.country_en, p.country_en)), '') as country_en,
       a.summary_en, a.detailed_analysis_en, a.npc_analysis_en, a.benchmark_en,
       a.summary_ar, a.detailed_analysis_ar, a.npc_analysis_ar, a.benchmark_ar
  from indicator_analysis a
  left join indicator_values v
         on a.source = 'item' and lower(v.value_id) = lower(a.ref_data_point_id)
  left join published_data_points p
         on a.source = 'published' and lower(p.published_data_point_id) = lower(a.ref_data_point_id)
  left join indicator_details d
         on lower(d.published_detail_id) = lower(p.published_indicator_detail_id)
 order by a.analysis_id
"""

DOMESTIC = ("", "qatar")
# JSON placeholders for the CMS columns, as in the sandbox registry
CMS_EMPTY = {"cms_benchmark_countries": "[]", "cms_country": "", "cms_detail": "{}", "cms_intervals": "[]", "cms_parent": "{}", "source_document_metadata": "{}", "source_documents": "[]"}


def country_of(value: Any) -> str | None:
    """None for Qatar (domestic), else the country name."""
    if value is None:
        return None
    s = str(value).strip()
    return None if s.lower() in DOMESTIC else s


def _text(v: Any) -> str:
    return "" if v is None else str(v).strip()


def definition(en: Any, ar: Any) -> str:
    return "\n\n".join(t for t in (_text(en), _text(ar)) if t)


@dataclass
class PresetResult:
    registry: list[list[Any]] = field(default_factory=list)
    observations: list[list[Any]] = field(default_factory=list)
    narratives: list[list[Any]] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)

    def rows(self, name: str) -> list[list[Any]]:
        return getattr(self, name)


def cms_id_for(detail: Mapping[str, Any], published_only: bool) -> str | None:
    pub = _text(detail.get("published_detail_id"))
    if pub:
        return pub
    return None if published_only else _text(detail.get("indicator_detail_id")) or None


def map_scai(details: Iterable[Mapping[str, Any]], values: Iterable[Mapping[str, Any]], analysis: Iterable[Mapping[str, Any]] | None, published_only: bool = True) -> PresetResult:
    res = PresetResult()
    by_detail = {_text(d["indicator_detail_id"]): d for d in details}
    cms_ids = {k: cms_id_for(d, published_only) for k, d in by_detail.items()}

    # ---- observations (and the series they reveal)
    series: dict[str, tuple[str, str, str | None]] = {}  # indicator id → (detail key, granularity, country)
    obs: dict[tuple[str, str, str], list[Any]] = {}
    skipped_unpublished = skipped_unknown = skipped_empty = dupes = 0
    for v in values:
        key = _text(v["indicator_detail_id"])
        detail = by_detail.get(key)
        if detail is None:
            skipped_unknown += 1
            continue
        cms = cms_ids[key]
        if cms is None:
            skipped_unpublished += 1
            continue
        gran = _text(v["granularity"]).lower()
        period = format_period(v["period_date"], gran)
        if gran not in FREQUENCY or period is None:
            skipped_unknown += 1
            continue
        if v["actual"] is None and v["target"] is None:
            skipped_empty += 1
            continue
        country = country_of(v["country_en"])
        iid = indicator_id(cms, gran, country)
        series.setdefault(iid, (key, gran, country))
        dim = country or ""
        domestic = country is None
        row = [
            iid, period, dim, "", v["actual"], v["target"],
            detail.get("baseline_value") if domestic else None,
            detail.get("baseline_year") if domestic and detail.get("baseline_value") is not None else None,
            "", "", "published", "",
        ]
        k = (iid, period, dim)
        if k in obs:
            dupes += 1
        obs[k] = row
    res.observations = [obs[k] for k in sorted(obs)]

    # ---- registry
    for iid in sorted(series):
        key, gran, country = series[iid]
        d = by_detail[key]
        name_en, name_ar = _text(d.get("name_en")), _text(d.get("name_ar"))
        if country:
            name_en = f"{name_en} ({country})" if name_en else country
            name_ar = f"{name_ar} ({country})" if name_ar else ""
        res.registry.append([
            iid, name_en, name_ar, FREQUENCY[gran], _text(d.get("unit_en")), country or "", country or "Qatar", "", "",
            definition(d.get("definition_en"), d.get("definition_ar")), "", _text(d.get("data_source_en")), "", "", "",
            CMS_EMPTY["cms_benchmark_countries"], CMS_EMPTY["cms_country"], CMS_EMPTY["cms_detail"], CMS_EMPTY["cms_intervals"],
            CMS_EMPTY["cms_parent"], CMS_EMPTY["source_document_metadata"], CMS_EMPTY["source_documents"],
        ])

    # ---- narratives
    if analysis is not None:
        narr: dict[tuple[str, str, str], tuple[int, list[Any]]] = {}
        unmatched = 0
        for a in analysis:
            key = _text(a.get("indicator_detail_id"))
            gran = _text(a.get("granularity")).lower()
            cms = cms_ids.get(key)
            period = format_period(a.get("period_date"), gran) if gran in FREQUENCY else None
            if not cms or period is None:
                unmatched += 1
                continue
            iid = indicator_id(cms, gran, country_of(a.get("country_en")))
            if iid not in series:
                unmatched += 1
                continue
            rank = 1 if _text(a.get("origin")) == "published" else 0
            source = _text(by_detail[key].get("data_source_en"))
            for lang in ("en", "ar"):
                texts = [_text(a.get(f"{f}_{lang}")) for f in ("summary", "detailed_analysis", "npc_analysis", "benchmark")]
                if not any(texts):
                    continue
                k = (iid, lang, period)
                if k in narr and narr[k][0] > rank:
                    continue
                narr[k] = (rank, [iid, lang, period, *texts, source, ""])
        res.narratives = [narr[k][1] for k in sorted(narr)]
        if unmatched:
            res.problems.append(f"narratives: {unmatched} analysis rows do not match an exported series and were left out")
    else:
        res.problems.append("narratives: the source has no usable indicator_analysis table; narratives.csv contains the header only")

    if skipped_unpublished:
        res.problems.append(f"observations: {skipped_unpublished} values of unpublished indicator details left out (published only)")
    if skipped_unknown:
        res.problems.append(f"observations: {skipped_unknown} values with an unknown detail or period left out")
    if skipped_empty:
        res.problems.append(f"observations: {skipped_empty} values with neither actual nor target left out")
    if dupes:
        res.problems.append(f"observations: {dupes} duplicate (indicator, period, dimension) values; the last one was kept")
    return res
