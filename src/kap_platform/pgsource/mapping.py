"""`mapped` preset: one table or view (a row per observation) + a column mapping → generated SQL.

The user never writes SQL. The mapping names columns; they are checked against
the table's real columns (information_schema), the table against the tables the
source login can SELECT, identifiers are double-quoted, and filter values go to
the server as parameters ($1 …). Two queries are generated:

- registry: one group per series key × frequency × dimension, with the newest
  non-blank name_en / name_ar / unit / source / definition of the group;
- observations: the mapped columns of every (filtered) row.

indicator_id = `<id_prefix>.<slug(series key values joined ".")>.<a|q|m>.<overall | slug(dimension)>`.
Narratives are not part of a mapping (header only).
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Iterable, Mapping

from .kapcsv import format_period
from .presets import CMS_EMPTY, PresetResult

ID_PREFIX = re.compile(r"^[a-z0-9_]{1,24}$")
SLUG_MAX = 48
FREQS = ("A", "Q", "M")
GRANULARITY = {"A": "yearly", "Q": "quarterly", "M": "monthly"}
PERIOD_TEXT = {"A": re.compile(r"^\d{4}$"), "Q": re.compile(r"^\d{4}-Q[1-4]$"), "M": re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")}
PERIOD_HINT = {"A": "YYYY", "Q": "YYYY-Qn", "M": "YYYY-MM"}
OPS = ("=", "!=", "is_true", "is_false", "is_not_null")
KEYS = (
    "table", "series_key", "id_prefix", "period", "frequency", "value", "target", "name_en", "name_ar",
    "unit", "source", "definition", "dimension", "domestic_values", "filters",
)
OPTIONAL_COLUMNS = ("target", "name_en", "name_ar", "definition", "dimension")

# information_schema data_type (or udt_name for USER-DEFINED / ARRAY) by kind
DATE_TYPES = frozenset({"date", "timestamp without time zone", "timestamp with time zone"})
TEXT_TYPES = frozenset({"text", "character varying", "character", "citext", "name"})
INT_TYPES = frozenset({"smallint", "integer", "bigint"})
NUMERIC_TYPES = INT_TYPES | {"numeric", "real", "double precision"}
BOOL_TYPES = frozenset({"boolean"})

COLUMNS_SQL = """
select column_name as name,
       case when data_type in ('USER-DEFINED', 'ARRAY') then udt_name else data_type end as type,
       is_nullable = 'YES' as nullable
  from information_schema.columns
 where table_schema = $1 and table_name = $2
 order by ordinal_position
"""


# ---------------------------------------------------------------- ids
def id_slug(text: str) -> str:
    """lower-case, non [a-z0-9] runs → '_', trimmed; > 48 chars → 39 chars + '_' + 8-char hash (stable)."""
    s = re.sub(r"[^a-z0-9]+", "_", text.strip().lower()).strip("_")
    if not s:
        return "unknown"
    if len(s) <= SLUG_MAX:
        return s
    digest = hashlib.sha1(s.encode("utf-8")).hexdigest()[:8]
    return f"{s[:SLUG_MAX - 9].rstrip('_')}_{digest}"


def series_id(prefix: str, key_values: Iterable[Any], freq: str, dimension: str | None) -> str:
    joined = ".".join(_text(v) for v in key_values)
    return f"{prefix}.{id_slug(joined)}.{freq.lower()}.{id_slug(dimension) if dimension else 'overall'}"


def default_prefix(name: str | None) -> str:
    s = re.sub(r"[^a-z0-9_]+", "_", (name or "").lower()).strip("_")[:24].strip("_")
    return s or "dataset"


def quote_ident(name: str) -> str:
    if "\x00" in name:
        raise ValueError("identifier contains a NUL character")
    return '"' + name.replace('"', '""') + '"'


def _text(v: Any) -> str:
    return "" if v is None else str(v).strip()


# ---------------------------------------------------------------- shape validation
def normalize(raw: Any, default_id_prefix: str) -> tuple[dict[str, Any] | None, list[str]]:
    """Check the mapping's shape (no database needed). Returns (clean mapping, problems)."""
    if not isinstance(raw, Mapping):
        return None, ["mapping: an object is required"]
    problems: list[str] = []
    out: dict[str, Any] = {}

    for k in raw:
        if k not in KEYS:
            problems.append(f'mapping: unknown field "{k}"')

    def column(value: Any, label: str, required: bool) -> str | None:
        if value is None or (isinstance(value, str) and not value.strip()):
            if required:
                problems.append(f"{label}: choose a column")
            return None
        if not isinstance(value, str):
            problems.append(f"{label}: must be a column name")
            return None
        return value.strip()

    table = raw.get("table")
    if not isinstance(table, str) or not table.strip():
        problems.append("table: choose a table or view")
    elif "." not in table.strip():
        problems.append(f'table: "{table}" must be written schema.name, e.g. public.{table}')
    else:
        out["table"] = table.strip()

    keys = raw.get("series_key")
    if not isinstance(keys, list) or not keys:
        problems.append("series_key: choose at least one column")
    else:
        cols = [column(k, "series_key", True) for k in keys]
        if all(cols):
            if len(set(cols)) != len(cols):
                problems.append("series_key: a column is listed twice")
            out["series_key"] = cols

    prefix = raw.get("id_prefix")
    if prefix is None or (isinstance(prefix, str) and not prefix.strip()):
        out["id_prefix"] = default_id_prefix
    elif not isinstance(prefix, str) or not ID_PREFIX.match(prefix.strip()):
        problems.append(f'id_prefix: "{prefix}" must be 1–24 characters a–z, 0–9 or _')
    else:
        out["id_prefix"] = prefix.strip()

    period = raw.get("period")
    if not isinstance(period, Mapping):
        problems.append("period: choose a column and its kind (date or text)")
    else:
        pcol = column(period.get("column"), "period", True)
        kind = period.get("kind")
        if kind not in ("date", "text"):
            problems.append(f'period: kind must be "date" or "text", not {kind!r}')
        elif pcol:
            out["period"] = {"column": pcol, "kind": kind}

    freq = raw.get("frequency")
    if not isinstance(freq, Mapping) or ("value" in freq) == ("column" in freq):
        problems.append('frequency: give either a fixed value ("A", "Q" or "M") or a column with a value map')
    elif "value" in freq:
        if freq["value"] not in FREQS:
            problems.append(f'frequency: value must be "A", "Q" or "M", not {freq["value"]!r}')
        else:
            out["frequency"] = {"value": freq["value"]}
    else:
        fcol = column(freq.get("column"), "frequency", True)
        fmap = freq.get("map")
        if not isinstance(fmap, Mapping) or not fmap:
            problems.append('frequency: a frequency column needs a map of its values to "A", "Q" or "M"')
        else:
            bad = [f"{k!r} → {v!r}" for k, v in fmap.items() if v not in FREQS]
            if bad:
                problems.append(f'frequency: map values must be "A", "Q" or "M" ({", ".join(bad)})')
            elif fcol:
                out["frequency"] = {"column": fcol, "map": {str(k): v for k, v in fmap.items()}}

    v = column(raw.get("value"), "value", True)
    if v:
        out["value"] = v
    for k in OPTIONAL_COLUMNS:
        c = column(raw.get(k), k, False)
        if c:
            out[k] = c

    for k in ("unit", "source"):
        spec = raw.get(k)
        if spec is None:
            continue
        if not isinstance(spec, Mapping) or ("value" in spec) == ("column" in spec):
            problems.append(f"{k}: give either a fixed value or a column")
        elif "value" in spec:
            if not isinstance(spec["value"], str):
                problems.append(f"{k}: the fixed value must be text")
            elif spec["value"].strip():
                out[k] = {"value": spec["value"].strip()}
        else:
            c = column(spec.get("column"), k, True)
            if c:
                out[k] = {"column": c}

    dom = raw.get("domestic_values")
    if dom is None:
        out["domestic_values"] = []
    elif not isinstance(dom, list) or not all(isinstance(x, str) for x in dom):
        problems.append("domestic_values: must be a list of texts")
    else:
        out["domestic_values"] = [x.strip() for x in dom if x.strip()]
    if out.get("domestic_values") and "dimension" not in out:
        problems.append("domestic_values: only used with a dimension column")

    filters = raw.get("filters")
    if filters is None:
        out["filters"] = []
    elif not isinstance(filters, list):
        problems.append("filters: must be a list")
    else:
        clean = []
        for i, f in enumerate(filters, 1):
            if not isinstance(f, Mapping):
                problems.append(f"filters: filter {i} must be an object")
                continue
            c = column(f.get("column"), f"filters: filter {i}", True)
            op = f.get("op")
            if op not in OPS:
                problems.append(f"filters: filter {i} has an unknown operator {op!r} (use {', '.join(OPS)})")
                continue
            item: dict[str, Any] = {"column": c, "op": op}
            if op in ("=", "!="):
                val = f.get("value")
                if val is None or isinstance(val, (dict, list)):
                    problems.append(f"filters: filter {i} ({op}) needs a value")
                    continue
                item["value"] = ("true" if val else "false") if isinstance(val, bool) else str(val)
            if c:
                clean.append(item)
        out["filters"] = clean

    return (None, problems) if problems else (out, [])


# ---------------------------------------------------------------- column validation
def check_columns(m: Mapping[str, Any], columns: Iterable[Mapping[str, Any]]) -> list[str]:
    """Every mapped column exists in the table and has a usable type."""
    types = {c["name"]: str(c["type"]).lower() for c in columns}
    problems: list[str] = []

    def known(label: str, col: str) -> str | None:
        if col not in types:
            problems.append(f'{label}: column "{col}" does not exist in {m["table"]}')
            return None
        return types[col]

    for c in m["series_key"]:
        known("series_key", c)

    p = m["period"]
    t = known("period", p["column"])
    if t is not None:
        if p["kind"] == "date" and t not in DATE_TYPES:
            problems.append(f'period: column "{p["column"]}" is {t}; choose kind text or a date column')
        elif p["kind"] == "text" and t in DATE_TYPES:
            problems.append(f'period: column "{p["column"]}" is {t}; choose kind date')
        elif p["kind"] == "text" and t not in TEXT_TYPES | INT_TYPES:
            problems.append(f'period: column "{p["column"]}" is {t}; choose a text, integer or date column')

    if "column" in m["frequency"]:
        known("frequency", m["frequency"]["column"])

    for label in ("value", "target"):
        if label in m:
            t = known(label, m[label])
            if t is not None and t not in NUMERIC_TYPES:
                problems.append(f'{label}: column "{m[label]}" is {t}; choose a numeric column')

    for label in ("name_en", "name_ar", "definition", "dimension"):
        if label in m:
            known(label, m[label])
    for label in ("unit", "source"):
        if "column" in m.get(label, {}):
            known(label, m[label]["column"])

    for f in m["filters"]:
        t = known("filters", f["column"])
        if t is not None and f["op"] in ("is_true", "is_false") and t not in BOOL_TYPES:
            problems.append(f'filters: column "{f["column"]}" is {t}; {f["op"]} needs a boolean column')
    return problems


# ---------------------------------------------------------------- SQL
@dataclass
class GeneratedSql:
    registry: str
    observations: str
    args: list[str] = field(default_factory=list)

    def queries(self) -> dict[str, str]:
        return {"registry": self.registry, "observations": self.observations}


def _newest(col: str, period: str) -> str:
    c = quote_ident(col)
    return f"(array_agg(btrim({c}::text) order by {period} desc) filter (where nullif(btrim({c}::text), '') is not null))[1]"


def build_sql(m: Mapping[str, Any], schema: str, table: str) -> GeneratedSql:
    """Both queries for a checked mapping. Only quoted identifiers and $n parameters; no user text in the SQL."""
    source = f"{quote_ident(schema)}.{quote_ident(table)}"
    period = quote_ident(m["period"]["column"])

    conds: list[str] = []
    args: list[str] = []
    for f in m["filters"]:
        c = quote_ident(f["column"])
        if f["op"] == "=":
            args.append(f["value"])
            conds.append(f"{c}::text = ${len(args)}")
        elif f["op"] == "!=":
            args.append(f["value"])
            conds.append(f"{c}::text is distinct from ${len(args)}")
        elif f["op"] == "is_true":
            conds.append(f"{c} is true")
        elif f["op"] == "is_false":
            conds.append(f"{c} is false")
        else:
            conds.append(f"{c} is not null")
    where = ("\n where " + "\n   and ".join(conds)) if conds else ""

    group = [f"{quote_ident(k)}::text as k{i}" for i, k in enumerate(m["series_key"])]
    if "column" in m["frequency"]:
        group.append(f"{quote_ident(m['frequency']['column'])}::text as freq")
    if "dimension" in m:
        group.append(f"{quote_ident(m['dimension'])}::text as dim")

    obs = list(group)
    obs.append(f"{period}{'::text' if m['period']['kind'] == 'text' else ''} as period")
    obs.append(f"{quote_ident(m['value'])} as value")
    if "target" in m:
        obs.append(f"{quote_ident(m['target'])} as target")
    observations = "select " + ",\n       ".join(obs) + f"\n  from {source}{where}"

    reg = list(group)
    for k in ("name_en", "name_ar", "definition"):
        if k in m:
            reg.append(f"{_newest(m[k], period)} as {k}")
    for k in ("unit", "source"):
        if "column" in m.get(k, {}):
            reg.append(f"{_newest(m[k]['column'], period)} as {k}")
    registry = (
        "select " + ",\n       ".join(reg) + f"\n  from {source}{where}"
        + f"\n group by {', '.join(str(i + 1) for i in range(len(group)))}"
    )
    return GeneratedSql(registry, observations, args)


# ---------------------------------------------------------------- rows → KAP files
def _examples(values: list[str]) -> str:
    shown = sorted(set(values))[:3]
    return ", ".join(f'"{v}"' for v in shown)


def map_rows(m: Mapping[str, Any], registry_rows: Iterable[Mapping[str, Any]], observation_rows: Iterable[Mapping[str, Any]]) -> PresetResult:
    res = PresetResult()
    n_keys = len(m["series_key"])
    fixed_freq = m["frequency"].get("value")
    fmap = m["frequency"].get("map") or {}
    fmap_ci = {k.strip().lower(): v for k, v in fmap.items()}
    domestic = {d.lower() for d in m.get("domestic_values", [])}
    has_dim = "dimension" in m
    has_target = "target" in m
    kind = m["period"]["kind"]

    def freq_of(row: Mapping[str, Any]) -> str | None:
        if fixed_freq:
            return fixed_freq
        raw = _text(row["freq"])
        return fmap.get(raw) or fmap.get(str(row["freq"])) or fmap_ci.get(raw.lower())

    def dim_of(row: Mapping[str, Any]) -> str | None:
        if not has_dim:
            return None
        d = _text(row["dim"])
        return None if not d or d.lower() in domestic else d

    def period_of(v: Any, freq: str) -> str | None:
        if kind == "date":
            return format_period(v, GRANULARITY[freq]) if isinstance(v, (date, datetime)) else None
        s = _text(v)
        return s if PERIOD_TEXT[freq].match(s) else None

    series: dict[str, tuple[list[str], str, str | None]] = {}
    obs: dict[tuple[str, str], list[Any]] = {}
    no_key = empty = dupes = 0
    bad_freq: list[str] = []
    bad_period: list[str] = []
    for row in observation_rows:
        keys = [_text(row[f"k{i}"]) for i in range(n_keys)]
        if not any(keys):
            no_key += 1
            continue
        freq = freq_of(row)
        if freq is None:
            bad_freq.append(_text(row["freq"]))
            continue
        period = period_of(row["period"], freq)
        if period is None:
            bad_period.append(f"{_text(row['period'])} ({freq})")
            continue
        value, target = row["value"], row["target"] if has_target else None
        if value is None and target is None:
            empty += 1
            continue
        dim = dim_of(row)
        iid = series_id(m["id_prefix"], keys, freq, dim)
        series.setdefault(iid, (keys, freq, dim))
        k = (iid, period)
        if k in obs:
            dupes += 1
        obs[k] = [iid, period, dim or "", "", value, target, "", "", "", "", "published", ""]
    res.observations = [obs[k] for k in sorted(obs)]

    # registry: names etc. from the grouped query, merged per series (newest non-blank first)
    meta: dict[str, dict[str, str]] = {}
    for row in registry_rows:
        keys = [_text(row[f"k{i}"]) for i in range(n_keys)]
        freq = freq_of(row)
        if freq is None or not any(keys):
            continue
        iid = series_id(m["id_prefix"], keys, freq, dim_of(row))
        info = meta.setdefault(iid, {})
        for k in ("name_en", "name_ar", "definition", "unit", "source"):
            if k in row.keys() and not info.get(k) and _text(row[k]):
                info[k] = _text(row[k])

    for iid in sorted(series):
        keys, freq, dim = series[iid]
        info = meta.get(iid, {})
        name_en = info.get("name_en") or " · ".join(k for k in keys if k)
        name_ar = info.get("name_ar", "")
        if dim:
            name_en = f"{name_en} ({dim})"
            name_ar = f"{name_ar} ({dim})" if name_ar else ""
        unit = m["unit"]["value"] if "value" in m.get("unit", {}) else info.get("unit", "")
        source = m["source"]["value"] if "value" in m.get("source", {}) else info.get("source", "")
        res.registry.append([
            iid, name_en, name_ar, freq, unit, dim or "", dim or "", "", "",
            info.get("definition", ""), "", source, "", "", "",
            CMS_EMPTY["cms_benchmark_countries"], CMS_EMPTY["cms_country"], CMS_EMPTY["cms_detail"], CMS_EMPTY["cms_intervals"],
            CMS_EMPTY["cms_parent"], CMS_EMPTY["source_document_metadata"], CMS_EMPTY["source_documents"],
        ])

    if no_key:
        res.problems.append(f"observations: {no_key} rows with an empty series key were left out")
    if bad_freq:
        res.problems.append(f"observations: {len(bad_freq)} rows whose frequency is not in the frequency map were left out ({_examples(bad_freq)})")
    if bad_period:
        what = "without a date period" if kind == "date" else "with a period not in the format YYYY / YYYY-Qn / YYYY-MM of their frequency"
        res.problems.append(f"observations: {len(bad_period)} rows {what} were left out" + (f" ({_examples(bad_period)})" if kind == "text" else ""))
    if empty:
        res.problems.append(f"observations: {empty} rows with neither value nor target were left out")
    if dupes:
        res.problems.append(f"observations: {dupes} duplicate (indicator, period) rows; the last one was kept")
    res.problems.append("narratives: not part of a column mapping; narratives.csv contains the header only")
    return res


# ---------------------------------------------------------------- against a source session
async def resolve_table(session: Any, table: str) -> tuple[str, str] | None:
    """(schema, name) of a table/view the login can SELECT, matched on 'schema.name'; None if unknown."""
    for t in await session.tables():
        if f"{t['schema']}.{t['name']}" == table:
            return t["schema"], t["name"]
    return None


@dataclass
class MappedRead:
    result: PresetResult | None = None
    queries: dict[str, str] | None = None
    problems: list[str] = field(default_factory=list)


async def read(session: Any, raw_mapping: Any, default_id_prefix: str) -> MappedRead:
    """Validate the mapping against the source, generate and run the SQL. Mapping problems → result None.
    asyncpg.PostgresError from the queries propagates."""
    m, problems = normalize(raw_mapping, default_id_prefix)
    if m is None:
        return MappedRead(problems=problems)
    found = await resolve_table(session, m["table"])
    if found is None:
        return MappedRead(problems=[f'table: "{m["table"]}" is not a table or view this login can read'])
    problems = check_columns(m, await session.columns(*found))
    if problems:
        return MappedRead(problems=problems)
    sql = build_sql(m, *found)
    reg = await session.fetch(sql.registry, *sql.args)
    obs = await session.fetch(sql.observations, *sql.args)
    res = map_rows(m, reg, obs)
    return MappedRead(result=res, queries=sql.queries(), problems=list(res.problems))
