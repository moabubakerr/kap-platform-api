"""`records` preset: each row of one table/view → a small Word document (a "record card").

The cards are uploaded one per folder (`<tenant>/<dataset>/<record slug>/record.docx`) and
loaded into a KAP *document* collection whose entity grouping is "folder at depth 1", so
every record is its own document group with its own summary for `hybrid_search`.

Like `mapped`, the user never writes SQL: the spec names columns, they are checked
against information_schema, identifiers are quoted and filter values are $n parameters
(helpers shared with mapping.py).

  records = {table, id_column, title: "Ticket {ticket_id}", fields: [{column, label}],
             sections: [{column, label}], filters?, order_by?: {column, desc},
             max_records (default 5000, ≤ 50000), language?: "en" | "ar"}
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Iterable, Mapping

from .mapping import check_filters, normalize_filters, quote_ident, resolve_table, where_clause

KEYS = ("table", "id_column", "title", "fields", "sections", "filters", "order_by", "max_records", "language")
MAX_DEFAULT = 5000
MAX_LIMIT = 50000
LANGUAGES = ("en", "ar")
SLUG_MAX = 64
TITLE_MAX = 300
LABEL_MAX = 200
PREVIEW_SAMPLE = 5
DOC_NAME = "record.docx"
DOCX_TYPE = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
PLACEHOLDER = re.compile(r"\{([^{}]+)\}")
UNUSABLE_ID_TYPES = frozenset({"bytea", "json", "jsonb", "xml"})
_CONTROL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f￾￿]")


# ---------------------------------------------------------------- shape validation
def normalize(raw: Any) -> tuple[dict[str, Any] | None, list[str]]:
    """Check the spec's shape (no database needed). Returns (clean spec, problems)."""
    if not isinstance(raw, Mapping):
        return None, ["records: an object is required"]
    problems: list[str] = []
    out: dict[str, Any] = {}
    for k in raw:
        if k not in KEYS:
            problems.append(f'records: unknown field "{k}"')

    def column(value: Any, label: str) -> str | None:
        if value is None or (isinstance(value, str) and not value.strip()):
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

    idc = column(raw.get("id_column"), "id_column")
    if idc:
        out["id_column"] = idc

    title = raw.get("title")
    if not isinstance(title, str) or not title.strip():
        problems.append('title: enter a title, e.g. "Ticket {ticket_id}"')
    elif len(title.strip()) > TITLE_MAX:
        problems.append(f"title: at most {TITLE_MAX} characters")
    else:
        out["title"] = title.strip()
        for name in PLACEHOLDER.findall(out["title"]):
            if not name.strip():
                problems.append("title: an empty {} placeholder")

    for part in ("fields", "sections"):
        items = raw.get(part)
        if items is None:
            out[part] = []
            continue
        if not isinstance(items, list):
            problems.append(f"{part}: must be a list")
            continue
        clean = []
        for i, it in enumerate(items, 1):
            if not isinstance(it, Mapping):
                problems.append(f"{part}: item {i} must be an object")
                continue
            c = column(it.get("column"), f"{part}: item {i}")
            label = it.get("label")
            if label is None or (isinstance(label, str) and not label.strip()):
                label = c
            elif not isinstance(label, str):
                problems.append(f"{part}: item {i}: the label must be text")
                continue
            elif len(label.strip()) > LABEL_MAX:
                problems.append(f"{part}: item {i}: the label is longer than {LABEL_MAX} characters")
                continue
            if c:
                clean.append({"column": c, "label": label.strip()})
        out[part] = clean
    if not problems and not out["fields"] and not out["sections"]:
        problems.append("fields / sections: choose at least one column to show in the document")

    filters = normalize_filters(raw.get("filters"), problems)
    if filters is not None:
        out["filters"] = filters

    ob = raw.get("order_by")
    if ob is not None:
        if not isinstance(ob, Mapping):
            problems.append("order_by: must be {column, desc}")
        else:
            c = column(ob.get("column"), "order_by")
            desc = ob.get("desc", False)
            if not isinstance(desc, bool):
                problems.append("order_by: desc must be true or false")
            elif c:
                out["order_by"] = {"column": c, "desc": desc}

    mx = raw.get("max_records")
    if mx is None:
        out["max_records"] = MAX_DEFAULT
    elif isinstance(mx, bool) or not isinstance(mx, int) or not 1 <= mx <= MAX_LIMIT:
        problems.append(f"max_records: a whole number from 1 to {MAX_LIMIT}")
    else:
        out["max_records"] = mx

    lang = raw.get("language")
    if lang is None or lang == "":
        out["language"] = "en"
    elif lang not in LANGUAGES:
        problems.append(f'language: "en" or "ar", not {lang!r}')
    else:
        out["language"] = lang

    return (None, problems) if problems else (out, [])


def title_columns(r: Mapping[str, Any]) -> list[str]:
    return [n.strip() for n in PLACEHOLDER.findall(r["title"])]


def used_columns(r: Mapping[str, Any]) -> list[str]:
    """id first, then every column shown in the card (each once)."""
    cols = [r["id_column"], *title_columns(r), *(f["column"] for f in r["fields"]), *(s["column"] for s in r["sections"])]
    return list(dict.fromkeys(cols))


# ---------------------------------------------------------------- column validation
def check_columns(r: Mapping[str, Any], columns: Iterable[Mapping[str, Any]]) -> list[str]:
    types = {c["name"]: str(c["type"]).lower() for c in columns}
    problems: list[str] = []

    def known(label: str, col: str) -> str | None:
        if col not in types:
            problems.append(f'{label}: column "{col}" does not exist in {r["table"]}')
            return None
        return types[col]

    t = known("id_column", r["id_column"])
    if t in UNUSABLE_ID_TYPES:
        problems.append(f'id_column: column "{r["id_column"]}" is {t}; choose a text, number, uuid or date column')
    for c in title_columns(r):
        known("title", c)
    for part in ("fields", "sections"):
        for it in r[part]:
            known(part, it["column"])
    if "order_by" in r:
        known("order_by", r["order_by"]["column"])
    problems += check_filters(r["table"], r["filters"], types)
    return problems


# ---------------------------------------------------------------- SQL
@dataclass
class RecordsSql:
    select: str  # the export query (limit max_records + 1)
    count: str
    args: list[str] = field(default_factory=list)
    columns: list[str] = field(default_factory=list)  # column of alias c<i>

    def sample(self, n: int) -> str:
        return re.sub(r"\n limit \d+$", f"\n limit {int(n)}", self.select)


def build_sql(r: Mapping[str, Any], schema: str, table: str) -> RecordsSql:
    """Only quoted identifiers, $n parameters and integer literals; no user text in the SQL."""
    source = f"{quote_ident(schema)}.{quote_ident(table)}"
    cols = used_columns(r)
    args: list[str] = []
    where = where_clause(r["filters"], args)
    select = ",\n       ".join(f"{quote_ident(c)} as c{i}" for i, c in enumerate(cols))
    idq = quote_ident(r["id_column"])
    order = [f"{idq}"]
    if "order_by" in r:
        ob = quote_ident(r["order_by"]["column"])
        order.insert(0, f"{ob} {'desc' if r['order_by']['desc'] else 'asc'} nulls last")
    sql = f"select {select}\n  from {source}{where}\n order by {', '.join(order)}\n limit {int(r['max_records']) + 1}"
    count = f"select count(*) as n\n  from {source}{where}"
    return RecordsSql(sql, count, args, cols)


# ---------------------------------------------------------------- values
def clean_text(s: str) -> str:
    return _CONTROL.sub("", s)


def format_value(v: Any) -> str | None:
    """A cell as card text; None for empty values (the line / section is omitted)."""
    if v is None:
        return None
    if isinstance(v, bool):
        return "Yes" if v else "No"
    if isinstance(v, str):
        s = clean_text(v).strip()
        return s or None
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        return None if math.isnan(v) or math.isinf(v) else repr(v)
    if isinstance(v, Decimal):
        return format(v, "f") if v.is_finite() else None
    if isinstance(v, (datetime, date, time)):
        return v.isoformat()
    if isinstance(v, timedelta):
        return str(v)
    if isinstance(v, uuid.UUID):
        return str(v)
    if isinstance(v, (bytes, bytearray, memoryview)):
        return None
    if isinstance(v, (list, tuple)):
        parts = [p for p in (format_value(x) for x in v) if p]
        return ", ".join(parts) or None
    if isinstance(v, dict):
        return clean_text(json.dumps(v, ensure_ascii=False, sort_keys=True, default=str)) if v else None
    return format_value(str(v))


def id_text(v: Any) -> str:
    return format_value(v) or ""


# ---------------------------------------------------------------- keys
def record_slug(id_value: str) -> str:
    """lower-case `[a-z0-9]` runs joined by "_", ≤ 64 chars. When that changed the id (case, other
    characters) or truncated it, an 8-char hash of the id is appended, so distinct ids get distinct slugs."""
    s = re.sub(r"[^a-z0-9]+", "_", id_value.lower()).strip("_")
    if s == id_value and len(s) <= SLUG_MAX:
        return s
    digest = hashlib.sha1(id_value.encode("utf-8")).hexdigest()[:8]
    base = s[: SLUG_MAX - 9].rstrip("_") or "id"
    return f"{base}_{digest}"


def record_path(slug: str) -> str:
    """Path below the dataset prefix: one folder per record."""
    return f"{slug}/{DOC_NAME}"


# ---------------------------------------------------------------- cards
@dataclass
class Card:
    id: str
    slug: str
    title: str
    fields: list[tuple[str, str]]
    sections: list[tuple[str, str]]
    source: str  # "schema.name"
    language: str = "en"

    @property
    def path(self) -> str:
        return record_path(self.slug)

    def text(self) -> str:
        """Plain-text rendering (preview display)."""
        blocks = [f"# {self.title}"]
        if self.fields:
            blocks.append("\n".join(f"{label}: {value}" for label, value in self.fields))
        for label, body in self.sections:
            blocks.append(f"## {label}")
            blocks += paragraphs(body)
        blocks.append(self.footer)
        return "\n\n".join(blocks) + "\n"

    @property
    def footer(self) -> str:
        return f"Source: {self.source} · record {self.id}"


def paragraphs(text: str) -> list[str]:
    """Section text split on blank lines (line breaks inside a paragraph are kept)."""
    parts = re.split(r"\n[ \t]*\n", text.replace("\r\n", "\n").replace("\r", "\n"))
    return [p.strip("\n").rstrip() for p in parts if p.strip()]


def render_title(template: str, values: Mapping[str, Any], fallback: str) -> str:
    def sub(m: re.Match[str]) -> str:
        return (format_value(values.get(m.group(1).strip())) or "").replace("\n", " ")

    t = re.sub(r"\s+", " ", PLACEHOLDER.sub(sub, template)).strip()
    return t or fallback


def make_card(r: Mapping[str, Any], row: Mapping[str, Any], cols: list[str], slug: str) -> Card:
    values = {c: row[f"c{i}"] for i, c in enumerate(cols)}
    rid = id_text(values[r["id_column"]])
    fields = []
    for f in r["fields"]:
        v = format_value(values.get(f["column"]))
        if v is not None:
            fields.append((f["label"], re.sub(r"\s*\n\s*", " ", v)))
    sections = []
    for s in r["sections"]:
        v = format_value(values.get(s["column"]))
        if v is not None:
            sections.append((s["label"], v))
    return Card(rid, slug, render_title(r["title"], values, f"Record {rid}"), fields, sections, r["table"], r.get("language", "en"))


def cards_from_rows(r: Mapping[str, Any], rows: Iterable[Mapping[str, Any]], cols: list[str]) -> tuple[list[Card], list[str]]:
    """Cards in query order; rows with an empty id and repeated ids are left out (counted in problems)."""
    cards: list[Card] = []
    problems: list[str] = []
    seen: dict[str, str] = {}  # slug → id
    no_id = dupes = 0
    for row in rows:
        rid = id_text(row["c0"])
        if not rid:
            no_id += 1
            continue
        slug = record_slug(rid)
        if slug in seen:
            if seen[slug] == rid:
                dupes += 1
                continue
            slug = f"{slug[:SLUG_MAX - 17]}_{hashlib.sha1(rid.encode('utf-8')).hexdigest()[:16]}"  # practically unreachable
        seen[slug] = rid
        cards.append(make_card(r, row, cols, slug))
    if no_id:
        problems.append(f"records: {no_id} rows with an empty {r['id_column']} were left out")
    if dupes:
        problems.append(f"records: {dupes} rows repeat an {r['id_column']} already seen; only the first was kept (id_column must be unique)")
    return cards, problems


# ---------------------------------------------------------------- against a source session
@dataclass
class RecordsRead:
    cards: list[Card] | None = None
    query: str | None = None
    problems: list[str] = field(default_factory=list)
    total: int | None = None  # preview: matching rows (count query)


async def _checked(session: Any, raw: Any) -> tuple[dict[str, Any] | None, RecordsSql | None, list[str]]:
    r, problems = normalize(raw)
    if r is None:
        return None, None, problems
    found = await resolve_table(session, r["table"])
    if found is None:
        return None, None, [f'table: "{r["table"]}" is not a table or view this login can read']
    problems = check_columns(r, await session.columns(*found))
    if problems:
        return None, None, problems
    return r, build_sql(r, *found), []


async def read(session: Any, raw: Any) -> RecordsRead:
    """Validate the spec, run the query, build the cards (≤ max_records). Spec problems → cards None.
    asyncpg.PostgresError propagates."""
    r, sql, problems = await _checked(session, raw)
    if r is None or sql is None:
        return RecordsRead(problems=problems)
    rows = await session.fetch(sql.select, *sql.args)
    problems = []
    if len(rows) > r["max_records"]:
        rows = rows[: r["max_records"]]
        problems.append(f"records: more rows match than max_records ({r['max_records']:,}); only the first {r['max_records']:,} in the chosen order were exported")
    cards, more = cards_from_rows(r, rows, sql.columns)
    return RecordsRead(cards=cards, query=sql.select, problems=problems + more)


async def preview(session: Any, raw: Any, limit: int = PREVIEW_SAMPLE) -> RecordsRead:
    """Count + the first `limit` cards."""
    r, sql, problems = await _checked(session, raw)
    if r is None or sql is None:
        return RecordsRead(problems=problems)
    count_rows = await session.fetch(sql.count, *sql.args)
    total = int(count_rows[0]["n"]) if count_rows else 0
    rows = await session.fetch(sql.sample(limit), *sql.args)
    cards, problems = cards_from_rows(r, rows, sql.columns)
    if total > r["max_records"]:
        problems.insert(0, f"records: {total:,} rows match but max_records is {r['max_records']:,}; only the first {r['max_records']:,} in the chosen order will be exported")
    return RecordsRead(cards=cards, query=sql.select, problems=problems, total=min(total, r["max_records"]))
