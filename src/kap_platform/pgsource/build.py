"""Produce the three KAP files from a source session: full export or a preview sample."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Mapping

import asyncpg

from . import mapping, presets, records
from .kapcsv import FILES, HEADERS, ColumnError, CsvOutput, column_map, format_value, remap
from .source import SourceSession, SqlRejected, check_select

log = logging.getLogger(__name__)
PREVIEW_ROWS = 20


class ExportError(Exception):
    """The dataset definition cannot be exported as is (bad query, wrong columns)."""


@dataclass
class ReadResult:
    rows: dict[str, list[list[Any]]] = field(default_factory=dict)  # preset: mapped rows, written in write()
    outputs: dict[str, CsvOutput] = field(default_factory=dict)  # custom: already streamed to CSV
    problems: list[str] = field(default_factory=list)

    def counts(self) -> dict[str, int]:
        return {n: (self.outputs[n].rows if n in self.outputs else len(self.rows.get(n, []))) for n in FILES}


async def _read_preset(session: SourceSession, dataset: Mapping[str, Any]) -> presets.PresetResult:
    details = await session.fetch(presets.DETAILS_SQL)
    values = await session.fetch(presets.VALUES_SQL)
    problems: list[str] = []
    try:
        analysis = await session.fetch(presets.NARRATIVES_SQL, savepoint=True)
    except asyncpg.PostgresError as exc:
        analysis = None
        problems.append(f"narratives query failed: {exc}")
    res = presets.map_scai(details, values, analysis, bool(dataset.get("published_only", True)))
    res.problems[:0] = problems
    return res


def _mapping_prefix(dataset: Mapping[str, Any]) -> str:
    """Default id_prefix: the dataset name (stored mappings already carry one)."""
    raw = dataset.get("mapping") or {}
    table = raw.get("table") if isinstance(raw, Mapping) else None
    return mapping.default_prefix(dataset.get("name") or (str(table).rsplit(".", 1)[-1] if table else None))


async def read(session: SourceSession, dataset: Mapping[str, Any]) -> ReadResult:
    """Run the dataset's queries. Preset rows are kept in memory (thousands); custom rows stream to disk."""
    rr = ReadResult()
    if dataset["preset"] == "mapped":
        try:
            mr = await mapping.read(session, dataset.get("mapping"), _mapping_prefix(dataset))
        except asyncpg.PostgresError as exc:
            raise ExportError(f"The column mapping query failed: {exc}") from exc
        if mr.result is None:
            raise ExportError("The column mapping cannot be used: " + "; ".join(mr.problems))
        rr.rows = {n: mr.result.rows(n) for n in FILES}
        rr.problems = mr.problems
        return rr
    if dataset["preset"] == "scai_indicators":
        try:
            res = await _read_preset(session, dataset)
        except asyncpg.PostgresError as exc:
            raise ExportError(f"The scai_indicators preset could not read the source: {exc}") from exc
        rr.rows = {n: res.rows(n) for n in FILES}
        rr.problems = res.problems
        return rr
    queries = dataset.get("queries") or {}
    try:
        for name in FILES:
            out = CsvOutput(name)
            rr.outputs[name] = out
            sql = (queries.get(name) or "").strip()
            if not sql:
                if name == "narratives":
                    rr.problems.append("narratives: no query; narratives.csv contains the header only")
                    continue
                raise ExportError(f"{name}: a query is required")
            try:
                sql = check_select(sql)
                async with session.stream(sql) as (cols, rows):
                    colmap = column_map(name, cols)
                    async for r in rows:
                        out.write(remap(r, colmap))
            except (SqlRejected, ColumnError) as exc:
                raise ExportError(f"{name}: {exc}" if isinstance(exc, SqlRejected) else str(exc)) from exc
            except asyncpg.PostgresError as exc:
                raise ExportError(f"{name}: query failed: {exc}") from exc
    except BaseException:
        for o in rr.outputs.values():
            o.discard()
        raise
    return rr


def write(rr: ReadResult) -> dict[str, CsvOutput]:
    """Finish the files (preset rows are written here). Returns name → closed CsvOutput."""
    outputs: dict[str, CsvOutput] = {}
    for name in FILES:
        out = rr.outputs.get(name)
        if out is None:
            out = CsvOutput(name)
            out.write_many(rr.rows.get(name, []))
        out.close()
        outputs[name] = out
    return outputs


# ---------------------------------------------------------------- preview
def _fmt(rows: list[list[Any]]) -> list[list[str]]:
    return [[format_value(v) for v in r] for r in rows]


async def read_records(session: SourceSession, dataset: Mapping[str, Any]) -> records.RecordsRead:
    """records preset: the cards to export (ExportError when the spec cannot be used)."""
    try:
        rr = await records.read(session, dataset.get("records"))
    except asyncpg.PostgresError as exc:
        raise ExportError(f"The records query failed: {exc}") from exc
    if rr.cards is None:
        raise ExportError("The records definition cannot be used: " + "; ".join(rr.problems))
    return rr


async def preview_records(session: SourceSession, raw: Any, limit: int = records.PREVIEW_SAMPLE) -> dict[str, Any]:
    """{documents: {count, sample: [{key, title, text}]}, problems, query}; nothing is rendered or uploaded."""
    limit = max(1, min(limit, records.PREVIEW_SAMPLE))
    try:
        rr = await records.preview(session, raw, limit)
    except asyncpg.PostgresError as exc:
        return {"documents": {"count": 0, "sample": []}, "problems": [f"The records query failed: {exc}"], "query": None}
    if rr.cards is None:
        return {"documents": {"count": 0, "sample": []}, "problems": rr.problems, "query": None}
    sample = [{"key": c.path, "title": c.title, "text": c.text()} for c in rr.cards[:limit]]
    return {"documents": {"count": rr.total or 0, "sample": sample}, "problems": rr.problems, "query": rr.query}


async def preview(session: SourceSession, dataset: Mapping[str, Any], limit: int = PREVIEW_ROWS) -> dict[str, Any]:
    if dataset["preset"] == "records":
        return await preview_records(session, dataset.get("records"), min(limit, records.PREVIEW_SAMPLE))
    files: dict[str, Any] = {}
    problems: list[str] = []
    if dataset["preset"] == "mapped":
        empty = {n: {"columns": list(HEADERS[n]), "rows": [], "count": 0} for n in FILES}
        try:
            mr = await mapping.read(session, dataset.get("mapping"), _mapping_prefix(dataset))
        except asyncpg.PostgresError as exc:
            return {"files": empty, "problems": [f"The column mapping query failed: {exc}"], "queries": {"registry": None, "observations": None}}
        if mr.result is None:
            return {"files": empty, "problems": mr.problems, "queries": {"registry": None, "observations": None}}
        for n in FILES:
            rows = mr.result.rows(n)
            files[n] = {"columns": list(HEADERS[n]), "rows": _fmt(rows[:limit]), "count": len(rows)}
        return {"files": files, "problems": mr.problems, "queries": mr.queries}
    if dataset["preset"] == "scai_indicators":
        try:
            res = await _read_preset(session, dataset)
        except asyncpg.PostgresError as exc:
            problems.append(f"The scai_indicators preset could not read the source: {exc}")
            return {"files": {n: {"columns": list(HEADERS[n]), "rows": [], "count": 0} for n in FILES}, "problems": problems}
        for n in FILES:
            rows = res.rows(n)
            files[n] = {"columns": list(HEADERS[n]), "rows": _fmt(rows[:limit]), "count": len(rows)}
        return {"files": files, "problems": res.problems}

    queries = dataset.get("queries") or {}
    for name in FILES:
        entry: dict[str, Any] = {"columns": list(HEADERS[name]), "rows": [], "count": 0}
        files[name] = entry
        sql = (queries.get(name) or "").strip()
        if not sql:
            problems.append(f"{name}: no query" + ("; narratives.csv will contain the header only" if name == "narratives" else " (required)"))
            continue
        try:
            sql = check_select(sql)
            cols, rows, _ = await session.sample(sql, limit)
            colmap = column_map(name, cols)
            entry["rows"] = _fmt([remap(r, colmap) for r in rows])
            entry["count"] = await session.count(sql)
        except (SqlRejected, ColumnError) as exc:
            problems.append(f"{name}: {exc}" if isinstance(exc, SqlRejected) else str(exc))
        except asyncpg.PostgresError as exc:
            problems.append(f"{name}: query failed: {exc}")
    return {"files": files, "problems": problems}
