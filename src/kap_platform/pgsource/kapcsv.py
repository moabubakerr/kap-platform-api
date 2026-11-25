"""KAP's indicator CSV format (as loaded by the `csv_loader` stage of an indicators pipeline).

Three files with fixed headers (order taken from the sandbox files on the test
stand): registry.csv, observations.csv, narratives.csv. UTF-8 with BOM, comma,
`.` decimals, RFC 4180 quoting (CRLF rows, quotes doubled), empty = blank.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import re
import tempfile
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterable, Sequence

FILES = ("registry", "observations", "narratives")

HEADERS: dict[str, tuple[str, ...]] = {
    "registry": (
        "indicator_id", "name_en", "name_ar", "frequency", "unit", "dimension", "geography", "seasonal_adjustment",
        "price_basis", "definition", "methodology", "source", "sector_id", "programme_id", "project_id",
        "cms_benchmark_countries", "cms_country", "cms_detail", "cms_intervals", "cms_parent",
        "source_document_metadata", "source_documents",
    ),
    "observations": (
        "indicator_id", "period", "dimension", "unit", "value", "target_value", "baseline_value", "baseline_year",
        "revision_status", "approval_status", "publication_status", "snapshot_date",
    ),
    "narratives": (
        "indicator_id", "language", "period", "summary", "details", "additional_info", "benchmark", "source", "snapshot_date",
    ),
}

# Columns a custom query must return; the rest may be omitted and are written blank.
REQUIRED: dict[str, frozenset[str]] = {
    "registry": frozenset({"indicator_id", "name_en", "frequency"}),
    "observations": frozenset({"indicator_id", "period", "value"}),
    "narratives": frozenset({"indicator_id", "language", "period"}),
}

FREQUENCY = {"yearly": "A", "quarterly": "Q", "monthly": "M"}


# ---------------------------------------------------------------- ids and periods
def format_period(d: date | datetime | None, granularity: str | None) -> str | None:
    """period_date + granularity → KAP period: 'YYYY', 'YYYY-Qn' or 'YYYY-MM'."""
    if d is None or granularity is None:
        return None
    g = granularity.strip().lower()
    if g == "yearly":
        return f"{d.year:04d}"
    if g == "quarterly":
        return f"{d.year:04d}-Q{(d.month - 1) // 3 + 1}"
    if g == "monthly":
        return f"{d.year:04d}-{d.month:02d}"
    return None


def slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.strip().lower()).strip("_") or "unknown"


def indicator_id(cms_detail_id: str, granularity: str, dimension: str | None = None) -> str:
    """`cms.<detail id, lower-case, no dashes>.<a|q|m>.<overall | dimension slug>`.

    72C12E80-DA0C-58CD-81C3-B7EC98B82BB0, yearly → cms.72c12e80da0c58cd81c3b7ec98b82bb0.a.overall
    """
    hex_id = cms_detail_id.strip().lower().replace("-", "").strip("{}")
    freq = FREQUENCY[granularity.strip().lower()].lower()
    return f"cms.{hex_id}.{freq}.{slug(dimension) if dimension else 'overall'}"


# ---------------------------------------------------------------- values
def format_value(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return ""
        v = Decimal(repr(v))
    if isinstance(v, Decimal):
        if not v.is_finite():
            return ""
        s = format(v, "f")
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        return "0" if s in ("-0", "") else s
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False, separators=(",", ":"))
    return str(v)


# ---------------------------------------------------------------- custom query columns
class ColumnError(ValueError):
    pass


def column_map(file: str, columns: Sequence[str]) -> list[int | None]:
    """For a query's result columns, the source index of each KAP column (None = blank).
    Raises ColumnError for unknown, duplicate or missing required columns."""
    header = HEADERS[file]
    names = [c.strip().lower() for c in columns]
    unknown = [c for c in names if c not in header]
    if unknown:
        raise ColumnError(f"{file}: unknown columns {', '.join(unknown)} (allowed: {', '.join(header)})")
    dupes = sorted({c for c in names if names.count(c) > 1})
    if dupes:
        raise ColumnError(f"{file}: duplicate columns {', '.join(dupes)}")
    missing = sorted(REQUIRED[file] - set(names))
    if missing:
        raise ColumnError(f"{file}: missing required columns {', '.join(missing)}")
    return [names.index(h) if h in names else None for h in header]


def remap(row: Sequence[Any], mapping: list[int | None]) -> list[Any]:
    return [row[i] if i is not None else None for i in mapping]


# ---------------------------------------------------------------- writer
class CsvOutput:
    """One KAP CSV file, written to a spooled temp file while hashing nothing until close."""

    def __init__(self, name: str, spool_bytes: int = 16 * 1024 * 1024):
        if name not in HEADERS:
            raise ValueError(f"unknown KAP file {name}")
        self.name = name
        self.filename = f"{name}.csv"
        self.rows = 0
        self.bytes = 0
        self.sha256 = ""
        self._raw = tempfile.SpooledTemporaryFile(max_size=spool_bytes, mode="w+b")
        self._text = io.TextIOWrapper(self._raw, encoding="utf-8-sig", newline="", write_through=True)
        self._w = csv.writer(self._text, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL, lineterminator="\r\n")
        self._w.writerow(HEADERS[name])
        self._closed = False

    def write(self, values: Sequence[Any]) -> None:
        if len(values) != len(HEADERS[self.name]):
            raise ValueError(f"{self.filename}: expected {len(HEADERS[self.name])} values, got {len(values)}")
        self._w.writerow([format_value(v) for v in values])
        self.rows += 1

    def write_many(self, rows: Iterable[Sequence[Any]]) -> None:
        for r in rows:
            self.write(r)

    def close(self) -> None:
        if self._closed:
            return
        self._text.flush()
        self._text.detach()
        self._raw.seek(0)
        h = hashlib.sha256()
        n = 0
        while chunk := self._raw.read(1 << 20):
            h.update(chunk)
            n += len(chunk)
        self.sha256, self.bytes = h.hexdigest(), n
        self._raw.seek(0)
        self._closed = True

    def fileobj(self):
        """The finished file, positioned at the start."""
        self.close()
        self._raw.seek(0)
        return self._raw

    def getvalue(self) -> bytes:
        data = self.fileobj().read()
        self._raw.seek(0)
        return data

    def discard(self) -> None:
        self._raw.close()


def write_rows(name: str, rows: Iterable[Sequence[Any]]) -> CsvOutput:
    out = CsvOutput(name)
    out.write_many(rows)
    out.close()
    return out
