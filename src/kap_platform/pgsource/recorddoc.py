"""Render a record card as a .docx (python-docx) with deterministic bytes.

Layout: Heading 1 = title; one "Label: value" paragraph per field (label bold);
Heading 2 + paragraphs per section; a last "Source: <table> · record <id>" line
(in the body, not the page footer, so KAP's docx parser keeps it).

Deterministic: the core properties (created / modified / last printed, revision,
author) are fixed and the package is zipped once with a fixed timestamp
(1980-01-01) and a stable entry order. Identical cards → identical bytes: a
re-uploaded unchanged record is the same object for KAP, and an object's ETag
(MD5) can be checked against a fresh render when no stored hash exists.

Change detection itself uses `content_hash()` (what the card shows + RENDER_VERSION),
so unchanged records are never rendered (~15–35 ms per document with python-docx).
"""
from __future__ import annotations

import hashlib
import io
import json
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from docx import Document
from docx.opc.pkgwriter import PackageWriter
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Pt
from docx.text.paragraph import Paragraph

from .records import Card, paragraphs

RENDER_VERSION = 1  # part of content_hash(): bump when the layout changes so every document is uploaded again
FIXED_TIME = datetime(2000, 1, 1, tzinfo=timezone.utc)
ZIP_TIME = (1980, 1, 1, 0, 0, 0)
AUTHOR = "KAP platform"


@dataclass(frozen=True)
class RenderedDoc:
    sha256: str
    md5: str
    bytes: int


def _rtl(p: Paragraph) -> None:
    """Right-to-left paragraph (w:bidi) and runs (w:rtl)."""
    p._p.get_or_add_pPr().append(OxmlElement("w:bidi"))
    for r in p.runs:
        r._r.get_or_add_rPr().append(OxmlElement("w:rtl"))


def _text_runs(p: Paragraph, text: str) -> None:
    """Text with line breaks inside a paragraph."""
    lines = text.split("\n")
    run = p.add_run(lines[0])
    for line in lines[1:]:
        run.add_break()
        run = p.add_run(line)


class _Collect:
    """Stands in for python-docx's zip writer: keeps the package items in memory (zipped once, by _pack)."""

    def __init__(self) -> None:
        self.items: dict[str, bytes] = {}

    def write(self, pack_uri: Any, blob: bytes) -> None:
        self.items[pack_uri.membername] = blob


def _items(doc: Any) -> dict[str, bytes]:
    """What Document.save() would zip (python-docx 1.x OpcPackage.save / PackageWriter.write), without zipping."""
    pkg = doc.part.package
    for part in pkg.parts:
        part.before_marshal()
    out = _Collect()
    PackageWriter._write_content_types_stream(out, pkg.parts)
    PackageWriter._write_pkg_rels(out, pkg.rels)
    PackageWriter._write_parts(out, pkg.parts)
    return out.items


def _pack(parts: dict[str, bytes]) -> bytes:
    """Zip with fixed timestamps and order: [Content_Types].xml, _rels/.rels, then by name."""
    first = ["[Content_Types].xml", "_rels/.rels"]
    order = [n for n in first if n in parts] + sorted(n for n in parts if n not in first)
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        for name in order:
            info = zipfile.ZipInfo(name, date_time=ZIP_TIME)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o600 << 16
            info.create_system = 0
            zout.writestr(info, parts[name], compresslevel=6)
    return out.getvalue()


def render(card: Card) -> bytes:
    doc = Document()
    rtl = card.language == "ar"

    cp = doc.core_properties
    cp.author = AUTHOR
    cp.last_modified_by = AUTHOR
    cp.title = card.title[:255]
    cp.subject = f"{card.source} · record {card.id}"[:255]
    cp.language = card.language
    cp.created = FIXED_TIME
    cp.modified = FIXED_TIME
    cp.last_printed = FIXED_TIME
    cp.revision = 1
    cp.comments = ""
    cp.keywords = ""
    cp.category = ""

    styled: list[Paragraph] = []
    styled.append(doc.add_heading(card.title, level=1))
    for label, value in card.fields:
        p = doc.add_paragraph()
        p.add_run(f"{label}: ").bold = True
        p.add_run(value)
        styled.append(p)
    for label, body in card.sections:
        styled.append(doc.add_heading(label, level=2))
        for para in paragraphs(body):
            p = doc.add_paragraph()
            _text_runs(p, para)
            styled.append(p)
    foot = doc.add_paragraph()
    run = foot.add_run(card.footer)
    run.italic = True
    run.font.size = Pt(8)
    styled.append(foot)
    if rtl:
        for p in styled:
            _rtl(p)

    return _pack(_items(doc))


def digest(data: bytes) -> RenderedDoc:
    return RenderedDoc(hashlib.sha256(data).hexdigest(), hashlib.md5(data).hexdigest(), len(data))


def content_hash(card: Card) -> str:
    """Hash of what the document shows (+ RENDER_VERSION): equal hash ⇔ equal document, without rendering.
    Stored per record by each run; bump RENDER_VERSION whenever render() changes its output."""
    payload = [RENDER_VERSION, card.id, card.slug, card.title, card.fields, card.sections, card.source, card.language]
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")).hexdigest()
