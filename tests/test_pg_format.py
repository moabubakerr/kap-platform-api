"""KAP CSV format, ids, periods, custom columns, SELECT-only check, password encryption."""
import csv
import io
from datetime import date
from decimal import Decimal

import pytest
from cryptography.fernet import Fernet

from kap_platform.pgsource import kapcsv
from kap_platform.pgsource.secrets import SecretBox, SecretKeyMissing
from kap_platform.pgsource.source import SqlRejected, check_select


# ---------------------------------------------------------------- headers / writer
def test_headers_are_the_sandbox_headers_in_order():
    assert ",".join(kapcsv.HEADERS["registry"]) == (
        "indicator_id,name_en,name_ar,frequency,unit,dimension,geography,seasonal_adjustment,price_basis,definition,methodology,"
        "source,sector_id,programme_id,project_id,cms_benchmark_countries,cms_country,cms_detail,cms_intervals,cms_parent,"
        "source_document_metadata,source_documents"
    )
    assert ",".join(kapcsv.HEADERS["observations"]) == (
        "indicator_id,period,dimension,unit,value,target_value,baseline_value,baseline_year,revision_status,approval_status,publication_status,snapshot_date"
    )
    assert ",".join(kapcsv.HEADERS["narratives"]) == "indicator_id,language,period,summary,details,additional_info,benchmark,source,snapshot_date"


def test_writer_bom_crlf_quoting_and_decimals():
    out = kapcsv.write_rows("observations", [
        ["cms.72c12e80da0c58cd81c3b7ec98b82bb0.a.overall", "2019", None, "", Decimal("0.70"), None, None, None, None, None, "published", None],
        ['id,"x"', "2024-Q1", "line1\nline2", "%", 1.5, Decimal("1E+2"), 0, 2018, "", "", "published", date(2024, 1, 31)],
    ])
    data = out.getvalue()
    assert data.startswith(b"\xef\xbb\xbf")  # UTF-8 BOM
    text = data[3:].decode("utf-8")
    lines = text.split("\r\n")
    assert lines[0] == ",".join(kapcsv.HEADERS["observations"])
    assert lines[1] == "cms.72c12e80da0c58cd81c3b7ec98b82bb0.a.overall,2019,,,0.7,,,,,,published,"
    rows = list(csv.reader(io.StringIO(text, newline="")))
    assert rows[2] == ['id,"x"', "2024-Q1", "line1\nline2", "%", "1.5", "100", "0", "2018", "", "", "published", "2024-01-31"]
    assert '"id,""x"""' in text  # RFC 4180: quoted, quotes doubled
    assert out.rows == 2 and out.bytes == len(data) and len(out.sha256) == 64


def test_writer_hash_is_stable_and_detects_changes():
    a = kapcsv.write_rows("narratives", [["i", "en", "2019", "s", "", "", "", "", ""]])
    b = kapcsv.write_rows("narratives", [["i", "en", "2019", "s", "", "", "", "", ""]])
    c = kapcsv.write_rows("narratives", [["i", "en", "2019", "t", "", "", "", "", ""]])
    assert a.sha256 == b.sha256 != c.sha256


def test_writer_rejects_wrong_width():
    out = kapcsv.CsvOutput("narratives")
    with pytest.raises(ValueError):
        out.write(["only", "three", "values"])


def test_arabic_text_survives():
    out = kapcsv.write_rows("narratives", [["i", "ar", "2019", "ملخص", "", "", "", "", ""]])
    assert "ملخص" in out.getvalue().decode("utf-8-sig")


@pytest.mark.parametrize("value,expected", [
    (None, ""), (Decimal("32.040"), "32.04"), (Decimal("50"), "50"), (Decimal("-0.0"), "0"), (0.1, "0.1"), (1e-7, "0.0000001"),
    (float("nan"), ""), (True, "true"), (7, "7"), ([], "[]"), ({"a": "ب"}, '{"a":"ب"}'), (date(2024, 3, 1), "2024-03-01"),
])
def test_format_value(value, expected):
    assert kapcsv.format_value(value) == expected


# ---------------------------------------------------------------- periods and ids
@pytest.mark.parametrize("d,g,expected", [
    (date(2019, 1, 1), "yearly", "2019"),
    (date(2024, 1, 1), "quarterly", "2024-Q1"),
    (date(2024, 4, 1), "quarterly", "2024-Q2"),
    (date(2024, 9, 30), "quarterly", "2024-Q3"),
    (date(2024, 10, 1), "Quarterly", "2024-Q4"),
    (date(2024, 3, 1), "monthly", "2024-03"),
    (date(2024, 3, 1), "weekly", None),
    (None, "yearly", None),
])
def test_format_period(d, g, expected):
    assert kapcsv.format_period(d, g) == expected


def test_indicator_id_scheme():
    assert kapcsv.indicator_id("72C12E80-DA0C-58CD-81C3-B7EC98B82BB0", "yearly") == "cms.72c12e80da0c58cd81c3b7ec98b82bb0.a.overall"
    assert kapcsv.indicator_id("72c12e80-da0c-58cd-81c3-b7ec98b82bb0", "quarterly") == "cms.72c12e80da0c58cd81c3b7ec98b82bb0.q.overall"
    assert kapcsv.indicator_id("72C12E80-DA0C-58CD-81C3-B7EC98B82BB0", "monthly", "United Arab Emirates") == "cms.72c12e80da0c58cd81c3b7ec98b82bb0.m.united_arab_emirates"


# ---------------------------------------------------------------- custom query columns
def test_column_map_fills_optional_blank_and_reorders():
    m = kapcsv.column_map("observations", ["VALUE", "indicator_id", "period", "target_value"])
    row = kapcsv.remap((1.5, "x", "2019", 2), m)
    assert row[:6] == ["x", "2019", None, None, 1.5, 2] and len(row) == len(kapcsv.HEADERS["observations"])


def test_column_map_rejects_unknown_duplicate_and_missing():
    with pytest.raises(kapcsv.ColumnError, match="unknown columns extra"):
        kapcsv.column_map("observations", ["indicator_id", "period", "value", "extra"])
    with pytest.raises(kapcsv.ColumnError, match="duplicate"):
        kapcsv.column_map("observations", ["indicator_id", "period", "value", "value"])
    with pytest.raises(kapcsv.ColumnError, match="missing required columns frequency"):
        kapcsv.column_map("registry", ["indicator_id", "name_en"])


# ---------------------------------------------------------------- SELECT-only
@pytest.mark.parametrize("sql", [
    "select 1",
    "SELECT * FROM indicators;",
    "  with x as (select 1 as a) select a from x ;  ",
    "(select 1) union (select 2)",
    "select 'drop table x; insert' as note, updated_at, last_update from t",
    "-- comment; delete\nselect 1",
    'select "update" from t',
    "select $$;delete$$",
])
def test_check_select_accepts(sql):
    assert check_select(sql).rstrip().endswith(";") is False


@pytest.mark.parametrize("sql", [
    "", "   ", "delete from t", "update t set a = 1", "select 1; select 2", "select 1; drop table t",
    "with d as (delete from t returning *) select * from d", "select * into new_t from t", "insert into t values (1)",
    "copy t to stdout", "set role postgres", "call p()", "select * from t for update",
])
def test_check_select_rejects(sql):
    with pytest.raises(SqlRejected):
        check_select(sql)


# ---------------------------------------------------------------- secrets
def test_fernet_round_trip():
    box = SecretBox(Fernet.generate_key().decode())
    token = box.encrypt("s3cr3t-ü")
    assert token != "s3cr3t-ü" and box.decrypt(token) == "s3cr3t-ü"


def test_fernet_wrong_or_missing_key():
    token = SecretBox(Fernet.generate_key().decode()).encrypt("x")
    with pytest.raises(SecretKeyMissing):
        SecretBox(Fernet.generate_key().decode()).decrypt(token)
    with pytest.raises(SecretKeyMissing):
        SecretBox("")
    with pytest.raises(SecretKeyMissing):
        SecretBox("not-a-key")
