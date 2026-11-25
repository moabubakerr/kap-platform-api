"""Read-only access to a customer PostgreSQL database.

Every session is one `BEGIN READ ONLY` transaction (plus
`default_transaction_read_only`), `statement_timeout` 60 s, connect timeout
10 s. Large results are streamed with a server-side cursor. The SELECT-only
check on user SQL is a convenience on top of that, not the security boundary:
use a read-only database login.
"""
from __future__ import annotations

import re
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, AsyncIterator, Sequence

import asyncpg

CONNECT_TIMEOUT_S = 10
STATEMENT_TIMEOUT_MS = 60_000
PREVIEW_MAX = 50
SSLMODES = ("disable", "allow", "prefer", "require", "verify-ca", "verify-full")


@dataclass
class SourceParams:
    host: str
    port: int
    database: str
    username: str
    password: str | None
    sslmode: str = "prefer"


class SqlRejected(ValueError):
    pass


class SourceUnavailable(Exception):
    """Could not connect (network, auth, TLS, timeout)."""


# ---------------------------------------------------------------- SELECT-only check
_FORBIDDEN = re.compile(
    r"\b(insert|update|delete|merge|truncate|drop|alter|create|grant|revoke|copy|call|vacuum|reindex|cluster|"
    r"refresh|lock|listen|notify|prepare|execute|deallocate|discard|into)\b",
    re.IGNORECASE,
)


def _strip_sql(sql: str) -> str:
    """Remove comments and the contents of string literals / quoted identifiers."""
    out, i, n = [], 0, len(sql)
    while i < n:
        c = sql[i]
        if sql.startswith("--", i):
            j = sql.find("\n", i)
            i = n if j < 0 else j
        elif sql.startswith("/*", i):
            j = sql.find("*/", i + 2)
            i = n if j < 0 else j + 2
            out.append(" ")
        elif c in ("'", '"'):
            j = i + 1
            while j < n:
                if sql[j] == c:
                    if j + 1 < n and sql[j + 1] == c:  # doubled quote
                        j += 2
                        continue
                    break
                j += 1
            out.append(c + c)  # keep an empty literal so the shape stays valid
            i = j + 1
        elif c == "$":
            m = re.match(r"\$([A-Za-z_][A-Za-z0-9_]*)?\$", sql[i:])
            if m:
                tag = m.group(0)
                j = sql.find(tag, i + len(tag))
                i = n if j < 0 else j + len(tag)
                out.append("''")
            else:
                out.append(c)
                i += 1
        else:
            out.append(c)
            i += 1
    return "".join(out)


def check_select(sql: str) -> str:
    """Accept a single SELECT / WITH … SELECT statement. Returns it without a trailing ';'."""
    if not sql or not sql.strip():
        raise SqlRejected("The query is empty.")
    bare = _strip_sql(sql).strip()
    while bare.endswith(";"):
        bare = bare[:-1].rstrip()
    if ";" in bare:
        raise SqlRejected("Only a single statement is allowed.")
    first = re.match(r"\(*\s*([A-Za-z]+)", bare)
    if not first or first.group(1).lower() not in ("select", "with", "values", "table"):
        raise SqlRejected("Only SELECT (or WITH … SELECT) queries are allowed.")
    bad = _FORBIDDEN.search(bare)
    if bad:
        raise SqlRejected(f"'{bad.group(1).upper()}' is not allowed in a read-only query.")
    # return the original text minus trailing semicolons/whitespace
    text = sql.strip()
    while text.endswith(";"):
        text = text[:-1].rstrip()
    return text


# ---------------------------------------------------------------- JSON-safe values for previews
def jsonable(v: Any) -> Any:
    if v is None or isinstance(v, (bool, int, str)):
        return v
    if isinstance(v, float):
        return v if v == v and v not in (float("inf"), float("-inf")) else None
    if isinstance(v, Decimal):
        return format(v, "f") if v.is_finite() else None
    if isinstance(v, (datetime, date, time)):
        return v.isoformat()
    if isinstance(v, timedelta):
        return str(v)
    if isinstance(v, uuid.UUID):
        return str(v)
    if isinstance(v, (bytes, bytearray, memoryview)):
        return f"<{len(bytes(v))} bytes>"
    if isinstance(v, (list, tuple)):
        return [jsonable(x) for x in v]
    if isinstance(v, dict):
        return {str(k): jsonable(x) for k, x in v.items()}
    return str(v)


# ---------------------------------------------------------------- session
class SourceSession:
    """One read-only transaction on a source database."""

    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn

    async def server_version(self) -> str:
        return await self.conn.fetchval("show server_version")

    async def tables(self) -> list[dict[str, Any]]:
        rows = await self.conn.fetch(
            """
            select n.nspname as schema, c.relname as name,
                   case when c.relkind in ('r', 'p', 'm') and c.reltuples >= 0 then c.reltuples::bigint end as rows_estimate
              from pg_class c join pg_namespace n on n.oid = c.relnamespace
             where c.relkind in ('r', 'p', 'v', 'm', 'f')
               and n.nspname not in ('pg_catalog', 'information_schema') and n.nspname not like 'pg\\_%'
               and has_table_privilege(c.oid, 'select')
             order by 1, 2
            """
        )
        return [dict(r) for r in rows]

    async def fetch(self, sql: str, *args: Any, savepoint: bool = False) -> list[Any]:
        if savepoint:
            async with self.conn.transaction():
                return await self.conn.fetch(sql, *args)
        return await self.conn.fetch(sql, *args)

    async def sample(self, sql: str, limit: int) -> tuple[list[str], list[tuple], bool]:
        """Columns and the first `limit` rows of a query; truncated if more exist."""
        async with self.conn.transaction():  # savepoint: an error here leaves the session usable
            stmt = await self.conn.prepare(sql)
            cols = [a.name for a in stmt.get_attributes()]
            cur = await stmt.cursor()
            rows = await cur.fetch(limit + 1)
        return cols, [tuple(r) for r in rows[:limit]], len(rows) > limit

    async def count(self, sql: str) -> int:
        async with self.conn.transaction():
            return await self.conn.fetchval(f"select count(*) from ({sql}\n) as _kap_q")

    @asynccontextmanager
    async def stream(self, sql: str, prefetch: int = 2000) -> AsyncIterator[tuple[list[str], AsyncIterator[Sequence[Any]]]]:
        stmt = await self.conn.prepare(sql)
        cols = [a.name for a in stmt.get_attributes()]

        async def rows() -> AsyncIterator[Sequence[Any]]:
            async for r in stmt.cursor(prefetch=prefetch):
                yield tuple(r)

        yield cols, rows()


@asynccontextmanager
async def open_source(p: SourceParams) -> AsyncIterator[SourceSession]:
    if p.sslmode not in SSLMODES:
        raise SourceUnavailable(f"unknown sslmode {p.sslmode}")
    try:
        conn = await asyncpg.connect(
            host=p.host,
            port=p.port,
            database=p.database,
            user=p.username,
            password=p.password or None,
            ssl=p.sslmode,
            timeout=CONNECT_TIMEOUT_S,
            command_timeout=STATEMENT_TIMEOUT_MS / 1000 + 5,
            statement_cache_size=0,
            server_settings={
                "application_name": "kap-platform-export",
                "default_transaction_read_only": "on",
                "statement_timeout": str(STATEMENT_TIMEOUT_MS),
            },
        )
    except (OSError, TimeoutError, asyncpg.PostgresError, asyncpg.InterfaceError) as exc:
        raise SourceUnavailable(_describe(exc)) from exc
    try:
        async with conn.transaction(readonly=True):
            await conn.execute(f"set local statement_timeout = {STATEMENT_TIMEOUT_MS}")
            yield SourceSession(conn)
    finally:
        await conn.close()


def _describe(exc: BaseException) -> str:
    if isinstance(exc, (TimeoutError,)):
        return f"connection timed out after {CONNECT_TIMEOUT_S} s"
    msg = str(exc) or type(exc).__name__
    return f"{type(exc).__name__}: {msg}"
