"""asyncpg pool and a small SQL migration runner (migrations/NNNN_name.sql)."""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import asyncpg

log = logging.getLogger(__name__)

# Source checkout: platform-api/migrations. In the image: /app/migrations (PLATFORM_MIGRATIONS_DIR).
MIGRATIONS_DIR = Path(os.environ.get("PLATFORM_MIGRATIONS_DIR") or Path(__file__).resolve().parents[2] / "migrations")


async def _init_conn(conn: asyncpg.Connection) -> None:
    # jsonb in and out as Python objects
    await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")
    await conn.set_type_codec("json", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")


async def create_pool(dsn: str, min_size: int = 1, max_size: int = 10) -> asyncpg.Pool:
    return await asyncpg.create_pool(dsn, min_size=min_size, max_size=max_size, init=_init_conn)


async def migrate(dsn: str, directory: Path = MIGRATIONS_DIR) -> list[str]:
    """Apply pending migrations in name order, each in its own transaction."""
    conn = await asyncpg.connect(dsn)
    applied_now: list[str] = []
    try:
        await conn.execute("create table if not exists schema_migrations (version text primary key, applied_at timestamptz not null default now())")
        # one migrator at a time
        await conn.execute("select pg_advisory_lock(hashtext('kap_platform_migrate'))")
        done = {r["version"] for r in await conn.fetch("select version from schema_migrations")}
        for f in sorted(directory.glob("*.sql")):
            if f.stem in done:
                continue
            async with conn.transaction():
                await conn.execute(f.read_text(encoding="utf-8"))
                await conn.execute("insert into schema_migrations (version) values ($1)", f.stem)
            applied_now.append(f.stem)
            log.info("applied migration %s", f.stem)
    finally:
        await conn.execute("select pg_advisory_unlock(hashtext('kap_platform_migrate'))")
        await conn.close()
    return applied_now
