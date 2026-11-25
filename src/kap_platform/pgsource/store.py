"""Reads and writes for pg_source, pg_dataset and pg_export_run.

Rows come back as plain dicts (ids as text). `PgStore` is the only class
touching SQL here, so tests can swap in an in-memory store with the same
methods.
"""
from __future__ import annotations

from typing import Any

import asyncpg

ACTIVE = ("queued", "running", "waiting_for_kap")


class Conflict(Exception):
    pass


class InUse(Exception):
    pass


_CONSTRAINT_MESSAGES = {
    "pg_source_tenant_id_name_key": "A source with this name already exists.",
    "pg_dataset_tenant_id_name_key": "A dataset with this name already exists.",
    "pg_dataset_tenant_id_collection_name_key": "Another dataset already exports to this collection.",
}

SOURCE_FIELDS = ("name", "host", "port", "database", "username", "password_enc", "sslmode", "last_test")
DATASET_FIELDS = ("source_id", "name", "preset", "queries", "published_only", "collection_name", "schedule_minutes", "kap", "next_run_at")
RUN_FIELDS = ("status", "steps", "counts", "files", "file_hashes", "kap", "uploaded_at", "polls", "error", "started_at", "finished_at")

_SOURCE_SELECT = """
select id::text, tenant_id::text, name, host, port, database, username, password_enc, sslmode, last_test, created_at, updated_at
  from pg_source
"""
_DATASET_SELECT = """
select d.id::text, d.tenant_id::text, d.source_id::text, s.name as source_name, d.name, d.preset, d.queries,
       d.published_only, d.collection_name, d.schedule_minutes, d.kap, d.next_run_at, d.created_at, d.updated_at,
       (select jsonb_build_object('id', r.id, 'status', r.status, 'finished_at', r.finished_at)
          from pg_export_run r where r.dataset_id = d.id order by r.created_at desc limit 1) as last_run
  from pg_dataset d join pg_source s on s.id = d.source_id
"""
_RUN_SELECT = """
select r.id::text, r.tenant_id::text, r.dataset_id::text, d.name as dataset_name, r.trigger, r.status, r.steps,
       r.counts, r.files, r.file_hashes, r.kap, r.uploaded_at, r.polls, r.error, r.created_at, r.started_at, r.finished_at
  from pg_export_run r join pg_dataset d on d.id = r.dataset_id
"""


def _set_clause(fields: dict[str, Any], allowed: tuple[str, ...], start: int) -> tuple[str, list[Any]]:
    parts, args = [], []
    for k, v in fields.items():
        if k not in allowed:
            raise ValueError(f"unknown field {k}")
        args.append(v)
        parts.append(f"{k} = ${start + len(args) - 1}")
    return ", ".join(parts), args


def _conflict(exc: asyncpg.UniqueViolationError) -> Conflict:
    return Conflict(_CONSTRAINT_MESSAGES.get(getattr(exc, "constraint_name", "") or "", "This name is already taken."))


class PgStore:
    def __init__(self, pool: asyncpg.Pool):
        self.pool = pool

    # ------------------------------------------------------------ sources
    async def create_source(self, tenant_id: str, fields: dict[str, Any]) -> dict[str, Any]:
        cols = [k for k in SOURCE_FIELDS if k in fields]
        try:
            row = await self.pool.fetchrow(
                f"insert into pg_source (tenant_id, {', '.join(cols)}) values ($1, {', '.join(f'${i + 2}' for i in range(len(cols)))}) returning id::text",
                tenant_id,
                *[fields[c] for c in cols],
            )
        except asyncpg.UniqueViolationError as exc:
            raise _conflict(exc) from exc
        return await self.get_source(tenant_id, row["id"])

    async def list_sources(self, tenant_id: str, limit: int, offset: int) -> tuple[list[dict[str, Any]], int]:
        rows = await self.pool.fetch(f"{_SOURCE_SELECT} where tenant_id = $1 order by name limit $2 offset $3", tenant_id, limit, offset)
        total = await self.pool.fetchval("select count(*) from pg_source where tenant_id = $1", tenant_id)
        return [dict(r) for r in rows], total

    async def get_source(self, tenant_id: str, source_id: str) -> dict[str, Any] | None:
        r = await self.pool.fetchrow(f"{_SOURCE_SELECT} where tenant_id = $1 and id = $2", tenant_id, source_id)
        return dict(r) if r else None

    async def update_source(self, tenant_id: str, source_id: str, fields: dict[str, Any]) -> dict[str, Any] | None:
        if fields:
            sets, args = _set_clause(fields, SOURCE_FIELDS, 3)
            try:
                await self.pool.execute(f"update pg_source set {sets}, updated_at = now() where tenant_id = $1 and id = $2", tenant_id, source_id, *args)
            except asyncpg.UniqueViolationError as exc:
                raise _conflict(exc) from exc
        return await self.get_source(tenant_id, source_id)

    async def delete_source(self, tenant_id: str, source_id: str) -> bool:
        try:
            status = await self.pool.execute("delete from pg_source where tenant_id = $1 and id = $2", tenant_id, source_id)
        except asyncpg.ForeignKeyViolationError as exc:
            raise InUse("Datasets use this source; delete them first.") from exc
        return status.endswith(" 1")

    async def count_datasets(self, tenant_id: str, source_id: str) -> int:
        return await self.pool.fetchval("select count(*) from pg_dataset where tenant_id = $1 and source_id = $2", tenant_id, source_id)

    # ------------------------------------------------------------ datasets
    async def create_dataset(self, tenant_id: str, fields: dict[str, Any]) -> dict[str, Any]:
        cols = [k for k in DATASET_FIELDS if k in fields]
        try:
            row = await self.pool.fetchrow(
                f"insert into pg_dataset (tenant_id, {', '.join(cols)}) values ($1, {', '.join(f'${i + 2}' for i in range(len(cols)))}) returning id::text",
                tenant_id,
                *[fields[c] for c in cols],
            )
        except asyncpg.UniqueViolationError as exc:
            raise _conflict(exc) from exc
        return await self.get_dataset(tenant_id, row["id"])

    async def list_datasets(self, tenant_id: str, source_id: str | None, limit: int, offset: int) -> tuple[list[dict[str, Any]], int]:
        rows = await self.pool.fetch(
            f"{_DATASET_SELECT} where d.tenant_id = $1 and ($2::uuid is null or d.source_id = $2) order by d.name limit $3 offset $4",
            tenant_id, source_id, limit, offset,
        )
        total = await self.pool.fetchval("select count(*) from pg_dataset where tenant_id = $1 and ($2::uuid is null or source_id = $2)", tenant_id, source_id)
        return [dict(r) for r in rows], total

    async def get_dataset(self, tenant_id: str | None, dataset_id: str) -> dict[str, Any] | None:
        r = await self.pool.fetchrow(f"{_DATASET_SELECT} where ($1::uuid is null or d.tenant_id = $1) and d.id = $2", tenant_id, dataset_id)
        return dict(r) if r else None

    async def update_dataset(self, tenant_id: str, dataset_id: str, fields: dict[str, Any]) -> dict[str, Any] | None:
        if fields:
            sets, args = _set_clause(fields, DATASET_FIELDS, 3)
            try:
                await self.pool.execute(f"update pg_dataset set {sets}, updated_at = now() where tenant_id = $1 and id = $2", tenant_id, dataset_id, *args)
            except asyncpg.UniqueViolationError as exc:
                raise _conflict(exc) from exc
        return await self.get_dataset(tenant_id, dataset_id)

    async def delete_dataset(self, tenant_id: str, dataset_id: str) -> bool:
        status = await self.pool.execute("delete from pg_dataset where tenant_id = $1 and id = $2", tenant_id, dataset_id)
        return status.endswith(" 1")

    async def due_datasets(self) -> list[dict[str, Any]]:
        """Datasets whose scheduled time has passed; their next time is advanced in the same statement."""
        rows = await self.pool.fetch(
            """
            with due as (
              select id from pg_dataset
               where schedule_minutes is not null and next_run_at is not null and next_run_at <= now()
               for update skip locked)
            update pg_dataset d set next_run_at = now() + make_interval(mins => d.schedule_minutes)
              from due where d.id = due.id
            returning d.id::text, d.tenant_id::text
            """
        )
        return [dict(r) for r in rows]

    # ------------------------------------------------------------ runs
    async def create_run(self, tenant_id: str, dataset_id: str, trigger: str, steps: list[dict[str, Any]]) -> dict[str, Any]:
        """Insert a queued run unless one is already active for the dataset (then Conflict)."""
        async with self.pool.acquire() as conn, conn.transaction():
            await conn.execute("select pg_advisory_xact_lock(hashtext($1))", f"pg_export:{dataset_id}")
            active = await conn.fetchval("select id::text from pg_export_run where dataset_id = $1 and status = any($2::text[]) limit 1", dataset_id, list(ACTIVE))
            if active:
                raise Conflict(f"An export of this dataset is already in progress ({active}).")
            run_id = await conn.fetchval(
                "insert into pg_export_run (tenant_id, dataset_id, trigger, steps) values ($1, $2, $3, $4) returning id::text",
                tenant_id, dataset_id, trigger, steps,
            )
        return await self.get_run(tenant_id, run_id)

    async def get_run(self, tenant_id: str | None, run_id: str) -> dict[str, Any] | None:
        r = await self.pool.fetchrow(f"{_RUN_SELECT} where ($1::uuid is null or r.tenant_id = $1) and r.id = $2", tenant_id, run_id)
        return dict(r) if r else None

    async def list_runs(self, tenant_id: str, dataset_id: str | None, limit: int, offset: int) -> tuple[list[dict[str, Any]], int]:
        rows = await self.pool.fetch(
            f"{_RUN_SELECT} where r.tenant_id = $1 and ($2::uuid is null or r.dataset_id = $2) order by r.created_at desc limit $3 offset $4",
            tenant_id, dataset_id, limit, offset,
        )
        total = await self.pool.fetchval("select count(*) from pg_export_run where tenant_id = $1 and ($2::uuid is null or dataset_id = $2)", tenant_id, dataset_id)
        return [dict(r) for r in rows], total

    async def update_run(self, run_id: str, **fields: Any) -> None:
        sets, args = _set_clause(fields, RUN_FIELDS, 2)
        await self.pool.execute(f"update pg_export_run set {sets} where id = $1", run_id, *args)

    async def last_success_hashes(self, dataset_id: str, exclude_run_id: str) -> dict[str, str] | None:
        return await self.pool.fetchval(
            "select file_hashes from pg_export_run where dataset_id = $1 and id <> $2 and status = 'succeeded' and file_hashes is not null order by created_at desc limit 1",
            dataset_id, exclude_run_id,
        )
