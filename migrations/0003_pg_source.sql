-- PostgreSQL data source: customer databases exported to KAP indicator CSVs on S3.
--   pg_source      — a connection (password encrypted with PLATFORM_SECRET_KEY, Fernet)
--   pg_dataset     — what to export from a source and into which KAP collection
--   pg_export_run  — one export: read → write → upload → KAP scan → KAP load

create table pg_source (
  id            uuid primary key default gen_random_uuid(),
  tenant_id     uuid not null,
  name          text not null,
  host          text not null,
  port          int not null default 5432 check (port between 1 and 65535),
  database      text not null,
  username      text not null,
  password_enc  text,                            -- Fernet token; never returned by the API
  sslmode       text not null default 'prefer'
                check (sslmode in ('disable', 'allow', 'prefer', 'require', 'verify-ca', 'verify-full')),
  last_test     jsonb,                           -- {ok, message, at}
  created_at    timestamptz not null default now(),
  updated_at    timestamptz not null default now(),
  unique (tenant_id, name)
);

create table pg_dataset (
  id                uuid primary key default gen_random_uuid(),
  tenant_id         uuid not null,
  source_id         uuid not null references pg_source(id) on delete restrict,
  name              text not null,
  preset            text not null check (preset in ('scai_indicators', 'custom')),
  queries           jsonb not null default '{}',  -- custom: {registry, observations, narratives?}
  published_only    boolean not null default true,
  collection_name   text not null,
  schedule_minutes  int check (schedule_minutes is null or schedule_minutes > 0),
  kap               jsonb,                       -- {connector, pipeline, collection, collection_id, pipeline_id, provisioned_at}
  next_run_at       timestamptz,
  created_at        timestamptz not null default now(),
  updated_at        timestamptz not null default now(),
  unique (tenant_id, name),
  unique (tenant_id, collection_name)
);
create index pg_dataset_source on pg_dataset (source_id);
create index pg_dataset_due on pg_dataset (next_run_at) where schedule_minutes is not null;

create table pg_export_run (
  id            uuid primary key default gen_random_uuid(),
  tenant_id     uuid not null,
  dataset_id    uuid not null references pg_dataset(id) on delete cascade,
  trigger       text not null check (trigger in ('manual', 'schedule')),
  status        text not null default 'queued'
                check (status in ('queued', 'running', 'waiting_for_kap', 'succeeded', 'failed')),
  steps         jsonb not null default '[]',
  counts        jsonb,                           -- {registry, observations, narratives}
  files         jsonb,                           -- [{name, key, bytes}]
  file_hashes   jsonb,                           -- {name: sha256}; "unchanged" check
  kap           jsonb,                           -- {collection, pipeline_job_id, state, progress, result}
  uploaded_at   timestamptz,                     -- KAP jobs created after this belong to the run
  polls         int not null default 0,
  error         text,
  created_at    timestamptz not null default now(),
  started_at    timestamptz,
  finished_at   timestamptz
);
create index pg_export_run_dataset on pg_export_run (dataset_id, created_at desc);
create index pg_export_run_tenant on pg_export_run (tenant_id, created_at desc);
create index pg_export_run_active on pg_export_run (dataset_id) where status in ('queued', 'running', 'waiting_for_kap');
