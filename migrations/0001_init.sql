-- Platform layer schema, slice 1: job queue, recorded model calls, mirrored KAP jobs.
-- Every row carries the KAP tenant_id; KAP objects are referenced by their KAP UUID.

-- ---------------------------------------------------------------- job queue
create table job_queue (
  id              uuid primary key default gen_random_uuid(),
  tenant_id       uuid,                          -- null for installation-wide jobs
  kind            text not null,                 -- sync_agent_jobs | link_trace | fetch_job | ...
  payload         jsonb not null default '{}',
  status          text not null default 'queued'
                  check (status in ('queued', 'running', 'succeeded', 'failed', 'cancelled', 'dead')),
  priority        smallint not null default 0,
  run_after       timestamptz not null default now(),
  attempts        int not null default 0,
  max_attempts    int not null default 5,
  locked_by       text,
  lease_until     timestamptz,
  idempotency_key text,
  result          jsonb,
  error           text,
  created_at      timestamptz not null default now(),
  updated_at      timestamptz not null default now()
);
create unique index job_queue_idem on job_queue (coalesce(tenant_id, '00000000-0000-0000-0000-000000000000'::uuid), idempotency_key)
  where idempotency_key is not null;
create index job_queue_claim on job_queue (priority desc, run_after) where status = 'queued';
create index job_queue_lease on job_queue (lease_until) where status = 'running';

-- ---------------------------------------------------------------- model calls (gateway)
create table model_call (
  id                uuid primary key default gen_random_uuid(),
  tenant_id         uuid,                        -- from the gateway route
  provider          text not null,               -- KAP model provider name from the route
  started_at        timestamptz not null,
  finished_at       timestamptz not null,
  duration_ms       int not null,
  method            text not null,
  path              text not null,
  status            int,
  model             text,
  stream            boolean not null default false,
  n_messages        int not null default 0,
  first_user_text   text,                        -- correlation key: the job's user_prompt
  prompt_hash       text,                        -- sha256(system + first user message)
  request           jsonb,
  response          jsonb,                       -- parsed JSON, or {"sse": "..."} for streams
  prompt_tokens     int,
  completion_tokens int,
  total_tokens      int,
  error             text,
  kap_job_id        uuid                         -- set once linked to a KAP agent job
);
create index model_call_corr on model_call (tenant_id, started_at) where kap_job_id is null and n_messages > 0;
create index model_call_job on model_call (kap_job_id, started_at);

-- ---------------------------------------------------------------- KAP agent jobs (mirror)
create table kap_job (
  id            uuid primary key,                -- KAP job id
  tenant_id     uuid not null,
  agent_id      uuid not null,
  agent_name    text,
  status        text not null,                   -- waiting | running | succeeded | failed | cancelled
  user_prompt   text,
  payload       jsonb,
  result        text,
  error         text,
  retry_count   int not null default 0,
  created_by    uuid,
  created_at    timestamptz not null,
  running_at    timestamptz,
  finished_at   timestamptz,
  trace_status  text not null default 'pending'
                check (trace_status in ('pending', 'linked', 'ambiguous', 'none')),
  trace_calls   int not null default 0,
  synced_at     timestamptz not null default now()
);
create index kap_job_tenant_time on kap_job (tenant_id, created_at desc);
create index kap_job_pending on kap_job (tenant_id) where trace_status = 'pending';
