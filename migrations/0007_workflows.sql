-- Workflows: graphs of KAP agents and platform steps, run by the platform's worker.
--   wf_workflow  — one workflow: the console's document (name, purpose, versions with their graph)
--   wf_run       — one execution of a workflow version for a request
--   wf_step      — one step (graph node) of a run: status, input, output, KAP job

create table wf_workflow (
  tenant_id   uuid not null,
  id          text not null,                     -- slug chosen in the console, e.g. contract_review
  doc         jsonb not null,                    -- {n, purpose, entry, dis, vers: [{v, st, cfg, note, by, at}], ...}
  created_at  timestamptz not null default now(),
  updated_at  timestamptz not null default now(),
  primary key (tenant_id, id)
);

create table wf_run (
  id            uuid primary key default gen_random_uuid(),
  tenant_id     uuid not null,
  workflow_id   text not null,
  workflow_name text,
  version       text not null,
  trigger       text not null default 'test' check (trigger in ('test', 'api', 'eval', 'schedule')),
  input         text not null,
  cfg           jsonb not null,                  -- the graph as run (a test may run unsaved changes)
  agents        jsonb not null default '{}',     -- agent name → KAP agent id, resolved at the start
  status        text not null default 'queued'
                check (status in ('queued', 'running', 'waiting', 'succeeded', 'failed', 'cancelled')),
  answer        text,
  error         text,
  fallback_used boolean not null default false,
  agent_jobs    int not null default 0,          -- KAP jobs submitted (limit: cfg.limits.steps)
  polls         int not null default 0,
  ticking_until timestamptz,                     -- one tick at a time per run
  created_by    text,
  created_at    timestamptz not null default now(),
  started_at    timestamptz,
  finished_at   timestamptz
);
create index wf_run_tenant on wf_run (tenant_id, created_at desc);
create index wf_run_workflow on wf_run (tenant_id, workflow_id, created_at desc);

create table wf_step (
  run_id       uuid not null references wf_run(id) on delete cascade,
  node_id      text not null,
  kind         text not null,                    -- entry | agent | router | aggregate | review | fallback | output | tool
  label        text not null default '',
  status       text not null default 'pending'
               check (status in ('pending', 'running', 'waiting', 'done', 'failed', 'skipped')),
  seq          int,                              -- order in which steps started
  input        text,
  output       text,
  note         text,
  chosen       jsonb,                            -- router: node ids of the routes taken
  job_id       uuid,                             -- KAP job of an agent / aggregator step
  attempts     int not null default 0,
  started_at   timestamptz,
  finished_at  timestamptz,
  primary key (run_id, node_id)
);
create index wf_step_job on wf_step (job_id);
