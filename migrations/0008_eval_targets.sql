-- Evaluations of workflows, regressions and automatic runs.
--   eval_run      — a target is an agent (agent_id) or a workflow version (workflow_id, version);
--                   trigger says who started it; regressed marks a drop against the previous run
--   eval_result   — a workflow question runs as a workflow run (wf_run_id)
--   eval_schedule — run a test set against a target on a schedule and/or after its data is reloaded

alter table eval_run alter column agent_id drop not null;
alter table eval_run add column target_kind text not null default 'agent' check (target_kind in ('agent', 'workflow'));
alter table eval_run add column workflow_id text;
alter table eval_run add column version text;
alter table eval_run add column trigger text not null default 'manual' check (trigger in ('manual', 'schedule', 'data'));
alter table eval_run add column regressed boolean;
alter table eval_run add column regress_reason text;
alter table eval_run add constraint eval_run_target check (
  (target_kind = 'agent' and agent_id is not null) or (target_kind = 'workflow' and workflow_id is not null));
create index eval_run_workflow on eval_run (tenant_id, workflow_id, created_at desc) where workflow_id is not null;

alter table eval_result add column wf_run_id uuid;

create table eval_schedule (
  id             uuid primary key default gen_random_uuid(),
  tenant_id      uuid not null,
  set_id         uuid not null references eval_set(id) on delete cascade,
  target_kind    text not null check (target_kind in ('agent', 'workflow')),
  agent_id       uuid,
  workflow_id    text,
  every_minutes  int check (every_minutes is null or every_minutes >= 60),   -- null = not on a schedule
  on_data        boolean not null default false,  -- after a PostgreSQL export into one of the target's collections
  min_pass_rate  numeric check (min_pass_rate is null or (min_pass_rate >= 0 and min_pass_rate <= 1)),
  options        jsonb not null default '{}',     -- run options (judge, min_correctness, …)
  enabled        boolean not null default true,
  next_run_at    timestamptz,
  data_seen_at   timestamptz,                     -- newest export already evaluated
  last_run_id    uuid,
  created_at     timestamptz not null default now(),
  updated_at     timestamptz not null default now(),
  check ((target_kind = 'agent' and agent_id is not null) or (target_kind = 'workflow' and workflow_id is not null)),
  check (every_minutes is not null or on_data)
);
create index eval_schedule_due on eval_schedule (next_run_at) where enabled;
