-- Evaluations: test sets of questions, runs of a set against a KAP agent, one result per question.
--   eval_set     — a named list of test questions
--   eval_case    — one question with what a correct answer must contain
--   eval_run     — one evaluation: every case of a set sent to one agent as a real KAP job
--   eval_result  — one case in one run: the KAP job, the answer, checks, judge scores, pass/fail

create table eval_set (
  id           uuid primary key default gen_random_uuid(),
  tenant_id    uuid not null,
  name         text not null,
  description  text not null default '',
  created_at   timestamptz not null default now(),
  updated_at   timestamptz not null default now(),
  unique (tenant_id, name)
);

create table eval_case (
  id               uuid primary key default gen_random_uuid(),
  set_id           uuid not null references eval_set(id) on delete cascade,
  position         int not null default 0,
  question         text not null,
  expected         text not null default '',      -- reference answer, for the judge
  must_include     jsonb not null default '[]',   -- facts the answer must contain (numbers compared as numbers)
  must_not_include jsonb not null default '[]',   -- text the answer must not contain
  tags             jsonb not null default '[]',
  created_at       timestamptz not null default now(),
  updated_at       timestamptz not null default now()
);
create index eval_case_set on eval_case (set_id, position);

create table eval_run (
  id            uuid primary key default gen_random_uuid(),
  tenant_id     uuid not null,
  set_id        uuid not null references eval_set(id) on delete cascade,
  agent_id      uuid not null,                    -- KAP agent
  agent_name    text,
  agent         jsonb,                            -- agent settings when the run started (model, tools, collections, updated_at)
  label         text not null default '',
  options       jsonb not null default '{}',      -- {judge, min_correctness, min_grounding, parallel}
  status        text not null default 'queued'
                check (status in ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
  summary       jsonb,                            -- totals, pass rate, averages
  polls         int not null default 0,
  error         text,
  created_at    timestamptz not null default now(),
  started_at    timestamptz,
  finished_at   timestamptz
);
create index eval_run_tenant on eval_run (tenant_id, created_at desc);
create index eval_run_set on eval_run (set_id, created_at desc);
create index eval_run_agent on eval_run (agent_id, created_at desc);

create table eval_result (
  id                uuid primary key default gen_random_uuid(),
  run_id            uuid not null references eval_run(id) on delete cascade,
  case_id           uuid references eval_case(id) on delete set null,
  position          int not null,
  question          text not null,                -- copied from the case when the run started
  expected          text not null default '',
  must_include      jsonb not null default '[]',
  must_not_include  jsonb not null default '[]',
  status            text not null default 'pending'
                    check (status in ('pending', 'submitted', 'answered', 'scored', 'error')),
  job_id            uuid,                         -- KAP agent job
  job_status        text,
  answer            text,
  checks            jsonb,                        -- [{kind, term, ok}]
  judge             jsonb,                        -- {correctness, grounding, completeness, reason} 1–5
  passed            boolean,
  reason            text,                         -- why it failed (or the judge's note)
  metrics           jsonb,                        -- {latency_ms, tokens, tool_calls, tool_errors, model_calls}
  error             text,
  submitted_at      timestamptz,
  answered_at       timestamptz,
  scored_at         timestamptz
);
create index eval_result_run on eval_result (run_id, position);
create index eval_result_job on eval_result (job_id);
