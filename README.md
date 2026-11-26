# KAP platform layer

Services we own on top of the KAP API (see `../docs/kap-platform-layer.md`). Slice 1:

| Role | What it does |
|---|---|
| **gateway** | Transparent proxy between KAP and the model servers. KAP model providers point at it; every model call (request, reply, tool calls, tokens, millisecond timing) is recorded. |
| **worker** | Postgres-backed job queue. Mirrors each tenant's KAP agent jobs every 15 s, and links recorded model calls to the job that made them. |
| **api** | REST API for the console: traces (`/api/traces`, `/api/traces/{job_id}`), job submission with callbacks, and the KAP callback endpoint. |
| **migrate** | Applies `migrations/*.sql`. |

One image runs all four: `python -m kap_platform <role>`.

## How a trace is built

1. KAP's model provider `base_url` is `http://platform-gateway:8090/t/{tenant_id}/p/{provider}/v1`. The gateway forwards to vLLM and stores each call in `model_call`.
2. The worker syncs agent jobs from KAP (`GET /api/agents`, `GET /api/agents/{id}/jobs`) into `kap_job`. Jobs submitted through our API also get a KAP callback.
3. When a job is finished, `link_trace` links the calls whose first user message equals the job's `user_prompt` and that happened while it ran. Overlapping jobs with an identical prompt are marked `ambiguous` instead of guessed.
4. `GET /api/traces/{job_id}` returns the steps: queue wait, each model call (plan / answer, tokens), each tool call with arguments and result.

## Develop

```bash
uv venv .venv && uv pip install --python .venv -e ".[dev]"
.venv/Scripts/python -m pytest            # Windows; .venv/bin/python on Linux
```

Unit tests need nothing else. The queue and linking tests need Postgres:

```bash
docker run -d --name platform-test-db -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:17-alpine
PLATFORM_TEST_DATABASE_URL=postgresql://postgres:test@localhost:55432/postgres .venv/Scripts/python -m pytest
```

## Deploy on the test stand

Prerequisites on the KAP side:

1. **Allowlist.** Add `platform-gateway` and `platform-api` to `OUTBOUND_PRIVATE_ALLOWLIST` in KAP's `docker-compose.yml` and recreate `api worker-agent worker-pipeline worker-beat`. Without it KAP refuses to call the gateway (`base_url_unreachable`) and callbacks to the API.
2. **Credentials per tenant.** Preferred: enable KAP's M2M installation parameter, create a service account in each tenant with an administrator grant, and an OAuth client for it (`POST /api/oauth-clients`, needs a user token). Until then: a legacy user API token per tenant.

Then, on the VM:

```bash
cd platform-api
cp .env.example .env                      # fill in passwords, keys, KAP credentials
mkdir -p config && cp tenants.example.yaml config/tenants.yaml
docker compose -f docker-compose.stand.yml up -d --build
curl -s http://127.0.0.1:8180/health
```

Building needs PyPI access from the VM. If the VM has none, build the image elsewhere, `docker save kap-platform:0.1.0 | gzip > kap-platform.tar.gz`, copy it over and `docker load`, or push it to the VM's Harbor registry.

Finally, point a KAP model provider at the gateway (start with sandbox; keep the old URL to roll back):

```
PATCH /api/model-providers/{id}   {"settings": {"base_url": "http://platform-gateway:8090/t/<tenant_id>/p/agent_default/v1"}}
POST  /api/model-providers/{id}/check      → "status": "ok"
```

## API (slice 1)

All `/api/*` calls need `Authorization: Bearer <one of PLATFORM_API_KEYS>`.

| Method | Path | |
|---|---|---|
| GET | `/health` | Liveness + database check |
| GET | `/api/traces?tenant_id=&limit=&offset=` | Mirrored agent jobs with `trace_status` (`pending`, `linked`, `ambiguous`, `none`) |
| GET | `/api/traces/{job_id}` | Job + trace steps + totals |
| POST | `/api/t/{tenant_id}/agents/{agent_id}/jobs` | Submit a KAP agent job with our callback (`user_prompt` and/or `payload`) |
| POST | `/hooks/kap/t/{tenant_id}/agents/{agent_id}/jobs` | KAP job-completion callback (header `X-Platform-Hook-Token`) |

Pages and errors follow KAP's conventions (`items/total/limit/offset/has_next`, `{type, message, data}`).

## PostgreSQL data source

Exports tables from a customer PostgreSQL database into KAP's indicator CSV format
(`registry.csv`, `observations.csv`, `narratives.csv`: UTF-8 with BOM, comma, `.` decimals,
RFC 4180), uploads them to S3 and lets KAP's own S3 connector and `indicators` pipeline load
them — or, with preset `records`, turns each row into a Word document for a KAP document collection.
Code: `src/kap_platform/pgsource/`, tables: `migrations/0003_pg_source.sql`, `0004_pg_mapping.sql`, `0005_pg_records.sql`.

- **Source** — host, port, database, read-only login, sslmode. The password is stored encrypted
  (Fernet, `PLATFORM_SECRET_KEY`) and never returned (`password_set: true`). Every session is a
  `READ ONLY` transaction, `statement_timeout` 60 s, connect timeout 10 s. `test` lists readable
  tables; `preview` runs one SELECT/WITH (≤ 50 rows).
- **Dataset** — a source + preset + target KAP collection (+ optional schedule in minutes). Four presets:
  - `scai_indicators`: built-in SQL for the scai-chatbot schema. One series per published indicator
    detail × granularity × geography in `indicator_values`; id
    `cms.<PublishedIndicatorDetailId, lower-case, no dashes>.<a|q|m>.overall` for Qatar (blank/`Qatar`
    country), `….<country_slug>` with dimension = country for benchmark countries. Narratives from
    `indicator_analysis` (published commentary preferred over item commentary).
  - `custom`: your own `registry` and `observations` (and optional `narratives`) SELECTs returning
    KAP column names; unknown columns are rejected, missing optional ones written blank.
  - `mapped`: no SQL. Pick one table or view where each row is an observation and map its columns
    (dataset field `mapping`, jsonb):
    ```
    {table: "schema.name", series_key: [col, …], id_prefix?: [a-z0-9_]{1,24} (default: dataset name),
     period: {column, kind: "date"|"text"}, frequency: {value: "A"|"Q"|"M"} | {column, map: {"<raw>": "A"|"Q"|"M"}},
     value: col, target?, name_en?, name_ar?, definition?, unit?/source?: {value} | {column},
     dimension?, domestic_values?: [str], filters?: [{column, op: "="|"!="|"is_true"|"is_false"|"is_not_null", value?}]}
    ```
    The platform generates a registry query (grouped per series × frequency × dimension, newest
    non-blank names/unit/source/definition) and an observations query. Safety: the table must be one
    the login can SELECT, every column must exist in it (`information_schema.columns`) with a fitting
    type (numeric value/target, date/timestamp for `kind: date`, text/integer for `kind: text`, boolean
    for `is_true`/`is_false`), identifiers are double-quoted and filter values are `$n` parameters.
    id = `<id_prefix>.<slug(series key values joined ".")>.<a|q|m>.<overall | slug(dimension)>` (slug:
    lower-case `[a-z0-9_]`, ≤ 48 chars, a hash suffix when truncated). `id_prefix` is stored on save, so
    renaming the dataset keeps the ids. Date periods are formatted per frequency (`YYYY`, `YYYY-Qn`,
    `YYYY-MM`); text periods must already be in that format. Dimension values that are blank or in
    `domestic_values` (case-insensitive) form the main series (`overall`, blank dimension); others get
    their own series named "Name (Dimension)". Rows without value and target, bad periods, unmapped
    frequencies and duplicates (last kept) are counted in `problems`. Narratives: header only.
  - `records`: text-heavy tables (tickets, contracts, products, FAQs) → searchable documents. Each row
    of one table/view becomes a small Word document (a "record card") in a KAP **document** collection
    that agents search with `hybrid_search`. Dataset field `records` (jsonb):
    ```
    {table: "schema.name", id_column: col (unique per row), title: "Ticket {ticket_id}" ({column} placeholders),
     fields: [{column, label}] (short "Label: value" lines), sections: [{column, label}] (long text under a heading),
     filters?: [...same as mapped], order_by?: {column, desc}, max_records: 1–50000 (default 5000), language?: "en"|"ar"}
    ```
    Same safety as `mapped` (readable table, columns checked, quoted identifiers, `$n` filter values).
    The query selects the id + shown columns, `order by <order_by> nulls last, <id>`, `limit max_records + 1`
    (more rows → the first `max_records` are exported and `problems` says so); empty and repeated ids are
    left out and counted. The card: Heading 1 = title, one bold "Label: value" paragraph per field,
    Heading 2 + paragraphs (split on blank lines) per section, a last line "Source: <table> · record <id>".
    Empty values drop their line/section; dates ISO, numbers as is, booleans Yes/No. `ar` makes every
    paragraph right-to-left. The .docx bytes are deterministic (fixed core properties, zip re-packed with
    1980-01-01 timestamps and a stable entry order), so an unchanged row gives an identical file.
    - **One record per folder**: `s3://<bucket>/<tenant_id>/<dataset>/<slug>/record.docx`. `hybrid_search`
      returns one summary per document group, and the collection groups by folder at depth 1 below the
      connector prefix; with many documents in one folder the group summary dropped details on the stand
      (Nov 2026), so every record gets its own folder = its own group and summary. Slug: lower-case
      `[a-z0-9]` runs joined by `_`, ≤ 64 chars; when that changed or truncated the id an 8-char hash of
      the id is appended, so distinct ids never share a folder.
    - **Run**: `read` → `write`: a content hash per record (what the card shows + a renderer version) is
      compared with the hashes stored by the last run that finished its upload and with the objects listed
      under the prefix (without a stored hash: a fresh render's MD5 vs the ETag; multipart ETags never
      match) → `upload`: **new** and **changed** documents are rendered (python-docx, ~30 ms each) and
      uploaded, folders of records that no longer exist are **removed** (KAP drops the file at its next
      scan), **unchanged** ones are neither rendered nor touched (KAP does not reprocess them). Nothing
      new/changed/removed → `kap_scan` / `kap_load` are skipped. Otherwise the watcher follows every pipeline job of the collection created since the
      upload began (a scan during a long upload starts several), with backoff (10 s, +10 s per minute of
      polling, ≤ 60 s): ≤ 15 min for the first job, ≤ 60 min overall (`PLATFORM_PG_EXPORT_RECORDS_WAIT_S`).
      Run `counts: {documents, new, changed, removed, unchanged}`, `files`: the first 20 uploaded documents.
    - **Cost**: KAP summarises every new or changed record with the chat model (`chat_default`), once per
      document and once per group — so roughly two model calls per changed record and run. The first
      load of 5000 records is 5000 documents to summarise and takes a while; later runs only pay for
      the rows that changed. Keep `fields` / `sections` to what agents need: any change to a shown column
      (or to the spec: labels, title, language) changes every affected document and re-summarises it.
    - **KAP objects**: connector `<collection>_pg` (include `**/*.docx`), pipeline `<collection>_pipeline`
      (kind `document`: `parse` docx → markdown with headings/lists/tables kept → `summarize_document`
      transformer with a `summary` task → `embed`, `embed_default`, chunks 800/120), collection
      `<collection>` with `entity` on: type `indicator_document_context` (or `document_group`, created
      when the tenant has neither), folder boundary at depth 1, id `{collection_id}:{relative_folder_path}`,
      group summary by `chat_default`. Prompts as in the console (`kap-console/src/features/v35/data/search.ts`).
      Switching a dataset between `records` and an indicator preset re-provisions; an existing pipeline of
      the other kind under the same name is refused (choose another collection name).
- **Run** (`pg_export` job): `read` → `write` → `upload` to `s3://<bucket>/<tenant_id>/<dataset>/<file>.csv`
  (fixed keys, so each run is a new file version in KAP) → `kap_scan` (KAP's connector, scanning every
  minute, starts a pipeline job) → `kap_load` (that job's progress and result). The worker polls KAP
  every 10 s for up to 15 min (`pg_export_watch`). Unchanged files (same hashes as the last successful
  run) skip the KAP steps.
- **KAP objects**, created on the first run and reused by name afterwards: connector
  `<collection>_pg` (S3, plain keys — the stand runs KAP with `SECRET_PROVIDER_MODE=disabled`),
  pipeline `<collection>_pipeline` (copy of the sandbox indicators pipeline, `embed_default`),
  collection `<collection>` (`sync_strategy: overwrite`). KAP names allow `[A-Za-z0-9_]`, 4–32 chars,
  so `collection_name` is limited to 29 characters. Deleting a dataset leaves its KAP objects and S3 files.

### Setup

Add to `.env` (api and worker both need them):

```bash
# Fernet key for source passwords; keep it stable
PLATFORM_SECRET_KEY=$(openssl rand -base64 32 | tr '+/' '-_')
# or: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
PLATFORM_S3_URL=http://s3:9000          # stand RustFS, path addressing, plain HTTP
PLATFORM_S3_ACCESS_KEY=<rustfs access key>
PLATFORM_S3_SECRET_KEY=<rustfs secret key>
PLATFORM_S3_BUCKET=platform-exports     # created if missing
# PLATFORM_S3_KAP_URL=…                 # only if KAP reaches S3 under another address
```

Then rebuild (new dependencies `cryptography`, `boto3`) and migrate: `docker compose -f docker-compose.stand.yml up -d --build`
(the `platform-migrate` service applies `0003_pg_source` and `0004_pg_mapping`).

On the stand: source host `scai-chatbot-postgres-1`, port 5432, database `scai_indicators`,
user `kap_platform_ro`, sslmode `disable`; preset `scai_indicators`.

| Method | Path | |
|---|---|---|
| POST/GET | `/api/t/{tid}/pg-sources` | create / list sources |
| GET/PATCH/DELETE | `/api/t/{tid}/pg-sources/{id}` | DELETE → 409 while datasets use it |
| POST | `/api/t/{tid}/pg-sources/{id}/test` | `{ok, message, server_version, tables}` |
| POST | `/api/t/{tid}/pg-sources/{id}/preview` | `{sql, limit≤50}` → `{columns, rows, truncated}` |
| GET | `/api/t/{tid}/pg-sources/{id}/columns?table=schema.name` | `{table, columns: [{name, type, nullable}]}`; 404 for a table the login cannot read |
| POST | `/api/t/{tid}/pg-sources/{id}/mapping-preview` | `{mapping, limit?, name?}` → like a dataset preview, nothing saved |
| POST | `/api/t/{tid}/pg-sources/{id}/records-preview` | `{records, name?, limit≤5}` → `{documents: {count, sample: [{key, title, text}]}, problems, query}`, nothing saved or uploaded |
| POST/GET | `/api/t/{tid}/pg-datasets[?source_id=]` | create / list datasets (`mapping` for preset `mapped`, `records` for preset `records`; `queries` ignored there) |
| GET/PATCH/DELETE | `/api/t/{tid}/pg-datasets/{id}` | |
| POST | `/api/t/{tid}/pg-datasets/{id}/preview` | first rows + counts of the three files, `problems`; `mapped` also `queries: {registry, observations}` (the generated SQL); `records`: like records-preview |
| POST | `/api/t/{tid}/pg-datasets/{id}/run` | 201 export run (409 while one is active) |
| GET | `/api/t/{tid}/pg-exports[?dataset_id=]`, `/api/t/{tid}/pg-exports/{id}` | runs, newest first |
