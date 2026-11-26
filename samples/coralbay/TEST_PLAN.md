# Full platform test: Coral Bay Water Authority

A fictional water utility. Its database has three kinds of data, and each one goes into KAP in a different way.

| Table | Data | How it reaches KAP | KAP collection | Agent |
|---|---|---|---|---|
| `kpi_values` | Monthly and quarterly KPIs with targets, overall and per district | **Column mapping (no SQL)** → indicator files | `cbwa_kpis` | `cbwa_kpi_agent` |
| `districts` | One text profile per district | **Records → searchable documents** | `cbwa_districts` | `cbwa_district_agent` |
| `incidents` | One text record per service incident | **Records → searchable documents** | `cbwa_incidents` | `cbwa_incident_agent` |

A fourth agent, `cbwa_writer`, has no data. It combines the other agents' answers inside a workflow.

On top of them come two workflows:
- **`cbwa_assistant`:** a router sends each question to the right agent, a validator checks the numbers, and a fallback answers if the check fails.
- **`cbwa_briefing`:** three agents run in parallel, the writer combines their answers, and a person approves the result before it is returned.

The files in this folder:
- `coralbay_db.sql`: the database (schema, data, read-only login);
- `eval_cbwa_kpis.csv`, `eval_cbwa_districts.csv`, `eval_cbwa_incidents.csv`: a test set per agent;
- `eval_cbwa_assistant.csv`: the test set for the `cbwa_assistant` workflow.

---

## 0. Create the database (on the VM)

It goes into the Ask AI Postgres container, which the platform already reaches on KAP's network. Pick a password for the read-only login and keep it; the platform asks for it in step 1.

```bash
cd ~/platform-api && git pull
PGC=scai-chatbot-postgres-1
PGU=$(docker exec $PGC printenv POSTGRES_USER)
docker exec $PGC psql -U "$PGU" -d postgres -c "create database coralbay"
docker cp samples/coralbay/coralbay_db.sql $PGC:/tmp/coralbay_db.sql
docker exec $PGC psql -U "$PGU" -d coralbay -v ro_pass='CHOOSE_A_PASSWORD' -f /tmp/coralbay_db.sql
docker exec $PGC psql -U "$PGU" -d coralbay -c "select (select count(*) from kpi_values) kpis, (select count(*) from districts) districts, (select count(*) from incidents) incidents"
```
**Expected:** `kpis 25 | districts 5 | incidents 6`.

---

## 1. PostgreSQL data source

**Data → Data sources → New data source → PostgreSQL**

| Field | Value |
|---|---|
| Name | `coralbay_pg` |
| Host | `scai-chatbot-postgres-1` |
| Port | `5432` |
| Database | `coralbay` |
| User | `coralbay_ro` |
| Password | the one you chose |
| SSL mode | `disable` |

| # | Test | Expected |
|---|---|---|
| 1.1 | **Test connection** | Connected. Tables listed: `districts`, `incidents`, `kpi_values`. |
| 1.2 | Edit the source and type a wrong password, then **Test connection** | A clear authentication error. Then put the right password back. |
| 1.3 | Reopen the source | The password shows as stored and is never displayed. |

---

## 2. Dataset 1: KPIs by column mapping (`cbwa_kpis`)

On `coralbay_pg`, click **New dataset** and choose **Column mapping**.

| Step | Setting | Value |
|---|---|---|
| Table | | `public.kpi_values` |
| Series and period | Series key | `kpi_code` |
| | ID prefix | `cbwa` |
| | Period | `period_start` |
| | Frequency | from column `frequency`: `monthly` → Monthly, `quarterly` → Quarterly |
| Values | Value | `actual` |
| | Target | `target` |
| | Dimension | `district`, main value `All districts` |
| Labels | Name (EN) | column `kpi_name_en` |
| | Name (AR) | column `kpi_name_ar` |
| | Unit | column `unit` |
| | Source | column `data_source` |
| | Definition | column `definition` |
| Filters | | `status` = `published` |
| Target collection | | `cbwa_kpis` |
| Schedule | | Manual |

| # | Test | Expected |
|---|---|---|
| 2.1 | **Preview** | The registry has **8 series**: `cbwa.wtr_cons.m.overall`, `cbwa.nrw_rate.m.overall`, `cbwa.nrw_rate.m.north_harbour`, `…south_plains`, `…east_ridge`, `…west_marina`, `cbwa.cmpl_48h.m.overall`, `cbwa.outage_min.q.overall`. |
| 2.2 | Observations in the preview | **24 rows.** The July 2026 draft (93.0) is not there. Periods look like `2026-05` (monthly) and `2026-Q2` (quarterly). |
| 2.3 | Generated SQL in the preview | Shown read-only. It has the `status = $1` filter as a parameter. |
| 2.4 | **Run now** | One run with 5 steps (read → write → upload → KAP scan → KAP load), all ✓. |
| 2.5 | **Run now** again with no change | The run finishes quickly: the files are unchanged, so KAP has nothing to load. |
| 2.6 | **Data → Collections → `cbwa_kpis`** | An indicator collection with 3 files (registry, observations, narratives). |

---

## 3. Dataset 2: districts as documents (`cbwa_districts`)

On `coralbay_pg`, click **New dataset** and choose **Records → searchable documents**.

| Setting | Value |
|---|---|
| Table | `public.districts` |
| ID column | `district_id` |
| Title | `District: {name_en}` |
| Fields | `name_ar` (label "Arabic name"), `population` ("Population"), `households` ("Households") |
| Sections | `overview` ("Overview"), `challenges` ("Challenges"), `projects` ("Projects") |
| Filter | `is_active` **is true** |
| Target collection | `cbwa_districts` |

| # | Test | Expected |
|---|---|---|
| 3.1 | **Preview** | **4 documents**: North Harbour, South Plains, East Ridge, West Marina. Old Town is not there. |
| 3.2 | **Run now** | Upload says **4 new**. The KAP load takes a few minutes, because KAP summarises each record. |
| 3.3 | Dataset page after the run | It shows **"Create an agent that searches cbwa_districts"**. |
| 3.4 | Change one row on the VM, then **Run now**: `docker exec $PGC psql -U "$PGU" -d coralbay -c "update districts set population = 85100 where district_id = 'D-NH'"` | Upload: **1 changed, 3 unchanged**. Only North Harbour is summarised again. |
| 3.5 | Change it back (`population = 84500`), then **Run now** | **1 changed** again. |

## 4. Dataset 3: incidents as documents (`cbwa_incidents`)

| Setting | Value |
|---|---|
| Table | `public.incidents` |
| ID column | `incident_id` |
| Title | `{incident_id}: {title}` |
| Fields | `district`, `category`, `severity`, `started_at` ("Started"), `restored_at` ("Restored"), `customers_affected` ("Customers affected") |
| Sections | `summary` ("Summary"), `root_cause` ("Root cause"), `resolution` ("Resolution") |
| Filter | `is_public` **is true** |
| Target collection | `cbwa_incidents` |

| # | Test | Expected |
|---|---|---|
| 4.1 | **Preview** | **5 documents.** INC-1080 (internal) is not there. |
| 4.2 | **Run now** | **5 new.** The KAP load ends with 5 documents in state *embedded*. |
| 4.3 | Hide one incident, then **Run now**: `docker exec $PGC psql -U "$PGU" -d coralbay -c "update incidents set is_public = false where incident_id = 'INC-1063'"` | Upload: **1 removed**. Make it public again and run once more: **1 new**. |
| 4.4 | **Ingestion runs** page | Each export is one run with its steps. There are no duplicate runs for one export. |

---

## 5. Agents

Create all four under **Agents → Create agent**, using the model you normally use. Use the **"Create an agent that searches …"** shortcut on the dataset pages for the two search agents.

| Agent | Type | Collections | Tools |
|---|---|---|---|
| `cbwa_kpi_agent` | Planner | `cbwa_kpis` | `find_indicator`, `get_indicator_value`, `actual_vs_target`, plus the time-series tool if your tenant has one |
| `cbwa_district_agent` | Planner | `cbwa_districts` | `hybrid_search` (entity_id `none`) |
| `cbwa_incident_agent` | Planner | `cbwa_incidents` | `hybrid_search` (entity_id `none`) |
| `cbwa_writer` | Workflow (no tools) | none | none |

If KAP refuses a Workflow-type agent without tools for `cbwa_writer`, create it as a Planner with no collections and no tools instead. It only rewrites text.

**System prompts**
- **`cbwa_kpi_agent`:**
  > You answer questions about the Coral Bay Water Authority's performance indicators, using only the indicator tools and the cbwa_kpis collection. Always give the period, the value with its unit, and the target when there is one, and say whether the target was met. KPIs are reported for "All districts" (dimension empty) and, for some indicators, per district (dimension = district name). If there is no published value for the period asked, say so and give the latest period that has one. Never estimate a number.
- **`cbwa_district_agent`** and **`cbwa_incident_agent`:**
  > You answer questions about Coral Bay districts (or service incidents) using only the search results from your collection. Quote numbers, names and dates exactly as written. If the search finds nothing relevant, say that there is no record of it.

  Then click **Insert search instructions** so the `hybrid_search` fields are pinned.
- **`cbwa_writer`:**
  > You receive a request and the answers of other agents. Write one clear answer to the request using only those answers. Keep every number, unit, period and name exactly as given. If the answers disagree or something is missing, say so. Do not add facts.

**Playground tests**

| # | Question (Playground) | Agent | Expected |
|---|---|---|---|
| 5.1 | What was the water consumption in May 2026? | kpi | **5.08 million m³**, target 4.50 |
| 5.2 | What was the non-revenue water rate in South Plains in June 2026? | kpi | **21.7%** |
| 5.3 | What share of complaints was resolved within 48 hours in July 2026? | kpi | No published value; latest is June (90.2%). It must **not** say 93. |
| 5.4 | What is the Smart Meter Rollout in South Plains? | district | **18,000** meters by **December 2026** |
| 5.5 | What is the population of Old Town? | district | No current figure. It must **not** say 12,900. |
| 5.6 | Why did the West Marina pumping station lose power? | incident | Single feed failed, **backup generator** did not start, **5,400** customers, 2 hours |
| 5.7 | Tell me about the chlorine dosing pump calibration. | incident | No public record |

For each Playground run:
- **5.8:** the trace shows the tool calls with their input and output, and the tokens.
- **5.9:** **Open in Monitoring** shows the same run, with the grounding (Evidence) row.

---

## 6. Evaluations of the agents

**Agents → Evaluation datasets.** Create one test set per CSV and use **Import CSV** to load its file:

| Test set | File | Run against |
|---|---|---|
| `CBWA KPIs` | `eval_cbwa_kpis.csv` (8 questions) | `cbwa_kpi_agent` |
| `CBWA districts` | `eval_cbwa_districts.csv` (6) | `cbwa_district_agent` |
| `CBWA incidents` | `eval_cbwa_incidents.csv` (5) | `cbwa_incident_agent` |
| `CBWA assistant` | `eval_cbwa_assistant.csv` (6) | the `cbwa_assistant` workflow (step 8) |

| # | Test | Expected |
|---|---|---|
| 6.1 | Import each CSV | "Imported N questions". The facts show in the **Must include** column. |
| 6.2 | **Run evaluation** for each agent (facts and AI judge, passes from 4) | The run reaches **Finished**. Aim for at least 80% passing. Each failed question names the missing fact and gives the judge's reason. |
| 6.3 | Open a question, then **Open trace** | The KAP job trace of that question. |
| 6.4 | Label a run `baseline` | The label shows in the runs list. |
| 6.5 | Break `cbwa_kpi_agent`: replace its system prompt with "Answer briefly from memory." Then **Run again**. | Pass rate falls. The run is marked **Regressed**, naming the questions that broke. **Compare with…** `baseline` shows Broken rows, and *What changed* says the configuration changed in KAP. |
| 6.6 | Restore the prompt and run again | Back to the baseline. |
| 6.7 | On a failed question where the agent was actually right, click **Use as expected answer** | The test set's question is updated. |

## 7. Add to test set and automatic runs

| # | Test | Expected |
|---|---|---|
| 7.1 | In the Playground, ask *"What was the non-revenue water rate in North Harbour in June 2026?"* (answer 12.1%), then **Add to test set** and pick `CBWA KPIs` | The dialog suggests `12.1` as a fact and the answer as the expected answer. The question is added, tagged `playground`. |
| 7.2 | Do the same from a **Monitoring** run page, and from a **workflow run** page (step 8) | Tagged `monitoring` and `workflow`. |
| 7.3 | On `CBWA KPIs`, click **Add automatic run**: agent `cbwa_kpi_agent`, not on a schedule, **after new data = Yes**, regressed below **80%** | Listed under Automatic runs as "After new data · below 80% is a regression". |
| 7.4 | Wait 30 s after adding it | **No** evaluation starts: the first check only records the newest `cbwa_kpis` export (from step 2) as already seen. |
| 7.5 | Change a value, then **Run now** on `cbwa_kpis`: `docker exec $PGC psql -U "$PGU" -d coralbay -c "update kpi_values set actual = 5.80 where kpi_code = 'WTR_CONS' and period_start = '2026-05-01'"` | Within about 15 s of the export finishing, a run labelled **"After new data"** starts. "Water consumption in May 2026" now fails (5.80 ≠ 5.08), so the run is **Regressed**. |
| 7.6 | Change it back (`actual = 5.08`) and export again | A new automatic run that passes again. |
| 7.7 | Edit the automatic run, set **Paused** | No more automatic runs. |

---

## 8. Workflow 1: `cbwa_assistant` (router, validator, fallback)

**Workflows → Create workflow** with a blank canvas. ID `cbwa_assistant`, name "CBWA assistant".

```
Request → Router ─(KPI words)──────→ cbwa_kpi_agent ──────┐
                 ├(district words)─→ cbwa_district_agent ──┼→ Numeric verifier ─→ Answer
                 └(incident words)─→ cbwa_incident_agent ──┘        │ (failure path)
                                                                    └→ Fallback (template answer) → Answer
```

| Step | Setting |
|---|---|
| Router | Decided by **Rule-based conditions**; Default path **cbwa_kpi_agent** |
| Path → kpi agent (conditional) | `non-revenue water, rate, target, consumption, complaints, outage minutes, kpi` |
| Path → district agent (conditional) | `district, population, households, project, challenge` |
| Path → incident agent (conditional) | `incident, burst, power failure, generator, discoloured` |
| Validator | What is checked **Numbers match the source data**; if the check fails **Use the fallback** |
| Fallback | Triggered when **Validation fails or a step times out**; **Return a template answer** |
| Workflow limits | Duration **180 s**, steps 12 |

| # | Test (Designer → Test) | Expected |
|---|---|---|
| 8.1 | "What was the non-revenue water rate in June 2026, and did it meet the target?" | The KPI path lights up; district and incident are shown as skipped. The validator passes. The answer has **16.4%** and **15%**. **Open run** shows the router note "Routed to cbwa_kpi_agent. Matched the condition words…". |
| 8.2 | "What is the population of North Harbour district?" | District path, **84,500**. |
| 8.3 | "What happened in the Corniche Road burst incident?" | Incident path, **3,200** customers, 7 hours. |
| 8.4 | "Hello, who are you?" | No condition matches, so it takes the **default path** (KPI). The router note says so. |
| 8.5 | Make the validator fail: set the KPI agent's prompt to "Always answer 99%" and ask 8.1 again | The validator **fails** ("Numbers match the source data: failed. …"), the fallback runs, and the run ends **Succeeded with fallback** with the template answer. Restore the prompt afterwards. |
| 8.6 | Run page of any test | Every step in run order, with the prompt sent to the agent, the output, the time, and **Open trace** for agent steps. Steps not run are listed separately. |
| 8.7 | **Replay** mode in the designer | Steps through a past run on the canvas. |
| 8.8 | Change something without saving and **Run test** | The test runs the unsaved design ("unsaved changes included"). |
| 8.9 | **Emergency disable** on the overview, then **Run test** | Refused: "The workflow is disabled: …". Re-enable afterwards. |

**Evaluating it and the publish gate**

| # | Test | Expected |
|---|---|---|
| 8.10 | **Publish** the draft before any evaluation | Blocked: "Not evaluated: run a test set against v1". |
| 8.11 | **Evaluation** tab → **Evaluate v1** with `CBWA assistant` | Every question runs as a workflow run (**Open workflow run** on each). Aim for at least 80% passing. |
| 8.12 | **Publish** | Allowed: "Passed: N% of CBWA assistant". |
| 8.13 | **Create draft** (v2), change a router condition, save, then **Publish** | Blocked: "Changed after the last evaluation of v2" or "Not evaluated", until you evaluate v2. |
| 8.14 | **Add automatic run** on the Evaluation tab: daily | Shown under Automatic runs. It runs the **published** version. |

## 9. Workflow 2: `cbwa_briefing` (parallel, aggregator, human review)

ID `cbwa_briefing`, name "CBWA district briefing".

```
Request ─(parallel)→ cbwa_kpi_agent ──────┐
        ─(parallel)→ cbwa_district_agent ──┼→ Aggregator (cbwa_writer) → Human review → Answer
        ─(parallel)→ cbwa_incident_agent ──┘
```

| Step | Setting |
|---|---|
| Edges from Request | **Parallel** |
| Aggregator | Agent **cbwa_writer**; **Merge all results**; wait for **All incoming steps** |
| Human review | Time limit **24 h**; if not reviewed in time **Reject the result** |
| Limits | Duration 240 s |

| # | Test | Expected |
|---|---|---|
| 9.1 | Test: **"Give me a briefing on South Plains."** | All three agents start **at the same time**; the aggregator starts only after all three finish. The run stops at **Waiting for review**. |
| 9.2 | Workflow **Overview** | "Waiting for a person" lists the run. |
| 9.3 | Open the run, then **Approve** after editing the answer (add a sentence) | The run finishes **Succeeded** with your edited answer. The step note says "Approved with an edited answer". |
| 9.4 | The answer should mention | NRW **21.7%** (June 2026), population **61,200**, Smart Meter Rollout **18,000** by December 2026, and the incidents: discoloured water (640 customers) and the smart meter data outage (**1,450** customers). |
| 9.5 | Run again and **Reject** with a reason | The review step fails; with no failure path the run ends **Failed** with your reason. |
| 9.6 | Set the aggregator to **First successful result** and run | No writer job: the answer is the first agent's answer. |

---

## 10. Monitoring and traces (throughout)

| # | Test | Expected |
|---|---|---|
| 10.1 | **Monitoring** after the steps above | Every agent job from the Playground, evaluations and workflows is listed, with the agent name (never a bare ID). |
| 10.2 | Any job → **Path and Trace** | The model calls and tool calls with input and output. A tool error (e.g. a validation error) is shown as failed. |
| 10.3 | Evidence row on a "not found" answer | It says when the tools returned nothing, so the answer is not backed by data. |

## 11. What each part covers

| Feature | Steps |
|---|---|
| PostgreSQL source (test, stored password) | 1 |
| Column mapping: frequency column, dimension, filter, Arabic names, targets | 2 |
| Records → documents: new / changed / removed, filters | 3, 4 |
| Ingestion runs (one run per export) | 2.4, 4.4 |
| Agents on indicators and on documents; search instructions | 5 |
| Playground, traces, Monitoring, evidence | 5, 10 |
| Test sets, CSV import, facts, AI judge, compare, regression | 6 |
| Add to test set (Playground, Monitoring, workflow run) | 7.1, 7.2 |
| Automatic runs after new data; regression below a minimum | 7.3 – 7.7 |
| Workflow router, validator, fallback, test, replay, unsaved test, disable | 8.1 – 8.9 |
| Workflow evaluation and publish gate | 8.10 – 8.14 |
| Parallel steps, aggregator, human review (approve / reject) | 9 |
