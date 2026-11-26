# Full platform test: Rimal Ministry of Health (fictional)

Rimal is a made-up state, and its Ministry of Health is made up too. No real ministry, person or figure is used, so this can be shown to clients.

| Table | Data | How it reaches KAP | KAP collection | Agent |
|---|---|---|---|---|
| `health_kpis` | 6 national health KPIs, monthly, quarterly and yearly, with targets, some per governorate | **Column mapping (no SQL)** → indicators | `rimal_kpis` | `rimal_kpi_agent` |
| `facilities` | One profile per hospital and clinic | **Records → searchable documents** | `rimal_facilities` | `rimal_facility_agent` |
| `circulars` | Ministerial circulars and decisions | **Records → searchable documents** | `rimal_circulars` | `rimal_policy_agent` |

`rimal_writer` has no data; it writes combined answers and briefings in workflows.

There are two workflows:
- **`rimal_health_assistant`:** a router sends each question to one of the three agents; a validator checks the numbers; a fallback answers if the check fails.
- **`rimal_minister_briefing`:** the three agents run in parallel, the writer drafts a briefing, and a person approves it before it is returned.

The files in this folder:
- `rimal_health_db.sql`: the database;
- `eval_rimal_kpis.csv` (9 questions), `eval_rimal_facilities.csv` (6), `eval_rimal_circulars.csv` (6), `eval_rimal_assistant.csv` (6);
- `DEMO_SCRIPT.md`: a 15-minute client demo built on the same setup.

---

## 0. Create the database (on the VM)
```bash
cd ~/platform-api && git pull
PGC=scai-chatbot-postgres-1
PGU=$(docker exec $PGC printenv POSTGRES_USER)
docker exec $PGC psql -U "$PGU" -d postgres -c "create database rimal_health"
docker cp samples/rimal_health/rimal_health_db.sql $PGC:/tmp/rimal_health_db.sql
docker exec $PGC psql -U "$PGU" -d rimal_health -v ro_pass='CHOOSE_A_PASSWORD' -f /tmp/rimal_health_db.sql
docker exec $PGC psql -U "$PGU" -d rimal_health -c "select (select count(*) from health_kpis) kpis, (select count(*) from facilities) facilities, (select count(*) from circulars) circulars"
```
**Expected:** `kpis 43 | facilities 7 | circulars 6`.

## 1. Data source
New PostgreSQL source `rimal_health_pg`:

| Field | Value |
|---|---|
| Host | `scai-chatbot-postgres-1` |
| Port | `5432` |
| Database | `rimal_health` |
| User | `rimal_ro` |
| Password | the one you chose |
| SSL mode | `disable` |

| # | Test | Expected |
|---|---|---|
| 1.1 | **Test connection** | Tables listed: `circulars`, `facilities`, `health_kpis` |
| 1.2 | Wrong password | A clear authentication error |

## 2. KPIs by column mapping (`rimal_kpis`)

| Setting | Value |
|---|---|
| Table | `public.health_kpis` |
| Series key | `kpi_code` |
| ID prefix | `rimal` |
| Period | `period_start` |
| Frequency | from column `frequency`: `monthly` → Monthly, `quarterly` → Quarterly, `yearly` → Yearly |
| Value | `actual` |
| Target | `target` |
| Dimension | `governorate`, main value `National` |
| Name EN / AR | `kpi_name_en` / `kpi_name_ar` |
| Unit / Source / Definition | `unit` / `data_source` / `definition` |
| Filter | `status` = `published` |
| Collection | `rimal_kpis` |

| # | Test | Expected |
|---|---|---|
| 2.1 | Preview, registry | **14 series**, e.g. `rimal.bed_occ.m.overall`, `rimal.bed_occ.m.capital_governorate`, `rimal.opd_wait.q.overall`, `rimal.vax_mmr.a.southern_governorate` |
| 2.2 | Preview, observations | **42 rows**. Periods like `2026-06`, `2026-Q2`, `2025`. The July 2026 draft (33 minutes) is absent. |
| 2.3 | **Run now** | 5 steps ✓ |
| 2.4 | **Run now** again with no change | Unchanged: nothing loaded into KAP |

## 3. Facilities as documents (`rimal_facilities`)

| Setting | Value |
|---|---|
| Table | `public.facilities` |
| ID column | `facility_id` |
| Title | `{name_en}` |
| Fields | `name_ar` ("Arabic name"), `facility_type` ("Type"), `governorate`, `beds` ("Beds"), `opened_year` ("Opened") |
| Sections | `services` ("Services"), `highlights` ("Highlights"), `challenges` ("Challenges") |
| Filter | `is_operational` **is true** |
| Collection | `rimal_facilities` |

| # | Test | Expected |
|---|---|---|
| 3.1 | Preview | **6 documents**. Old Quarter Hospital is absent. |
| 3.2 | Run now | **6 new**, then the KAP load (a few minutes) |
| 3.3 | Change a row, then **Run now**: `docker exec $PGC psql -U "$PGU" -d rimal_health -c "update facilities set beds = 640 where facility_id = 'RH-01'"` | **1 changed, 5 unchanged**. Set it back to 620 and run again. |

## 4. Circulars as documents (`rimal_circulars`)

| Setting | Value |
|---|---|
| Table | `public.circulars` |
| ID column | `ref_no` |
| Title | `{ref_no}: {title_en}` |
| Fields | `title_ar` ("Arabic title"), `doc_type` ("Type"), `issued_on` ("Issued"), `effective_from` ("Effective from"), `issued_by` ("Issued by"), `audience` ("Applies to") |
| Sections | `summary` ("Summary"), `key_changes` ("Key changes") |
| Filter | `is_public` **is true** |
| Collection | `rimal_circulars` |

| # | Test | Expected |
|---|---|---|
| 4.1 | Preview | **5 documents**. MOH-C-21/2026 (confidential) is absent. |
| 4.2 | Run now | **5 new** |
| 4.3 | Withdraw a circular, then **Run now**: `docker exec $PGC psql -U "$PGU" -d rimal_health -c "update circulars set is_public = false where ref_no = 'MOH-C-19/2026'"` | **1 removed**. Make it public again and run: **1 new**. |

---

## 5. Agents

| Agent | Type | Collection | Tools |
|---|---|---|---|
| `rimal_kpi_agent` | Planner | `rimal_kpis` | `find_indicator`, `get_indicator_value`, `actual_vs_target` (+ the time-series tool if available) |
| `rimal_facility_agent` | Planner | `rimal_facilities` | `hybrid_search`, entity_id `none` (use the dataset's "Create an agent" shortcut) |
| `rimal_policy_agent` | Planner | `rimal_circulars` | `hybrid_search`, entity_id `none` |
| `rimal_writer` | Workflow (or Planner without tools) | none | none |

**System prompts**
- **`rimal_kpi_agent`:**
  > You answer questions about the Rimal Ministry of Health's performance indicators, using only the indicator tools and the rimal_kpis collection. Always give the period, the value with its unit, and the target, and say whether the target is met. Figures are national (dimension empty) unless a governorate is named (dimension = the governorate's name, e.g. "Southern Governorate"). If there is no published value for the period asked, say so and give the latest period that has one. Never estimate a number. Answer in the language of the question.
- **`rimal_facility_agent`:**
  > You answer questions about the hospitals and clinics of the Rimal Ministry of Health, using only the search results from your collection. Quote numbers and names exactly. If nothing relevant is found, say there is no record of it. Answer in the language of the question.
- **`rimal_policy_agent`:**
  > You answer questions about the circulars and decisions of the Rimal Ministry of Health, using only the search results from your collection. Always cite the reference number (for example MOH-C-07/2026) and the date it takes effect. If nothing relevant is found, say there is no published circular on it. Answer in the language of the question.
- **`rimal_writer`:**
  > You receive a request and the answers of other agents. Write one clear answer, or a briefing when asked, using only those answers. Keep every number, unit, period, name and reference exactly as given. Say where information is missing. Do not add facts. Write in the language of the request.

For the two search agents, click **Insert search instructions** after the prompt.

| # | Playground question | Agent | Expected |
|---|---|---|---|
| 5.1 | What was the national hospital bed occupancy rate in June 2026? | kpi | **74.3%** (target 85) |
| 5.2 | What was childhood MMR vaccination coverage in 2025 in the Southern Governorate? | kpi | **92.4%**, below 95 |
| 5.3 | What was the emergency department median wait time in July 2026? | kpi | No published figure; latest is June (38 min). It must **not** say 33. |
| 5.4 | ما نسبة إشغال أسرّة المستشفيات على المستوى الوطني في يونيو 2026؟ | kpi | **74.3%**, answered in Arabic |
| 5.5 | How does Southern District Hospital reach remote patients? | facility | A telehealth hub linked to **9** primary health centres |
| 5.6 | Which services does Old Quarter Hospital offer? | facility | No current record. It must **not** list a maternity ward. |
| 5.7 | Can chronic patients have follow-up visits by video, and how are they paid? | policy | **MOH-C-07/2026**, insurers cover **100%** of the in-person rate |
| 5.8 | Is there a new pricing rule for imported medicines? | policy | No published circular. It must **not** mention "six countries". |

Also check:
- **5.9:** each run's trace (tool calls, tokens) and **Open in Monitoring**;
- **5.10:** the **Evidence** row when nothing is found.

## 6. Evaluations

| Test set | File | Run against |
|---|---|---|
| `Rimal health KPIs` | `eval_rimal_kpis.csv` | `rimal_kpi_agent` |
| `Rimal facilities` | `eval_rimal_facilities.csv` | `rimal_facility_agent` |
| `Rimal circulars` | `eval_rimal_circulars.csv` | `rimal_policy_agent` |
| `Rimal assistant` | `eval_rimal_assistant.csv` | workflow `rimal_health_assistant` (step 8) |

| # | Test | Expected |
|---|---|---|
| 6.1 | **Import CSV** (pick the file) | "Imported N questions" |
| 6.2 | Run each agent's set, labelled `baseline` | Finished. Aim for at least 80% passing; failures name the missing fact and the judge's reason. |
| 6.3 | Break `rimal_policy_agent`: prompt "Answer briefly from general knowledge." Then **Run again** | **Regressed**; **Compare with…** `baseline` shows the broken questions and "configuration changed in KAP". Restore the prompt. |
| 6.4 | **Use as expected answer** on a question the agent got right | The test question is updated |

## 7. Add to test set and automatic runs

| # | Test | Expected |
|---|---|---|
| 7.1 | Playground: *"What was bed occupancy in the Northern Governorate in June 2026?"* (71.2%), then **Add to test set** → `Rimal health KPIs` | `71.2` suggested as a fact; added, tagged `playground` |
| 7.2 | Same from a Monitoring run, and from a workflow run | Tagged `monitoring` / `workflow` |
| 7.3 | On `Rimal health KPIs`, click **Add automatic run**: `rimal_kpi_agent`, after new data, regressed below **80%** | Listed. No run starts by itself: it records the last export. |
| 7.4 | Change a value, then **Run now** on `rimal_kpis`: `docker exec $PGC psql -U "$PGU" -d rimal_health -c "update health_kpis set actual = 79.9 where kpi_code = 'BED_OCC' and governorate = 'National' and period_start = '2026-06-01'"` | An "After new data" run starts within about 15 s of the export. The June occupancy question fails (79.9 ≠ 74.3), so the run is **Regressed**. |
| 7.5 | Set it back to 74.3, export again | The automatic run passes again |

---

## 8. Workflow `rimal_health_assistant`
```
Request → Router ─(KPI words)───────→ rimal_kpi_agent ───────┐
                 ├(facility words)──→ rimal_facility_agent ──┼→ Numeric verifier → Answer
                 └(policy words)────→ rimal_policy_agent ────┘        └(failure)→ Fallback (template) → Answer
```

| Step | Setting |
|---|---|
| Router | **Rule-based conditions**; Default path **rimal_kpi_agent** |
| → kpi agent | `occupancy, wait time, waiting, coverage, prevalence, telehealth share, target, kpi` |
| → facility agent | `hospital, beds, clinic, centre, unit, facility, cots` |
| → policy agent | `circular, decision, rule, licence, prescription, insurers, campaign` |
| Validator | **Numbers match the source data**; if the check fails **Use the fallback** |
| Fallback | **Return a template answer** |
| Limits | Duration **180 s** |

| # | Test | Expected |
|---|---|---|
| 8.1 | "What was the emergency department median wait time in June 2026, and is it on target?" | KPI path; validator passes; **38** vs **30** |
| 8.2 | "Which clinic serves the villages of the Southern Governorate?" | Facility path; **Wadi Sareer Mobile Clinic** |
| 8.3 | "What decision launched the MMR catch-up campaign?" | Policy path; **MOH-D-03/2026** |
| 8.4 | "Good morning" | Default path (KPI); the router note says no condition matched |
| 8.5 | KPI agent prompt set to "Always answer 99%", ask 8.1 | Validator **fails**, fallback answers: **Succeeded with fallback**. Restore the prompt. |
| 8.6 | Run page, Replay, unsaved-change test, Emergency disable | As in the Coral Bay plan: steps in order with traces; the replay steps through the run; the unsaved test runs; a disabled workflow is refused. |
| 8.7 | **Publish** with no evaluation | Blocked: "Not evaluated" |
| 8.8 | Evaluation tab → **Evaluate v1** with `Rimal assistant`, then **Publish** | Allowed once at least 80% passes |
| 8.9 | New draft, change a condition, **Publish** | Blocked until the new draft is evaluated |

## 9. Workflow `rimal_minister_briefing`
```
Request ─(parallel)→ rimal_kpi_agent ───────┐
        ─(parallel)→ rimal_facility_agent ──┼→ Aggregator (rimal_writer) → Human review → Answer
        ─(parallel)→ rimal_policy_agent ────┘
```
Aggregator: **Merge all results**, wait for **All incoming steps**. Human review: **24 h**, then **Reject the result**. Duration 240 s.

| # | Test | Expected |
|---|---|---|
| 9.1 | "Prepare a briefing for the Minister on health services in the Southern Governorate." | The three agents start together; the writer starts after all three; the run waits for review |
| 9.2 | Workflow overview | Listed under "Waiting for a person" |
| 9.3 | **Approve** with an edit | **Succeeded** with the edited text |
| 9.4 | The briefing should mention | MMR coverage **92.4%** (2025, below 95), bed occupancy **62.8%** (June 2026), Southern District Hospital and its telehealth hub (**9** centres), Wadi Sareer Mobile Clinic (**6** villages), the catch-up campaign **MOH-D-03/2026** (**4,800** children) |
| 9.5 | Same question in Arabic: "أعدّ إيجازاً للوزير عن الخدمات الصحية في المحافظة الجنوبية" | The briefing is in Arabic with the same figures |
| 9.6 | **Reject** with a reason | **Failed** with your reason |

## 10. Feature coverage
| Feature | Steps |
|---|---|
| PostgreSQL source | 1 |
| Column mapping: three frequencies from a column, dimension, filter, Arabic names, targets | 2 |
| Records → documents: new / changed / removed, filters | 3, 4 |
| Agents on indicators and documents; Arabic questions | 5 |
| Playground, traces, Monitoring, evidence | 5 |
| Test sets, CSV file import, judge, compare, regression | 6 |
| Add to test set; automatic runs after new data | 7 |
| Router, validator, fallback, publish gate | 8 |
| Parallel steps, aggregator, human review, Arabic briefing | 9 |
