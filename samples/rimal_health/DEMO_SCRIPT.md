# Client demo: "Ask your ministry's data" (about 15 minutes)

**Before the meeting:** do steps 0–5, 8 and 9 of `TEST_PLAN.md`, and run each test set once so there is a `baseline`. Keep one browser tab on the Playground and one on the `rimal_minister_briefing` designer.

**Say at the start:** the Rimal Ministry of Health is fictional, and all of its data is made up for the demo.

---

### 1. Your data, connected in minutes (3 min)
**Data sources → rimal_health_pg.**
> "This is a normal PostgreSQL database, like the ones your departments already have. We connect with a read-only login: the platform can never change your data."

Open the **rimal_kpis** dataset → **Column mapping**.
> "No SQL and no IT ticket. We point at the table and say which column is the indicator, the period and the target. Drafts are filtered out: only published figures reach the assistant."

Open **rimal_facilities** → **Preview**.
> "Text records become searchable documents: hospital profiles here, circulars next door. A closed hospital or a confidential circular is filtered out before anything leaves the database."

### 2. Ask questions (4 min), in the Playground
1. *"Is the emergency department median wait time on target in June 2026?"* → 38 minutes against a target of 30.
   > "Exact figures, with the target. Click the trace: here is the tool the assistant used, and the exact value it read."
2. *ما نسبة إشغال أسرّة المستشفيات على المستوى الوطني في يونيو 2026؟* → 74.3%.
   > "The same, in Arabic."
3. *"Can chronic patients have follow-up visits by video, and how are they paid?"* → MOH-C-07/2026, 100% of the in-person rate.
   > "Policy answers name their reference."
4. *"Is there a new pricing rule for imported medicines?"* → no published circular.
   > "A confidential draft exists in the database, but it was filtered out. The assistant cannot leak it, and it does not invent an answer."

### 3. A briefing for the Minister (4 min)
In the **rimal_minister_briefing** designer, open **Test** and ask: *"Prepare a briefing for the Minister on health services in the Southern Governorate."*
> "Three specialist assistants work at the same time: indicators, facilities, policy. A writer combines their answers into one briefing."

The run stops at **Waiting for review**. Open it, edit one sentence, then **Approve**.
> "Nothing reaches the Minister without a person's approval, and every step is recorded: who asked, what each assistant found, who approved."

### 4. Proof that it stays correct (3 min)
Open **Agents → Evaluation datasets → Rimal health KPIs**.
> "These are questions your analysts checked by hand. Every change is tested against them before it goes live."

Show a **Regressed** run and its **Compare with…** view.
> "When a configuration change or a data reload breaks an answer, we see exactly which question broke. The workflow cannot be published until it passes."

Show the **Automatic runs** on the test set.
> "When new data is loaded, the questions are asked again automatically."

### 5. Close (1 min)
> "Your databases, your documents, your questions. Answers in Arabic and English, with sources, human approval where it matters, and continuous testing."

---

**Have ready if asked:**
- **Where does the data stay?** In your database. The platform reads it with a read-only login, and the assistant works on the published copy in the platform on your infrastructure.
- **Can it count or total across many rows?** Live SQL questions are the next step and need one tool enabled in KAP.
- **Who can see what?** Sign-in and access control are on the roadmap, before roll-out to departments.
