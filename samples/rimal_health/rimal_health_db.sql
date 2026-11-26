-- Rimal Ministry of Health (fictional state, fictional ministry) — demo and test database.
--
-- Run as a superuser in an empty database named rimal_health:
--   psql -U <admin> -d rimal_health -v ro_pass='<choose a password>' -f rimal_health_db.sql
--
--   health_kpis  national health KPIs: monthly, quarterly AND yearly rows (frequency read from a column),
--                a governorate dimension with "National" as the main series, 2030-style targets, Arabic
--                names, and one draft row a filter must leave out          → Column mapping (indicators)
--   facilities   one profile per hospital / clinic (one closed facility a filter must leave out)
--                                                                           → Records (searchable documents)
--   circulars    ministerial circulars and decisions (one confidential draft a filter must leave out)
--                                                                           → Records (searchable documents)
-- Every number is used by the test sets (eval_rimal_*.csv) and TEST_PLAN.md.

\set ON_ERROR_STOP on

-- ---------------------------------------------------------------- KPIs
create table health_kpis (
  value_id      serial primary key,
  kpi_code      text not null,
  kpi_name_en   text not null,
  kpi_name_ar   text not null,
  unit          text not null,
  frequency     text not null check (frequency in ('monthly', 'quarterly', 'yearly')),
  period_start  date not null,               -- first day of the month, quarter or year
  governorate   text not null,               -- 'National' = the whole state
  actual        numeric,
  target        numeric,
  data_source   text not null default 'Rimal Health Information System',
  definition    text,
  status        text not null default 'published' check (status in ('published', 'draft'))
);

insert into health_kpis (kpi_code, kpi_name_en, kpi_name_ar, unit, frequency, period_start, governorate, actual, target, definition) values
-- Hospital bed occupancy, % (target 85; above it hospitals are over-stretched); June also per governorate
('BED_OCC', 'Hospital bed occupancy rate', 'معدل إشغال أسرّة المستشفيات', '%', 'monthly', '2026-01-01', 'National', 78.4, 85, 'Share of staffed inpatient beds occupied at midnight, averaged over the month.'),
('BED_OCC', 'Hospital bed occupancy rate', 'معدل إشغال أسرّة المستشفيات', '%', 'monthly', '2026-02-01', 'National', 81.2, 85, 'Share of staffed inpatient beds occupied at midnight, averaged over the month.'),
('BED_OCC', 'Hospital bed occupancy rate', 'معدل إشغال أسرّة المستشفيات', '%', 'monthly', '2026-03-01', 'National', 83.5, 85, 'Share of staffed inpatient beds occupied at midnight, averaged over the month.'),
('BED_OCC', 'Hospital bed occupancy rate', 'معدل إشغال أسرّة المستشفيات', '%', 'monthly', '2026-04-01', 'National', 80.1, 85, 'Share of staffed inpatient beds occupied at midnight, averaged over the month.'),
('BED_OCC', 'Hospital bed occupancy rate', 'معدل إشغال أسرّة المستشفيات', '%', 'monthly', '2026-05-01', 'National', 76.9, 85, 'Share of staffed inpatient beds occupied at midnight, averaged over the month.'),
('BED_OCC', 'Hospital bed occupancy rate', 'معدل إشغال أسرّة المستشفيات', '%', 'monthly', '2026-06-01', 'National', 74.3, 85, 'Share of staffed inpatient beds occupied at midnight, averaged over the month.'),
('BED_OCC', 'Hospital bed occupancy rate', 'معدل إشغال أسرّة المستشفيات', '%', 'monthly', '2026-06-01', 'Capital Governorate', 88.6, 85, 'Share of staffed inpatient beds occupied at midnight, averaged over the month.'),
('BED_OCC', 'Hospital bed occupancy rate', 'معدل إشغال أسرّة المستشفيات', '%', 'monthly', '2026-06-01', 'Northern Governorate', 71.2, 85, 'Share of staffed inpatient beds occupied at midnight, averaged over the month.'),
('BED_OCC', 'Hospital bed occupancy rate', 'معدل إشغال أسرّة المستشفيات', '%', 'monthly', '2026-06-01', 'Coastal Governorate', 69.5, 85, 'Share of staffed inpatient beds occupied at midnight, averaged over the month.'),
('BED_OCC', 'Hospital bed occupancy rate', 'معدل إشغال أسرّة المستشفيات', '%', 'monthly', '2026-06-01', 'Southern Governorate', 62.8, 85, 'Share of staffed inpatient beds occupied at midnight, averaged over the month.'),
-- Emergency department median wait, minutes (target 30)
('ED_WAIT', 'Emergency department median wait time', 'متوسط وقت الانتظار في أقسام الطوارئ', 'minutes', 'monthly', '2026-01-01', 'National', 52, 30, 'Median time from arrival to first assessment by a doctor in public emergency departments.'),
('ED_WAIT', 'Emergency department median wait time', 'متوسط وقت الانتظار في أقسام الطوارئ', 'minutes', 'monthly', '2026-02-01', 'National', 49, 30, 'Median time from arrival to first assessment by a doctor in public emergency departments.'),
('ED_WAIT', 'Emergency department median wait time', 'متوسط وقت الانتظار في أقسام الطوارئ', 'minutes', 'monthly', '2026-03-01', 'National', 47, 30, 'Median time from arrival to first assessment by a doctor in public emergency departments.'),
('ED_WAIT', 'Emergency department median wait time', 'متوسط وقت الانتظار في أقسام الطوارئ', 'minutes', 'monthly', '2026-04-01', 'National', 45, 30, 'Median time from arrival to first assessment by a doctor in public emergency departments.'),
('ED_WAIT', 'Emergency department median wait time', 'متوسط وقت الانتظار في أقسام الطوارئ', 'minutes', 'monthly', '2026-05-01', 'National', 41, 30, 'Median time from arrival to first assessment by a doctor in public emergency departments.'),
('ED_WAIT', 'Emergency department median wait time', 'متوسط وقت الانتظار في أقسام الطوارئ', 'minutes', 'monthly', '2026-06-01', 'National', 38, 30, 'Median time from arrival to first assessment by a doctor in public emergency departments.'),
-- Days to first specialist appointment, quarterly (target 14)
('OPD_WAIT', 'Days to first specialist appointment', 'أيام الانتظار لأول موعد مع أخصائي', 'days', 'quarterly', '2025-01-01', 'National', 34, 14, 'Median days from referral to the first outpatient appointment with a specialist.'),
('OPD_WAIT', 'Days to first specialist appointment', 'أيام الانتظار لأول موعد مع أخصائي', 'days', 'quarterly', '2025-04-01', 'National', 31, 14, 'Median days from referral to the first outpatient appointment with a specialist.'),
('OPD_WAIT', 'Days to first specialist appointment', 'أيام الانتظار لأول موعد مع أخصائي', 'days', 'quarterly', '2025-07-01', 'National', 29, 14, 'Median days from referral to the first outpatient appointment with a specialist.'),
('OPD_WAIT', 'Days to first specialist appointment', 'أيام الانتظار لأول موعد مع أخصائي', 'days', 'quarterly', '2025-10-01', 'National', 27, 14, 'Median days from referral to the first outpatient appointment with a specialist.'),
('OPD_WAIT', 'Days to first specialist appointment', 'أيام الانتظار لأول موعد مع أخصائي', 'days', 'quarterly', '2026-01-01', 'National', 24, 14, 'Median days from referral to the first outpatient appointment with a specialist.'),
('OPD_WAIT', 'Days to first specialist appointment', 'أيام الانتظار لأول موعد مع أخصائي', 'days', 'quarterly', '2026-04-01', 'National', 21, 14, 'Median days from referral to the first outpatient appointment with a specialist.'),
-- Childhood MMR vaccination coverage, % yearly (target 95); 2025 also per governorate
('VAX_MMR', 'Childhood MMR vaccination coverage', 'تغطية تطعيم الأطفال ضد الحصبة والنكاف والحصبة الألمانية', '%', 'yearly', '2021-01-01', 'National', 93.1, 95, 'Children aged 2 who received two doses of the measles, mumps and rubella vaccine.'),
('VAX_MMR', 'Childhood MMR vaccination coverage', 'تغطية تطعيم الأطفال ضد الحصبة والنكاف والحصبة الألمانية', '%', 'yearly', '2022-01-01', 'National', 94.0, 95, 'Children aged 2 who received two doses of the measles, mumps and rubella vaccine.'),
('VAX_MMR', 'Childhood MMR vaccination coverage', 'تغطية تطعيم الأطفال ضد الحصبة والنكاف والحصبة الألمانية', '%', 'yearly', '2023-01-01', 'National', 95.2, 95, 'Children aged 2 who received two doses of the measles, mumps and rubella vaccine.'),
('VAX_MMR', 'Childhood MMR vaccination coverage', 'تغطية تطعيم الأطفال ضد الحصبة والنكاف والحصبة الألمانية', '%', 'yearly', '2024-01-01', 'National', 96.4, 95, 'Children aged 2 who received two doses of the measles, mumps and rubella vaccine.'),
('VAX_MMR', 'Childhood MMR vaccination coverage', 'تغطية تطعيم الأطفال ضد الحصبة والنكاف والحصبة الألمانية', '%', 'yearly', '2025-01-01', 'National', 97.1, 95, 'Children aged 2 who received two doses of the measles, mumps and rubella vaccine.'),
('VAX_MMR', 'Childhood MMR vaccination coverage', 'تغطية تطعيم الأطفال ضد الحصبة والنكاف والحصبة الألمانية', '%', 'yearly', '2025-01-01', 'Capital Governorate', 98.0, 95, 'Children aged 2 who received two doses of the measles, mumps and rubella vaccine.'),
('VAX_MMR', 'Childhood MMR vaccination coverage', 'تغطية تطعيم الأطفال ضد الحصبة والنكاف والحصبة الألمانية', '%', 'yearly', '2025-01-01', 'Northern Governorate', 96.8, 95, 'Children aged 2 who received two doses of the measles, mumps and rubella vaccine.'),
('VAX_MMR', 'Childhood MMR vaccination coverage', 'تغطية تطعيم الأطفال ضد الحصبة والنكاف والحصبة الألمانية', '%', 'yearly', '2025-01-01', 'Coastal Governorate', 97.5, 95, 'Children aged 2 who received two doses of the measles, mumps and rubella vaccine.'),
('VAX_MMR', 'Childhood MMR vaccination coverage', 'تغطية تطعيم الأطفال ضد الحصبة والنكاف والحصبة الألمانية', '%', 'yearly', '2025-01-01', 'Southern Governorate', 92.4, 95, 'Children aged 2 who received two doses of the measles, mumps and rubella vaccine.'),
-- Adult diabetes prevalence, % yearly (target 15)
('DIAB_PREV', 'Adult diabetes prevalence', 'انتشار السكري بين البالغين', '%', 'yearly', '2021-01-01', 'National', 17.8, 15, 'Adults aged 20 to 79 living with diagnosed diabetes.'),
('DIAB_PREV', 'Adult diabetes prevalence', 'انتشار السكري بين البالغين', '%', 'yearly', '2022-01-01', 'National', 17.5, 15, 'Adults aged 20 to 79 living with diagnosed diabetes.'),
('DIAB_PREV', 'Adult diabetes prevalence', 'انتشار السكري بين البالغين', '%', 'yearly', '2023-01-01', 'National', 17.2, 15, 'Adults aged 20 to 79 living with diagnosed diabetes.'),
('DIAB_PREV', 'Adult diabetes prevalence', 'انتشار السكري بين البالغين', '%', 'yearly', '2024-01-01', 'National', 16.9, 15, 'Adults aged 20 to 79 living with diagnosed diabetes.'),
('DIAB_PREV', 'Adult diabetes prevalence', 'انتشار السكري بين البالغين', '%', 'yearly', '2025-01-01', 'National', 16.6, 15, 'Adults aged 20 to 79 living with diagnosed diabetes.'),
-- Telehealth share of outpatient consultations, % quarterly (target 20)
('TELE_SHARE', 'Telehealth share of outpatient consultations', 'حصة الاستشارات الخارجية عن بُعد', '%', 'quarterly', '2025-01-01', 'National', 8.5, 20, 'Outpatient consultations held by video or phone, as a share of all outpatient consultations.'),
('TELE_SHARE', 'Telehealth share of outpatient consultations', 'حصة الاستشارات الخارجية عن بُعد', '%', 'quarterly', '2025-04-01', 'National', 10.2, 20, 'Outpatient consultations held by video or phone, as a share of all outpatient consultations.'),
('TELE_SHARE', 'Telehealth share of outpatient consultations', 'حصة الاستشارات الخارجية عن بُعد', '%', 'quarterly', '2025-07-01', 'National', 12.8, 20, 'Outpatient consultations held by video or phone, as a share of all outpatient consultations.'),
('TELE_SHARE', 'Telehealth share of outpatient consultations', 'حصة الاستشارات الخارجية عن بُعد', '%', 'quarterly', '2025-10-01', 'National', 14.1, 20, 'Outpatient consultations held by video or phone, as a share of all outpatient consultations.'),
('TELE_SHARE', 'Telehealth share of outpatient consultations', 'حصة الاستشارات الخارجية عن بُعد', '%', 'quarterly', '2026-01-01', 'National', 16.9, 20, 'Outpatient consultations held by video or phone, as a share of all outpatient consultations.'),
('TELE_SHARE', 'Telehealth share of outpatient consultations', 'حصة الاستشارات الخارجية عن بُعد', '%', 'quarterly', '2026-04-01', 'National', 19.4, 20, 'Outpatient consultations held by video or phone, as a share of all outpatient consultations.');

-- not yet validated: must never reach KAP
insert into health_kpis (kpi_code, kpi_name_en, kpi_name_ar, unit, frequency, period_start, governorate, actual, target, definition, status) values
('ED_WAIT', 'Emergency department median wait time', 'متوسط وقت الانتظار في أقسام الطوارئ', 'minutes', 'monthly', '2026-07-01', 'National', 33, 30, 'Median time from arrival to first assessment by a doctor in public emergency departments.', 'draft');

-- ---------------------------------------------------------------- facilities
create table facilities (
  facility_id     text primary key,
  name_en         text not null,
  name_ar         text not null,
  facility_type   text not null,
  governorate     text not null,
  beds            integer,
  opened_year     integer,
  services        text not null,
  highlights      text not null,
  challenges      text not null,
  is_operational  boolean not null default true
);

insert into facilities values
('RH-01', 'Rimal General Hospital', 'مستشفى الرمال العام', 'Tertiary hospital', 'Capital Governorate', 620, 1998,
 'Emergency medicine, cardiology, oncology, neurosurgery, intensive care and the national trauma centre.',
 'A new cardiac unit with 40 beds opened in January 2026, including two catheterisation labs. The hospital received international accreditation in 2024.',
 'Bed occupancy in the Capital Governorate is above the 85% target, so planned surgery is sometimes postponed in the winter months.',
 true),
('RH-02', 'Northern Regional Hospital', 'مستشفى المنطقة الشمالية', 'Regional hospital', 'Northern Governorate', 240, 2009,
 'Emergency medicine, general surgery, internal medicine, paediatrics and maternity.',
 'The emergency department is being expanded from 22 to 40 treatment bays; the expansion is due in September 2026.',
 'A shortage of anaesthetists limits the number of operating sessions; three posts have been vacant since 2025.',
 true),
('RH-03', 'Coastal Women and Children''s Hospital', 'مستشفى الساحل للنساء والأطفال', 'Specialist hospital', 'Coastal Governorate', 180, 2015,
 'Obstetrics, gynaecology, paediatrics and neonatal intensive care.',
 'The neonatal intensive care unit has 24 cots and admits about 1,100 babies a year.',
 'Demand for paediatric outpatient appointments grows faster than the number of paediatricians.',
 true),
('RH-04', 'Southern District Hospital', 'مستشفى المنطقة الجنوبية', 'District hospital', 'Southern Governorate', 110, 2004,
 'Emergency medicine, internal medicine, general surgery and dialysis.',
 'A telehealth hub links the hospital''s specialists to 9 primary health centres, so patients in remote villages are seen without travelling.',
 'Long distances to the hospital, and the lowest childhood vaccination coverage of the four governorates.',
 true),
('PHC-11', 'Al Nakheel Primary Health Centre', 'مركز النخيل للرعاية الصحية الأولية', 'Primary health centre', 'Capital Governorate', null, 2012,
 'Family medicine, chronic disease clinics, vaccination, dental care and a pharmacy.',
 'Open from 7 am to 11 pm every day, with 38,000 registered patients.',
 'Evening clinics are fully booked about two weeks ahead.',
 true),
('PHC-27', 'Wadi Sareer Mobile Clinic', 'العيادة المتنقلة في وادي السرير', 'Mobile clinic', 'Southern Governorate', null, 2023,
 'General consultations, vaccination, antenatal checks and chronic disease follow-up.',
 'A mobile clinic that visits 6 villages twice a week.',
 'The service stops during sandstorms because the vehicle cannot travel safely.',
 true),
('RH-09', 'Old Quarter Hospital', 'مستشفى الحي القديم', 'District hospital', 'Capital Governorate', 90, 1976,
 'Former general wards and a maternity ward.',
 'Closed in 2024; its services moved to Rimal General Hospital.',
 'None.',
 false);

-- ---------------------------------------------------------------- circulars and decisions
create table circulars (
  ref_no          text primary key,
  title_en        text not null,
  title_ar        text not null,
  doc_type        text not null,               -- Circular | Ministerial decision
  issued_on       date not null,
  effective_from  date not null,
  issued_by       text not null,
  audience        text not null,
  summary         text not null,
  key_changes     text not null,
  is_public       boolean not null default true
);

insert into circulars values
('MOH-C-07/2026', 'Telehealth consultations for follow-up visits', 'الاستشارات الصحية عن بُعد لزيارات المتابعة', 'Circular', '2026-01-15', '2026-02-01',
 'Department of Digital Health', 'Hospitals, health centres and health insurers',
 'Follow-up visits of patients with chronic conditions may be held by video consultation instead of in person.',
 'Insurers must cover video follow-up visits at 100% of the in-person rate. A first visit for a new condition must still be in person. Consultations must use the ministry''s approved telehealth platform.',
 true),
('MOH-C-11/2026', 'Emergency department triage standards', 'معايير الفرز في أقسام الطوارئ', 'Circular', '2026-02-20', '2026-03-01',
 'Department of Hospital Services', 'All public and private emergency departments',
 'Sets the time limits for triage and first assessment in emergency departments.',
 'Every patient must be triaged within 15 minutes of arrival. Category 1 patients are seen immediately. Hospitals report their waiting times to the ministry every week.',
 true),
('MOH-C-14/2026', 'Repeat prescriptions for chronic medication', 'صرف الأدوية المزمنة بوصفات متكررة', 'Circular', '2026-03-10', '2026-04-01',
 'Department of Pharmacy and Drug Control', 'Pharmacies and prescribing doctors',
 'Allows longer repeat prescriptions for stable patients with chronic conditions.',
 'Chronic medication may be dispensed for up to 90 days at a time, instead of 30. Controlled medicines are excluded.',
 true),
('MOH-D-03/2026', 'Free MMR catch-up vaccination campaign in the Southern Governorate', 'حملة تطعيم استدراكية مجانية ضد الحصبة في المحافظة الجنوبية', 'Ministerial decision', '2026-02-05', '2026-03-01',
 'Office of the Minister', 'Health centres and schools in the Southern Governorate',
 'Launches a free catch-up campaign for children who missed MMR doses, from March to May 2026.',
 'The campaign targets 4,800 children aged 2 to 12. Vaccination teams visit schools and the Wadi Sareer Mobile Clinic adds extra village rounds.',
 true),
('MOH-C-19/2026', 'Online licence renewal for private clinics', 'تجديد تراخيص العيادات الخاصة إلكترونياً', 'Circular', '2026-05-12', '2026-06-01',
 'Department of Licensing', 'Private clinics and medical centres',
 'Private clinic licences are renewed online only, through the ministry''s e-services portal.',
 'Paper applications are no longer accepted. The renewal fee is 1,500 and the licence is valid for 2 years.',
 true),
('MOH-C-21/2026', 'Draft pricing review for imported medicines', 'مسودة مراجعة أسعار الأدوية المستوردة', 'Circular', '2026-06-18', '2026-09-01',
 'Department of Pharmacy and Drug Control', 'Internal: not for publication',
 'Confidential draft: prices of imported medicines would be set against a reference pricing basket of six countries.',
 'Under discussion; not approved.',
 false);

-- ---------------------------------------------------------------- read-only login for the platform
create role rimal_ro login password :'ro_pass';
grant connect on database rimal_health to rimal_ro;
grant usage on schema public to rimal_ro;
grant select on health_kpis, facilities, circulars to rimal_ro;
