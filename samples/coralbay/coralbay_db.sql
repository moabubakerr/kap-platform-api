-- Coral Bay Water Authority (fictional) — test database for the KAP platform.
--
-- Run as a superuser in an empty database, with the read-only password as a variable:
--   psql -U <admin> -d coralbay -v ro_pass='<choose a password>' -f coralbay_db.sql
--
-- Tables and what they test:
--   kpi_values  numbers per period → "Column mapping (no SQL)": monthly AND quarterly rows (frequency
--               read from a column), a district dimension with "All districts" as the main series,
--               targets, Arabic names, and a draft row that a filter must leave out
--   districts   one text record per district → "Records → searchable documents" (one inactive row
--               that a filter must leave out)
--   incidents   one text record per incident → a second records dataset (one internal row that a
--               filter must leave out)
-- Every number below is referenced by the test cases in TEST_PLAN.md and the eval_*.csv files.

\set ON_ERROR_STOP on

-- ---------------------------------------------------------------- KPIs
create table kpi_values (
  value_id      serial primary key,
  kpi_code      text not null,
  kpi_name_en   text not null,
  kpi_name_ar   text not null,
  unit          text not null,
  frequency     text not null check (frequency in ('monthly', 'quarterly')),
  period_start  date not null,                 -- first day of the month or quarter
  district      text not null,                 -- 'All districts' = the authority as a whole
  actual        numeric,
  target        numeric,
  data_source   text not null default 'CBWA Operations Dashboard',
  definition    text,
  status        text not null default 'published' check (status in ('published', 'draft'))
);

insert into kpi_values (kpi_code, kpi_name_en, kpi_name_ar, unit, frequency, period_start, district, actual, target, definition) values
-- Water consumption, million m³ per month (target 4.50)
('WTR_CONS', 'Water consumption', 'استهلاك المياه', 'million m³', 'monthly', '2026-01-01', 'All districts', 4.10, 4.50, 'Total drinking water delivered to customers in the month.'),
('WTR_CONS', 'Water consumption', 'استهلاك المياه', 'million m³', 'monthly', '2026-02-01', 'All districts', 3.95, 4.50, 'Total drinking water delivered to customers in the month.'),
('WTR_CONS', 'Water consumption', 'استهلاك المياه', 'million m³', 'monthly', '2026-03-01', 'All districts', 4.30, 4.50, 'Total drinking water delivered to customers in the month.'),
('WTR_CONS', 'Water consumption', 'استهلاك المياه', 'million m³', 'monthly', '2026-04-01', 'All districts', 4.62, 4.50, 'Total drinking water delivered to customers in the month.'),
('WTR_CONS', 'Water consumption', 'استهلاك المياه', 'million m³', 'monthly', '2026-05-01', 'All districts', 5.08, 4.50, 'Total drinking water delivered to customers in the month.'),
('WTR_CONS', 'Water consumption', 'استهلاك المياه', 'million m³', 'monthly', '2026-06-01', 'All districts', 5.41, 4.50, 'Total drinking water delivered to customers in the month.'),
-- Non-revenue water rate, % (target 15.0); June also per district
('NRW_RATE', 'Non-revenue water rate', 'نسبة المياه غير المحتسبة', '%', 'monthly', '2026-01-01', 'All districts', 18.2, 15.0, 'Share of water produced that is lost to leaks, theft or metering errors.'),
('NRW_RATE', 'Non-revenue water rate', 'نسبة المياه غير المحتسبة', '%', 'monthly', '2026-02-01', 'All districts', 17.9, 15.0, 'Share of water produced that is lost to leaks, theft or metering errors.'),
('NRW_RATE', 'Non-revenue water rate', 'نسبة المياه غير المحتسبة', '%', 'monthly', '2026-03-01', 'All districts', 17.5, 15.0, 'Share of water produced that is lost to leaks, theft or metering errors.'),
('NRW_RATE', 'Non-revenue water rate', 'نسبة المياه غير المحتسبة', '%', 'monthly', '2026-04-01', 'All districts', 17.1, 15.0, 'Share of water produced that is lost to leaks, theft or metering errors.'),
('NRW_RATE', 'Non-revenue water rate', 'نسبة المياه غير المحتسبة', '%', 'monthly', '2026-05-01', 'All districts', 16.8, 15.0, 'Share of water produced that is lost to leaks, theft or metering errors.'),
('NRW_RATE', 'Non-revenue water rate', 'نسبة المياه غير المحتسبة', '%', 'monthly', '2026-06-01', 'All districts', 16.4, 15.0, 'Share of water produced that is lost to leaks, theft or metering errors.'),
('NRW_RATE', 'Non-revenue water rate', 'نسبة المياه غير المحتسبة', '%', 'monthly', '2026-06-01', 'North Harbour', 12.1, 15.0, 'Share of water produced that is lost to leaks, theft or metering errors.'),
('NRW_RATE', 'Non-revenue water rate', 'نسبة المياه غير المحتسبة', '%', 'monthly', '2026-06-01', 'South Plains', 21.7, 15.0, 'Share of water produced that is lost to leaks, theft or metering errors.'),
('NRW_RATE', 'Non-revenue water rate', 'نسبة المياه غير المحتسبة', '%', 'monthly', '2026-06-01', 'East Ridge', 15.3, 15.0, 'Share of water produced that is lost to leaks, theft or metering errors.'),
('NRW_RATE', 'Non-revenue water rate', 'نسبة المياه غير المحتسبة', '%', 'monthly', '2026-06-01', 'West Marina', 18.9, 15.0, 'Share of water produced that is lost to leaks, theft or metering errors.'),
-- Complaints resolved within 48 hours, % (target 90.0); July is a draft and must not be exported
('CMPL_48H', 'Complaints resolved within 48 hours', 'الشكاوى المحلولة خلال 48 ساعة', '%', 'monthly', '2026-01-01', 'All districts', 82.5, 90.0, 'Customer complaints closed within 48 hours of being logged.'),
('CMPL_48H', 'Complaints resolved within 48 hours', 'الشكاوى المحلولة خلال 48 ساعة', '%', 'monthly', '2026-02-01', 'All districts', 84.0, 90.0, 'Customer complaints closed within 48 hours of being logged.'),
('CMPL_48H', 'Complaints resolved within 48 hours', 'الشكاوى المحلولة خلال 48 ساعة', '%', 'monthly', '2026-03-01', 'All districts', 86.1, 90.0, 'Customer complaints closed within 48 hours of being logged.'),
('CMPL_48H', 'Complaints resolved within 48 hours', 'الشكاوى المحلولة خلال 48 ساعة', '%', 'monthly', '2026-04-01', 'All districts', 85.3, 90.0, 'Customer complaints closed within 48 hours of being logged.'),
('CMPL_48H', 'Complaints resolved within 48 hours', 'الشكاوى المحلولة خلال 48 ساعة', '%', 'monthly', '2026-05-01', 'All districts', 88.7, 90.0, 'Customer complaints closed within 48 hours of being logged.'),
('CMPL_48H', 'Complaints resolved within 48 hours', 'الشكاوى المحلولة خلال 48 ساعة', '%', 'monthly', '2026-06-01', 'All districts', 90.2, 90.0, 'Customer complaints closed within 48 hours of being logged.'),
-- Average outage minutes per customer, quarterly (target 35.0)
('OUTAGE_MIN', 'Average outage minutes per customer', 'متوسط دقائق الانقطاع لكل مشترك', 'minutes', 'quarterly', '2026-01-01', 'All districts', 41.5, 35.0, 'Minutes without water supply per customer in the quarter, planned and unplanned.'),
('OUTAGE_MIN', 'Average outage minutes per customer', 'متوسط دقائق الانقطاع لكل مشترك', 'minutes', 'quarterly', '2026-04-01', 'All districts', 36.8, 35.0, 'Minutes without water supply per customer in the quarter, planned and unplanned.');

insert into kpi_values (kpi_code, kpi_name_en, kpi_name_ar, unit, frequency, period_start, district, actual, target, definition, status) values
('CMPL_48H', 'Complaints resolved within 48 hours', 'الشكاوى المحلولة خلال 48 ساعة', '%', 'monthly', '2026-07-01', 'All districts', 93.0, 90.0, 'Customer complaints closed within 48 hours of being logged.', 'draft');

-- ---------------------------------------------------------------- districts
create table districts (
  district_id   text primary key,
  name_en       text not null,
  name_ar       text not null,
  population    integer not null,
  households    integer not null,
  overview      text not null,
  challenges    text not null,
  projects      text not null,
  is_active     boolean not null default true,
  updated_at    timestamptz not null default now()
);

insert into districts values
('D-NH', 'North Harbour', 'الميناء الشمالي', 84500, 21300,
 'North Harbour is the oldest and most populated district of Coral Bay, around the port and the Corniche Road. It is supplied from the Harbour Reservoir.',
 'Most of the network is cast-iron pipe laid in the 1970s, which corrodes and bursts under pressure. Bursts on the Corniche Road disrupt traffic as well as supply.',
 'Harbour Mains Renewal: replacing 38 km of cast-iron mains with ductile iron, budget 12.4 million, due to finish in Q3 2026. 21 km had been replaced by June 2026.',
 true, now()),
('D-SP', 'South Plains', 'السهول الجنوبية', 61200, 14800,
 'South Plains is a fast-growing residential and farming district on the southern edge of Coral Bay.',
 'Illegal connections and old mechanical meters make South Plains the district with the highest water losses. Farms draw heavily in the summer months.',
 'Smart Meter Rollout: installing 18,000 smart meters by December 2026, so that losses and illegal connections show up within days. 6,500 meters were installed by June 2026.',
 true, now()),
('D-ER', 'East Ridge', 'المرتفعات الشرقية', 47800, 11900,
 'East Ridge lies on the hills east of the city centre. Homes on the hilltop are the highest points of the network.',
 'Hilltop areas suffer from low water pressure, especially in the evening peak, because they sit above the reservoir level.',
 'Ridge Booster Station: a new pumping station that raises pressure for the hilltop areas, opened in March 2026 at a cost of 3.1 million.',
 true, now()),
('D-WM', 'West Marina', 'المارينا الغربية', 39300, 10200,
 'West Marina is the coastal district with the marina, hotels and the main pumping station of the western network.',
 'Saltwater intrusion threatens the coastal wells, and the West Marina pumping station depends on a single power feed.',
 'Marina Coastal Wall: a barrier that protects the coastal wells from seawater, due in 2027. A second power feed for the pumping station is planned for 2026.',
 true, now()),
('D-OT', 'Old Town', 'البلدة القديمة', 12900, 3100,
 'Old Town was merged into North Harbour in 2025 and is no longer reported as a separate district.',
 'None reported since the merger.',
 'None.',
 false, now());

-- ---------------------------------------------------------------- incidents
create table incidents (
  incident_id        text primary key,
  title              text not null,
  district           text not null,
  category           text not null,
  severity           text not null,
  started_at         timestamptz not null,
  restored_at        timestamptz,
  customers_affected integer,
  summary            text not null,
  root_cause         text not null,
  resolution         text not null,
  is_public          boolean not null default true
);

insert into incidents values
('INC-1042', 'Main burst on Corniche Road', 'North Harbour', 'Burst main', 'High', '2026-02-11 06:20+03', '2026-02-11 13:20+03', 3200,
 'A 400 mm water main burst on the Corniche Road early in the morning. Supply was cut for 7 hours to 3,200 customers and the road was closed in both directions.',
 'A corroded cast-iron main laid in 1974.',
 'The damaged 60 m section was replaced with ductile iron pipe. The section is now part of the Harbour Mains Renewal.', true),
('INC-1057', 'Low pressure on East Ridge hilltop', 'East Ridge', 'Low pressure', 'Medium', '2026-03-02 18:00+03', '2026-03-20 10:00+03', 2100,
 'For 18 days, 2,100 customers on the East Ridge hilltop had very low pressure in the evenings.',
 'The old booster pump failed and could not be repaired.',
 'Supply was restored when the new Ridge Booster Station opened on 20 March 2026.', true),
('INC-1063', 'Discoloured water in South Plains', 'South Plains', 'Water quality', 'Low', '2026-04-15 09:00+03', '2026-04-16 17:00+03', 640,
 '640 customers reported brown, discoloured water after maintenance work on a valve.',
 'Sediment stirred up in the pipes when a valve was opened too quickly.',
 'The network was flushed for 32 hours; lab tests confirmed the water was safe to drink.', true),
('INC-1071', 'Power failure at West Marina pumping station', 'West Marina', 'Power failure', 'High', '2026-05-08 14:10+03', '2026-05-08 16:10+03', 5400,
 'The West Marina pumping station lost power for 2 hours and 5,400 customers had no water.',
 'The single power feed failed and the backup generator did not start.',
 'The backup generator was replaced, and a second power feed is planned for 2026.', true),
('INC-1078', 'Smart meter data outage in South Plains', 'South Plains', 'Metering', 'Low', '2026-06-03 00:00+03', '2026-06-09 12:00+03', 1450,
 'Readings from 1,450 new smart meters did not reach the billing system for 6 days.',
 'An expired security certificate on the meter data gateway.',
 'The certificate was renewed; bills for the affected customers were estimated and later corrected.', true),
('INC-1080', 'Chlorine dosing pump calibration', 'East Ridge', 'Treatment', 'Low', '2026-06-12 08:00+03', '2026-06-12 11:00+03', 0,
 'Internal: the chlorine dosing pump at the Ridge treatment unit was recalibrated. No customer impact.',
 'Scheduled calibration drift.',
 'Recalibrated by the treatment team.', false);

-- ---------------------------------------------------------------- read-only login for the platform
create role coralbay_ro login password :'ro_pass';
grant connect on database coralbay to coralbay_ro;
grant usage on schema public to coralbay_ro;
grant select on kpi_values, districts, incidents to coralbay_ro;
