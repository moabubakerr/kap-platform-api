-- PostgreSQL data source: fourth preset `records` (each row of one table/view → a Word "record card"
-- in a KAP document collection, one folder per record).

alter table pg_dataset add column if not exists records jsonb;  -- records: {table, id_column, title, fields, sections, filters, order_by, max_records, language}

alter table pg_dataset drop constraint if exists pg_dataset_preset_check;
alter table pg_dataset add constraint pg_dataset_preset_check
  check (preset in ('scai_indicators', 'custom', 'mapped', 'records'));
alter table pg_dataset drop constraint if exists pg_dataset_records_check;
alter table pg_dataset add constraint pg_dataset_records_check
  check (preset <> 'records' or records is not null);
