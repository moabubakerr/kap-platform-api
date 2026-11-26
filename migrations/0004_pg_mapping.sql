-- PostgreSQL data source: third preset `mapped` (one table/view + a column mapping; the SQL is generated).

alter table pg_dataset add column mapping jsonb;  -- mapped: {table, series_key, id_prefix, period, frequency, value, …}

alter table pg_dataset drop constraint if exists pg_dataset_preset_check;
alter table pg_dataset add constraint pg_dataset_preset_check
  check (preset in ('scai_indicators', 'custom', 'mapped'));
alter table pg_dataset add constraint pg_dataset_mapping_check
  check (preset <> 'mapped' or mapping is not null);
