"""PostgreSQL data source: export tables from a customer database into KAP's
indicator CSV format, upload them to S3 and let KAP's own connector and
indicator pipeline load them.

  secrets.py    Fernet encryption of stored source passwords
  kapcsv.py     KAP indicator CSV format: headers, writer, periods, ids, custom-query columns
  source.py     read-only access to a source database (test, preview, fetch, stream)
  presets.py    built-in SQL + mapping for the scai_indicators schema
  build.py      produce the three files (export) or a sample of them (preview)
  s3.py         uploads (boto3 in a thread, path addressing)
  provision.py  KAP connector / pipeline / collection for a dataset (idempotent)
  store.py      pg_source / pg_dataset / pg_export_run
  export.py     queue handlers pg_export / pg_export_watch and the schedule tick
  api.py        REST routes /api/t/{tid}/pg-sources, pg-datasets, pg-exports
"""
