-- Name of the account that submitted the job (KAP returns only its UUID).
alter table kap_job add column created_by_name text;
