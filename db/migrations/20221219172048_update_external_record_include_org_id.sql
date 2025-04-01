-- migrate:up
DROP TYPE eligibility.external_record;
CREATE TYPE eligibility.external_record AS (
	first_name text,
	last_name text,
	email text,
	unique_corp_id eligibility.ilztext,
	dependent_id eligibility.citext,
	date_of_birth date,
	work_state text,
	record jsonb,
	effective_range daterange,
	"source" text,
	external_id text,
	external_name text,
	received_ts int8,
	do_not_contact eligibility.iwstext,
	gender_code eligibility.iwstext,
	employer_assigned_id eligibility.iwstext,
    organization_id int8)
;

-- migrate:down

DROP TYPE eligibility.external_record;
CREATE TYPE eligibility.external_record AS (
	first_name text,
	last_name text,
	email text,
	unique_corp_id eligibility.ilztext,
	dependent_id eligibility.citext,
	date_of_birth date,
	work_state text,
	record jsonb,
	effective_range daterange,
	"source" text,
	external_id text,
	external_name text,
	received_ts int8,
	do_not_contact eligibility.iwstext,
	gender_code eligibility.iwstext,
	employer_assigned_id eligibility.iwstext);