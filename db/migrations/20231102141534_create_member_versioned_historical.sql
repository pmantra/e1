-- migrate:up


CREATE TABLE eligibility.member_versioned_historical (
	id bigserial NOT NULL,
	organization_id int8 NOT NULL,
	file_id int8 NULL,
	first_name eligibility."iwstext" NOT NULL DEFAULT ''::text,
	last_name eligibility."iwstext" NOT NULL DEFAULT ''::text,
	email eligibility."iwstext" NOT NULL DEFAULT ''::text,
	unique_corp_id eligibility."ilztext" NOT NULL DEFAULT ''::text,
	dependent_id eligibility."citext" NOT NULL DEFAULT ''::text,
	date_of_birth date NOT NULL,
	work_state eligibility."iwstext" NULL,
	record jsonb NULL,
	created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
	effective_range daterange NOT NULL DEFAULT eligibility.default_range(),
	policies jsonb NULL,
	do_not_contact eligibility."iwstext" NULL,
	gender_code eligibility."iwstext" NULL,
	employer_assigned_id eligibility."iwstext" NULL,
	work_country eligibility."citext" NULL DEFAULT NULL::text,
	custom_attributes jsonb NULL,
	pre_verified bool NOT NULL DEFAULT false,
	CONSTRAINT member_versioned_historical_pkey PRIMARY KEY (id)
);

-- Table Triggers

create trigger set_member_versioned_historical_timestamp before
update
    on
    eligibility.member_versioned_historical for each row execute function eligibility.trigger_set_timestamp();



CREATE TABLE eligibility.member_address_versioned_historical (
	id bigserial NOT NULL,
	member_id int8 NOT NULL,
	address_1 text NULL,
	address_2 text NULL,
	city text NULL,
	state text NULL,
	postal_code text NULL,
	postal_code_suffix text NULL,
	country_code text NULL,
	address_type text NULL,
	created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT member_address_versioned_historical_member_id_key UNIQUE (member_id),
	CONSTRAINT member_address_versioned_historical_pkey PRIMARY KEY (id)
);

create trigger set_address_historical_timestamp before
update
    on
    eligibility.member_address_versioned_historical for each row execute function eligibility.trigger_set_timestamp();



ALTER TABLE eligibility.member_address_versioned_historical ADD CONSTRAINT fk_member_address_versioned_historical_member FOREIGN KEY (member_id) REFERENCES eligibility.member_versioned_historical(id) ON DELETE CASCADE;




-- migrate:down

DROP TABLE IF EXISTS eligibility.member_address_versioned_historical;

DROP TABLE IF EXISTS eligibility.member_versioned_historical;



