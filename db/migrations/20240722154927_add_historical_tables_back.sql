-- migrate:up

-- eligibility.member_versioned_historical definition

CREATE TABLE eligibility.member_versioned_historical (
	id bigserial NOT NULL,
	organization_id int8 NOT NULL,
	file_id int8 NULL,
	first_name eligibility."iwstext" DEFAULT ''::text NOT NULL,
	last_name eligibility."iwstext" DEFAULT ''::text NOT NULL,
	email eligibility."iwstext" DEFAULT ''::text NOT NULL,
	unique_corp_id eligibility."ilztext" DEFAULT ''::text NOT NULL,
	dependent_id eligibility."citext" DEFAULT ''::text NOT NULL,
	date_of_birth date NOT NULL,
	work_state eligibility."iwstext" NULL,
	record jsonb NULL,
	created_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	updated_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	effective_range daterange DEFAULT eligibility.default_range() NOT NULL,
	policies jsonb NULL,
	do_not_contact eligibility."iwstext" NULL,
	gender_code eligibility."iwstext" NULL,
	employer_assigned_id eligibility."iwstext" NULL,
	work_country eligibility."citext" DEFAULT NULL::text NULL,
	custom_attributes jsonb NULL,
	pre_verified bool DEFAULT false NOT NULL,
	hash_value eligibility."iwstext" NULL,
	hash_version int4 NULL,
	CONSTRAINT member_versioned_historical_hash_value_hash_version_key UNIQUE (hash_value, hash_version),
	CONSTRAINT member_versioned_historical_pkey PRIMARY KEY (id)
);

create trigger set_member_versioned_historical_timestamp before
update
    on
    eligibility.member_versioned_historical for each row execute function eligibility.trigger_set_timestamp();


-- eligibility.member_address_versioned_historical definition

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
	created_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	updated_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
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
