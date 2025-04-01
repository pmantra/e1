-- migrate:up

CREATE TABLE eligibility."member_versioned" (
    id bigserial NOT NULL,
    organization_id int8 NOT NULL,
    file_id int8 NULL,
    first_name eligibility.iwstext NOT NULL DEFAULT ''::text,
    last_name eligibility.iwstext NOT NULL DEFAULT ''::text,
    email eligibility.iwstext NOT NULL DEFAULT ''::text,
    unique_corp_id eligibility.ilztext NOT NULL DEFAULT ''::text,
    dependent_id eligibility.citext NOT NULL DEFAULT ''::text,
    date_of_birth date NOT NULL,
    work_state eligibility.iwstext NULL,
    record jsonb NULL,
    created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
    effective_range daterange NOT NULL DEFAULT eligibility.default_range(),
    policies jsonb NULL,
    do_not_contact eligibility.iwstext NULL,
    gender_code eligibility.iwstext NULL,
    employer_assigned_id eligibility.iwstext NULL,
    CONSTRAINT member_versioned_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_member_versioned_effective_range ON eligibility.member_versioned USING gist (effective_range);
CREATE INDEX idx_member_versioned_email ON eligibility.member_versioned USING btree (btrim(lower((email)::text)) text_pattern_ops);
CREATE INDEX idx_member_versioned_file_id ON eligibility.member_versioned USING btree (file_id);
CREATE INDEX idx_member_versioned_id_do_not_contact ON eligibility.member_versioned USING btree (id, do_not_contact);
CREATE INDEX idx_member_versioned_name ON eligibility.member_versioned USING gin (first_name, last_name);
CREATE INDEX idx_member_versioned_organization_id ON eligibility.member_versioned USING btree (organization_id);
CREATE INDEX idx_member_versioned_primary_verification ON eligibility.member_versioned USING btree (date_of_birth, btrim(lower((email)::text)) text_pattern_ops);
CREATE INDEX idx_member_versioned_secondary_verification ON eligibility.member_versioned USING btree (date_of_birth, btrim(lower((first_name)::text)), btrim(lower((last_name)::text)), btrim(lower((work_state)::text)) text_pattern_ops);
CREATE INDEX idx_member_versioned_unique_corp_id ON eligibility.member_versioned USING btree (ltrim(lower((unique_corp_id)::text), '0'::text) text_pattern_ops);


-- Table Triggers

create trigger set_member_versioned_timestamp before
update
    on
    eligibility.member_versioned for each row execute function eligibility.trigger_set_timestamp();


-- eligibility."member_versioned" foreign keys

ALTER TABLE eligibility."member_versioned" ADD CONSTRAINT member_file_id_fkey FOREIGN KEY (file_id) REFERENCES eligibility.file(id);
ALTER TABLE eligibility."member_versioned" ADD CONSTRAINT member_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES eligibility."configuration"(organization_id) ON DELETE CASCADE;




-- member address versioned table
CREATE TABLE eligibility.member_address_versioned (
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
	CONSTRAINT member_address_versioned_member_id_key UNIQUE (member_id),
	CONSTRAINT member_address_versioned_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_member_versioned_address_country ON eligibility.member_address_versioned USING btree (country_code);
CREATE INDEX idx_member_versioned_address_member_id ON eligibility.member_address_versioned USING btree (member_id);
CREATE INDEX idx_member_versioned_address_updated_at ON eligibility.member_address_versioned USING btree (updated_at);
CREATE UNIQUE INDEX uidx_member_versioned_member_address_state_zip ON eligibility.member_address_versioned USING btree (member_id, address_1, city, state, postal_code, country_code);

-- Table Triggers

create trigger set_address_timestamp before
update
    on
    eligibility.member_address_versioned for each row execute function eligibility.trigger_set_timestamp();


-- eligibility.member_address_versioned foreign keys

ALTER TABLE eligibility.member_address_versioned ADD CONSTRAINT fk_member_address_versioned_member FOREIGN KEY (member_id) REFERENCES eligibility."member_versioned"(id) ON DELETE CASCADE;





--- Verification tables

CREATE TABLE eligibility.verification (
    id bigserial NOT NULL,
    user_id bigserial NOT NULL,
    organization_id int8 NOT NULL,
    unique_corp_id eligibility.ilztext NOT NULL DEFAULT ''::text,
    dependent_id eligibility.citext NOT NULL DEFAULT ''::text,
    first_name eligibility.iwstext NOT NULL DEFAULT ''::text,
    last_name eligibility.iwstext NOT NULL DEFAULT ''::text,
    email eligibility.iwstext NOT NULL DEFAULT ''::text,
    date_of_birth date NOT NULL,
    work_state eligibility.iwstext NULL,
    verification_type eligibility.iwstext NOT NULL DEFAULT ''::text,
    created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
    deactivated_at timestamptz NULL,
    CONSTRAINT verification_pkey PRIMARY KEY (id)
);

-- Table Triggers

create trigger set_verification_timestamp before
update
    on
    eligibility.verification for each row execute function eligibility.trigger_set_timestamp();

ALTER TABLE eligibility."verification" ADD CONSTRAINT verification_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES eligibility."configuration"(organization_id) ON DELETE CASCADE;



CREATE TABLE eligibility.verification_attempt (
    id bigserial NOT NULL,
    --- We do not necessarily get an organization for failed verification attempts, so no longer a required field
    organization_id int8 NULL,
    unique_corp_id eligibility.ilztext NULL,
    dependent_id eligibility.citext NULL,
    first_name eligibility.iwstext NULL,
    last_name eligibility.iwstext NULL,
    email eligibility.iwstext NULL,
    date_of_birth date NULL,
    work_state eligibility.iwstext NULL,
    verification_type eligibility.iwstext NOT NULL DEFAULT ''::text,
    policy_used jsonb NULL, -- policy we attempted to verify using- may be null
    successful_verification boolean,
    verification_id BIGINT NULL,
    created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT failed_verification_pkey PRIMARY KEY (id)
);

-- Table Triggers

create trigger set_verification_attempt_timestamp before
update
    on
    eligibility.verification_attempt for each row execute function eligibility.trigger_set_timestamp();

ALTER TABLE eligibility."verification_attempt" ADD CONSTRAINT verification_attempt_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES eligibility."configuration"(organization_id) ON DELETE CASCADE;


--- Table used to tie verification tables to the member table

CREATE TABLE eligibility.member_verification (
    id bigserial NOT NULL,
    member_id BIGINT NULL, --- may be null in the case of failed verification where we weren't able to tie a member records
    verification_id BIGINT NULL, --- may be null in the case of failed verification attempt
    verification_attempt_id BIGINT NULL, --- ties to the verification attempt for a user
    created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE eligibility."member_verification" ADD CONSTRAINT member_verification_member_id_fkey FOREIGN KEY (member_id) REFERENCES eligibility."member_versioned"(id) ON DELETE CASCADE;
ALTER TABLE eligibility."member_verification" ADD CONSTRAINT verification_id_fkey FOREIGN KEY (verification_id) REFERENCES eligibility."verification"(id) ON DELETE CASCADE;
ALTER TABLE eligibility."member_verification" ADD CONSTRAINT verification_attempt_id_fkey FOREIGN KEY (verification_attempt_id) REFERENCES eligibility."verification_attempt"(id) ON DELETE CASCADE;



create trigger set_member_verification_timestamp before
update
    on
    eligibility.member_verification for each row execute function eligibility.trigger_set_timestamp();


-- migrate:down

DROP TABLE eligibility.member_address_versioned;

DROP TABLE eligibility.member_verification;

DROP TABLE eligibility.member_versioned;

DROP TABLE eligibility.verification;

DROP TABLE eligibility.verification_attempt;
