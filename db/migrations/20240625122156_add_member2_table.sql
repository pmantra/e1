-- migrate:up
CREATE TABLE IF NOT EXISTS eligibility.member_2 (
	id bigint NOT NULL,
	version int8 not null,
	organization_id int8 NOT NULL,
	first_name eligibility.iwstext DEFAULT ''::text NOT NULL,
	last_name eligibility.iwstext DEFAULT ''::text NOT NULL,
	email eligibility.iwstext DEFAULT ''::text NOT NULL,
	unique_corp_id eligibility.ilztext DEFAULT ''::text NOT NULL,
	dependent_id eligibility.citext DEFAULT ''::text NOT NULL,
	date_of_birth date NOT NULL,
	work_state eligibility.iwstext NULL,
    work_country eligibility.citext DEFAULT NULL::text NULL,
	record jsonb NULL,
	custom_attributes jsonb NULL,
	effective_range daterange DEFAULT eligibility.default_range() NOT NULL,
	do_not_contact eligibility.iwstext NULL,
	gender_code eligibility.iwstext NULL,
	employer_assigned_id eligibility.iwstext NULL,
	created_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	updated_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT member_2_pkey PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_member_2_client_specific_verification ON eligibility.member_2 USING btree (date_of_birth, organization_id, ltrim(lower((unique_corp_id)::text)) text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_member_2_effective_range ON eligibility.member_2 USING gist (effective_range);
CREATE INDEX IF NOT EXISTS idx_member_2_email ON eligibility.member_2 USING btree (btrim(lower((email)::text)) text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_member_2_employer_assigned_id ON eligibility.member_2 USING btree (btrim(lower((employer_assigned_id)::text)) text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_member_2_identity ON eligibility.member_2 USING btree (organization_id, ltrim(lower((dependent_id)::text)), ltrim(lower((unique_corp_id)::text)) text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_member_2_name ON eligibility.member_2 USING gin (first_name, last_name);
CREATE INDEX IF NOT EXISTS idx_member_2_name_dob ON eligibility.member_2 USING gin (first_name, last_name, date_of_birth);
CREATE INDEX IF NOT EXISTS idx_member_2_primary_verification ON eligibility.member_2 USING btree (date_of_birth, btrim(lower((email)::text)) text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_member_2_secondary_verification ON eligibility.member_2 USING btree (date_of_birth, btrim(lower((first_name)::text)), btrim(lower((last_name)::text)), btrim(lower((work_state)::text)) text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_member_2_tertiary_verification ON eligibility.member_2 USING btree (date_of_birth, ltrim(lower((unique_corp_id)::text)) text_pattern_ops);
CREATE INDEX IF NOT EXISTS idx_member_2_unique_corp_id ON eligibility.member_2 USING btree (ltrim(lower((unique_corp_id)::text), '0'::text) text_pattern_ops);
CREATE INDEX IF NOT EXISTS trgm_idx_member_2_dependent_id ON eligibility.member_2 USING gin (dependent_id gin_trgm_ops);
CREATE INDEX IF NOT EXISTS trgm_idx_member_2_email ON eligibility.member_2 USING gin (email gin_trgm_ops);
CREATE INDEX IF NOT EXISTS trgm_idx_member_2_first_name ON eligibility.member_2 USING gin (first_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS trgm_idx_member_2_last_name ON eligibility.member_2 USING gin (last_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS trgm_idx_member_2_unique_corp_id ON eligibility.member_2 USING gin (unique_corp_id gin_trgm_ops);

ALTER TABLE eligibility.member_2 ADD CONSTRAINT member_2_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES eligibility."configuration"(organization_id) ON DELETE CASCADE;

-- migrate:down
DROP TABLE IF EXISTS eligibility.member_2