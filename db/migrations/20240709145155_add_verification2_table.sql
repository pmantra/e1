-- migrate:up
CREATE TABLE IF NOT EXISTS eligibility.verification_2 (
	id bigserial NOT NULL,
	user_id int8 NOT NULL,
	organization_id int8 NOT NULL,
	unique_corp_id eligibility.ilztext DEFAULT ''::text NOT NULL,
	dependent_id eligibility.citext DEFAULT ''::text NOT NULL,
	first_name eligibility.iwstext DEFAULT ''::text NOT NULL,
	last_name eligibility.iwstext DEFAULT ''::text NOT NULL,
	email eligibility.iwstext DEFAULT ''::text NOT NULL,
	date_of_birth date NULL,
	work_state eligibility.iwstext NULL,
	verification_type eligibility.iwstext DEFAULT ''::text NOT NULL,
	created_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	updated_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	deactivated_at timestamptz NULL,
	verified_at timestamptz NULL,
	additional_fields jsonb NULL,
	verification_session uuid NULL,
    member_id bigint NULL,
    member_version int8 NULL,
	CONSTRAINT verification_2_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_verification_2_deactivated_at ON eligibility.verification_2 USING btree (deactivated_at);
CREATE INDEX idx_verification_2_organization_id ON eligibility.verification_2 USING btree (organization_id);
CREATE INDEX idx_verification_2_user_id ON eligibility.verification_2 USING btree (user_id);
CREATE INDEX idx_verification_2_member_id ON eligibility.verification_2 USING btree (member_id);

create trigger set_verification_2_timestamp before
update
    on
    eligibility.verification_2 for each row execute function eligibility.trigger_set_timestamp();


ALTER TABLE eligibility.verification_2 ADD CONSTRAINT verification_2_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES eligibility."configuration"(organization_id) ON DELETE CASCADE;
ALTER TABLE eligibility.verification_2 ADD CONSTRAINT verification_2_member_id_fkey FOREIGN KEY (member_id) REFERENCES eligibility."member_2"(id);

-- migrate:down
DROP TABLE IF EXISTS eligibility.verification_2