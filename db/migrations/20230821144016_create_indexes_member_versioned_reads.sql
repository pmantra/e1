-- migrate:up

CREATE INDEX idx_member_versioned_tertiary_verification ON eligibility.member_versioned USING btree (date_of_birth, ltrim(lower((unique_corp_id)::text)) text_pattern_ops);

CREATE INDEX idx_member_versioned_client_specific_verification ON eligibility.member_versioned USING btree (date_of_birth, organization_id, ltrim(lower((unique_corp_id)::text)) text_pattern_ops);

CREATE INDEX idx_member_versioned_identity ON eligibility.member_versioned USING btree (organization_id, ltrim(lower((dependent_id)::text)), ltrim(lower((unique_corp_id)::text)) text_pattern_ops);

CREATE INDEX idx_member_versioned_name_dob ON eligibility.member_versioned USING gin (first_name, last_name, date_of_birth);

-- migrate:down

DROP INDEX IF EXISTS eligibility.idx_member_versioned_tertiary_verification;

DROP INDEX IF EXISTS eligibility.idx_member_versioned_client_specific_verification;

DROP INDEX IF EXISTS eligibility.idx_member_versioned_identity;

DROP INDEX IF EXISTS eligibility.idx_member_versioned_name_dob;