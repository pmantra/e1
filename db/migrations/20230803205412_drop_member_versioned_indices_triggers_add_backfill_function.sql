-- migrate:up

DROP INDEX IF EXISTS eligibility.idx_member_versioned_effective_range;
DROP INDEX IF EXISTS eligibility.idx_member_versioned_email;
DROP INDEX IF EXISTS eligibility.idx_member_versioned_file_id;
DROP INDEX IF EXISTS eligibility.idx_member_versioned_id_do_not_contact;
DROP INDEX IF EXISTS eligibility.idx_member_versioned_name;
DROP INDEX IF EXISTS eligibility.idx_member_versioned_organization_id;
DROP INDEX IF EXISTS eligibility.idx_member_versioned_primary_verification;
DROP INDEX IF EXISTS eligibility.idx_member_versioned_secondary_verification;
DROP INDEX IF EXISTS eligibility.idx_member_versioned_unique_corp_id;

DROP TRIGGER IF EXISTS set_member_versioned_timestamp ON eligibility.member_versioned;

ALTER TABLE eligibility."member_versioned" DROP CONSTRAINT member_file_id_fkey;
ALTER TABLE eligibility."member_versioned" DROP CONSTRAINT member_organization_id_fkey;


DROP INDEX IF EXISTS eligibility.idx_member_versioned_address_country;
DROP INDEX IF EXISTS eligibility.idx_member_versioned_address_member_id;
DROP INDEX IF EXISTS eligibility.idx_member_versioned_address_updated_at;
DROP INDEX IF EXISTS eligibility.uidx_member_versioned_member_address_state_zip;


DROP TRIGGER IF EXISTS set_address_timestamp ON eligibility.member_address_versioned;

ALTER TABLE eligibility.member_address_versioned DROP CONSTRAINT fk_member_address_versioned_member;


CREATE OR REPLACE FUNCTION eligibility.batch_migrate_member(start_id integer, end_id integer) RETURNS void AS $$
BEGIN
    INSERT INTO eligibility.member_versioned(id, organization_id, file_id, first_name, last_name, email, unique_corp_id, dependent_id, date_of_birth, work_state, record, created_at, updated_at, effective_range, do_not_contact, gender_code, employer_assigned_id)
    SELECT id, organization_id, file_id, first_name, last_name, email, unique_corp_id, dependent_id, date_of_birth, work_state, record, created_at, updated_at, effective_range, do_not_contact, gender_code, employer_assigned_id
    FROM eligibility.member
    WHERE id >= start_id AND id < end_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION eligibility.batch_migrate_member_address(start_id integer, end_id integer) RETURNS void AS $$
BEGIN
    INSERT INTO eligibility.member_address_versioned
    SELECT *
    FROM eligibility.member_address
    WHERE id >= start_id AND id < end_id;
END;
$$ LANGUAGE plpgsql;

-- migrate:down

CREATE INDEX idx_member_versioned_effective_range ON eligibility.member_versioned USING gist (effective_range);
CREATE INDEX idx_member_versioned_email ON eligibility.member_versioned USING btree (btrim(lower((email)::text)) text_pattern_ops);
CREATE INDEX idx_member_versioned_file_id ON eligibility.member_versioned USING btree (file_id);
CREATE INDEX idx_member_versioned_id_do_not_contact ON eligibility.member_versioned USING btree (id, do_not_contact);
CREATE INDEX idx_member_versioned_name ON eligibility.member_versioned USING gin (first_name, last_name);
CREATE INDEX idx_member_versioned_organization_id ON eligibility.member_versioned USING btree (organization_id);
CREATE INDEX idx_member_versioned_primary_verification ON eligibility.member_versioned USING btree (date_of_birth, btrim(lower((email)::text)) text_pattern_ops);
CREATE INDEX idx_member_versioned_secondary_verification ON eligibility.member_versioned USING btree (date_of_birth, btrim(lower((first_name)::text)), btrim(lower((last_name)::text)), btrim(lower((work_state)::text)) text_pattern_ops);
CREATE INDEX idx_member_versioned_unique_corp_id ON eligibility.member_versioned USING btree (ltrim(lower((unique_corp_id)::text), '0'::text) text_pattern_ops);

create trigger set_member_versioned_timestamp before
update
    on
    eligibility.member_versioned for each row execute function eligibility.trigger_set_timestamp();

ALTER TABLE eligibility."member_versioned" ADD CONSTRAINT member_file_id_fkey FOREIGN KEY (file_id) REFERENCES eligibility.file(id);
ALTER TABLE eligibility."member_versioned" ADD CONSTRAINT member_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES eligibility."configuration"(organization_id) ON DELETE CASCADE;


CREATE INDEX idx_member_versioned_address_country ON eligibility.member_address_versioned USING btree (country_code);
CREATE INDEX idx_member_versioned_address_member_id ON eligibility.member_address_versioned USING btree (member_id);
CREATE INDEX idx_member_versioned_address_updated_at ON eligibility.member_address_versioned USING btree (updated_at);
CREATE UNIQUE INDEX uidx_member_versioned_member_address_state_zip ON eligibility.member_address_versioned USING btree (member_id, address_1, city, state, postal_code, country_code);

create trigger set_address_timestamp before
update
    on
    eligibility.member_address_versioned for each row execute function eligibility.trigger_set_timestamp();

ALTER TABLE eligibility.member_address_versioned ADD CONSTRAINT fk_member_address_versioned_member FOREIGN KEY (member_id) REFERENCES eligibility."member_versioned"(id) ON DELETE CASCADE;

DROP FUNCTION IF EXISTS eligibility.batch_migrate_member;

DROP FUNCTION IF EXISTS eligibility.batch_migrate_member_address;