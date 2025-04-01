-- migrate:up
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


CREATE OR REPLACE FUNCTION eligibility.pre_verify_member()
   RETURNS TRIGGER
   LANGUAGE PLPGSQL
AS $$
BEGIN
   -- check the existing members in this org to see if there is an existing record
   -- that matches on first_name, last_name, work_state, email, date_of_birth, unique_corp_id
    WITH matched_verifications AS (
        SELECT MAX(v.id) as latest_verification_id, -- Take the latest verification
            v.user_id as user_id,
            v.first_name as first_name,
            v.last_name as last_name,
            v.email as email,
            v.date_of_birth as date_of_birth,
            v.unique_corp_id as unique_corp_id,
            v.work_state as work_state,
            v.organization_id as organization_id
        FROM eligibility.member_versioned m
                LEFT JOIN
            eligibility.member_verification mv on m.id = mv.member_id
                LEFT JOIN
            eligibility.verification v on mv.verification_id = v.id
        WHERE
            -- make sure we look within the org
            m.organization_id = NEW.organization_id
            -- verification exists and is active
            AND v.id IS NOT NULL
            AND v.deactivated_at IS NULL
            -- verification is primary or alternate
            AND v.verification_type in ('PRIMARY', 'ALTERNATE')
            -- verification fields
            AND LOWER(m.first_name) = LOWER(NEW.first_name)
            AND LOWER(m.last_name) = LOWER(NEW.last_name)
            AND m.date_of_birth = NEW.date_of_birth
            AND LOWER(m.unique_corp_id) = LOWER(NEW.unique_corp_id)
            AND LOWER(m.email) = LOWER(NEW.email)
            -- because work_state could be NULL, empty string, or non-empty string
            -- we want to match on both cases
            AND (
                m.work_state = NEW.work_state
                OR (
                   m.work_state IS NULL
                   AND NEW.work_state IS NULL
                )
            )
            -- not the same record as the one we just inserted
            AND m.id <> NEW.id
        GROUP BY user_id, v.first_name, v.last_name, v.email, v.date_of_birth, v.unique_corp_id, v.work_state, v.organization_id
    ), verification_attempts as (
        INSERT INTO eligibility.verification_attempt (organization_id, unique_corp_id, first_name, last_name, email, date_of_birth, work_state, verification_type, successful_verification, verification_id)
        SELECT
            mv.organization_id as organization_id,
            mv.unique_corp_id as unique_corp_id,
            mv.first_name as first_name,
            mv.last_name as last_name,
            mv.email as email,
            mv.date_of_birth as date_of_birth,
            mv.work_state as work_state,
            'PRE_VERIFY' as verification_type,
            TRUE as successful_verification,
            mv.latest_verification_id as verification_id
        FROM matched_verifications mv
        RETURNING *
    )
    -- for each of the verifications in matched_verifications, create a member_verification
    INSERT INTO eligibility.member_verification (member_id, verification_id, verification_attempt_id)
    SELECT
        NEW.id as member_id,
        latest_verification_id as verification_id,
        va.id as verification_attempt_id
    FROM matched_verifications mv
    INNER JOIN verification_attempts va
    ON mv.latest_verification_id = va.verification_id;
    RETURN NULL;
END;
$$;

CREATE TRIGGER verify_member AFTER INSERT
    ON eligibility.member_versioned
    FOR EACH ROW
    EXECUTE FUNCTION eligibility.pre_verify_member();

SELECT SETVAL(
    (SELECT PG_GET_SERIAL_SEQUENCE('eligibility.member_versioned', 'id')),
    (SELECT (MAX(id) + 1) FROM eligibility.member_versioned),
    FALSE
);

SELECT SETVAL(
    (SELECT PG_GET_SERIAL_SEQUENCE('eligibility.member_address_versioned', 'id')),
    (SELECT (MAX(id) + 1) FROM eligibility.member_address_versioned),
    FALSE
);

-- migrate:down
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

DROP TRIGGER IF EXISTS verify_member
ON eligibility.member_versioned;

DROP FUNCTION IF EXISTS eligibility.pre_verify_member();