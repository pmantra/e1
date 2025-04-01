-- migrate:up
DROP TRIGGER IF EXISTS verify_member
ON eligibility.member_versioned;

DROP FUNCTION IF EXISTS eligibility.pre_verify_member();

-- migrate:down
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
