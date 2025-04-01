
-- Mutations pertaining to the `eligibility.verification` table.


-- name: persist<!
-- Create or Update a verification record for an user.
INSERT INTO eligibility.verification(
    user_id,
    organization_id,
    unique_corp_id,
    dependent_id,
    first_name,
    last_name,
    email,
    date_of_birth,
    work_state,
    verification_type,
    deactivated_at,
    verified_at,
    additional_fields,
    verification_session,
    verification_2_id
)
VALUES (
    :user_id,
    :organization_id,
    :unique_corp_id,
    :dependent_id,
    :first_name,
    :last_name,
    :email,
    :date_of_birth,
    :work_state,
    :verification_type,
    :deactivated_at,
    :verified_at,
    :additional_fields::jsonb,
    :verification_session,
    :verification_2_id
)
RETURNING *;

-- name: bulk_persist
-- Create or Update a series of verification records.
WITH verification_records AS (
    SELECT (unnest(:records::eligibility.verification[])::eligibility.verification).*
)
INSERT INTO eligibility.verification
(
    user_id,
    organization_id,
    unique_corp_id,
    dependent_id,
    first_name,
    last_name,
    email,
    date_of_birth,
    work_state,
    verification_type,
    deactivated_at,
    verified_at,
    additional_fields,
    verification_session,
    verification_2_id
)
SELECT
    vr.user_id,
    vr.organization_id,
    vr.unique_corp_id,
    vr.dependent_id,
    vr.first_name,
    vr.last_name,
    vr.email,
    vr.date_of_birth,
    vr.work_state,
    vr.verification_type,
    vr.deactivated_at,
    vr.verified_at,
    vr.additional_fields::jsonb,
    vr.verification_session,
    vr.verification_2_id
FROM verification_records vr
RETURNING *;

-- name: delete<!
-- Delete a verification record.
DELETE FROM eligibility.verification
WHERE id = :id
RETURNING *;

-- name: bulk_delete!
-- Delete multiple verification records.
DELETE FROM eligibility.verification
WHERE id = any(:ids);

-- name: delete_all_for_org!
-- Delete all verification records for a given organization.
DELETE FROM eligibility.verification
WHERE organization_id = :organization_id;

-- name: batch_pre_verify_records_by_org$
-- Batch pre-verify member records by ID
WITH params AS (
    SELECT :organization_id::int AS organization_id,
           :batch_size::int AS batch_size
), unprocessed_members AS (
    SELECT
        m.id as id,
        m.first_name as first_name,
        m.last_name as last_name,
        m.date_of_birth as date_of_birth,
        m.email as email,
        m.work_state as work_state,
        m.organization_id as organization_id,
        m.unique_corp_id as unique_corp_id
    FROM eligibility.member_versioned m
    WHERE m.organization_id=(SELECT organization_id FROM params)
    AND m.pre_verified=FALSE
    AND m.effective_range @> CURRENT_DATE
    AND id NOT IN (
        SELECT member_id FROM eligibility.member_verification
    )
    LIMIT (SELECT batch_size FROM params)
), existing_member_verifications AS (
    SELECT MAX(mv.member_id) as member_id,
           v.id as verification_id,
           v.user_id
    FROM eligibility.verification v
    INNER JOIN eligibility.member_verification mv
    ON v.id = mv.verification_id
    WHERE v.organization_id=(SELECT organization_id FROM params)
    AND v.deactivated_at IS NULL
    GROUP BY v.id, v.user_id
), matched AS (
    SELECT
        um.id as member_id,
        um.first_name as first_name,
        um.last_name as last_name,
        um.date_of_birth as date_of_birth,
        um.email as email,
        um.work_state as work_state,
        um.organization_id as organization_id,
        um.unique_corp_id as unique_corp_id,
        emv.user_id as user_id,
        emv.verification_id as verification_id,
        m.id as existing_member_id
    FROM existing_member_verifications emv
    INNER JOIN eligibility.member_versioned m
    ON emv.member_id=m.id
    INNER JOIN unprocessed_members um
        ON m.organization_id=um.organization_id
        AND BTRIM(LOWER(m.first_name)) = BTRIM(LOWER(um.first_name))
        AND BTRIM(LOWER(m.last_name)) = BTRIM(LOWER(um.last_name))
        AND m.date_of_birth = um.date_of_birth
        AND LTRIM(LOWER(m.unique_corp_id), '0') = LTRIM(LOWER(um.unique_corp_id), '0')
        AND BTRIM(LOWER(m.email)) = BTRIM(LOWER(um.email))
        AND (
            m.work_state = um.work_state
            OR (
               m.work_state IS NULL
               AND um.work_state IS NULL
            )
        )
), new_member_verifications AS (
    -- for each of the verifications in matched_verifications, create a member_verification
    INSERT INTO eligibility.member_verification (member_id, verification_id)
    SELECT
        mv.member_id as member_id,
        mv.verification_id as verification_id
    FROM matched mv
), update_pre_verified AS (
    UPDATE eligibility.member_versioned
    SET pre_verified = TRUE
    WHERE id IN (SELECT id FROM unprocessed_members)
)
SELECT COUNT(*) FROM unprocessed_members;

-- name: batch_pre_verify_records_by_org_and_file$
-- Batch pre-verify member records by ID
WITH verified_members AS (
    SELECT mv.member_id as member_id,
           v.id as verification_id,
           v.user_id as user_id
    FROM eligibility.member_verification mv
    INNER JOIN eligibility.verification v ON mv.verification_id = v.id
    INNER JOIN eligibility.member_versioned m ON mv.member_id = m.id
    WHERE v.organization_id=:organization_id
      AND v.deactivated_at IS NULL
      AND m.file_id != :file_id
    ), unprocessed_members AS (
    SELECT
        m.id as id,
        m.first_name as first_name,
        m.last_name as last_name,
        m.date_of_birth as date_of_birth,
        m.email as email,
        m.work_state as work_state,
        m.organization_id as organization_id,
        m.unique_corp_id as unique_corp_id
    FROM eligibility.member_versioned m
    WHERE m.organization_id=:organization_id
    AND m.file_id=:file_id
    AND m.pre_verified=FALSE
    AND m.effective_range @> CURRENT_DATE
    AND id NOT IN (
        SELECT member_id FROM verified_members
    )
    LIMIT :batch_size
), existing_member_verifications AS (
    SELECT MAX(mv.member_id) as member_id,
           v.id as verification_id,
           v.user_id
    FROM eligibility.verification v
    INNER JOIN eligibility.member_verification mv ON v.id = mv.verification_id
    WHERE v.organization_id=:organization_id
    AND v.deactivated_at IS NULL
    GROUP BY v.id, v.user_id
), matched AS (
    SELECT
        um.id as member_id,
        um.first_name as first_name,
        um.last_name as last_name,
        um.date_of_birth as date_of_birth,
        um.email as email,
        um.work_state as work_state,
        um.organization_id as organization_id,
        um.unique_corp_id as unique_corp_id,
        emv.user_id as user_id,
        emv.verification_id as verification_id,
        m.id as existing_member_id
    FROM existing_member_verifications emv
    INNER JOIN eligibility.member_versioned m
    ON emv.member_id=m.id
    INNER JOIN unprocessed_members um
        ON m.organization_id=um.organization_id
        AND m.date_of_birth = um.date_of_birth
        AND (
            (
                -- Allow for work_state mismatch
                BTRIM(LOWER(m.first_name)) = BTRIM(LOWER(um.first_name))
                AND BTRIM(LOWER(m.last_name)) = BTRIM(LOWER(um.last_name))
                AND LTRIM(LOWER(m.unique_corp_id), '0') = LTRIM(LOWER(um.unique_corp_id), '0')
                AND BTRIM(LOWER(m.email)) = BTRIM(LOWER(um.email))
            )
            OR (
                -- Allow for email mismatch
                BTRIM(LOWER(m.first_name)) = BTRIM(LOWER(um.first_name))
                AND BTRIM(LOWER(m.last_name)) = BTRIM(LOWER(um.last_name))
                AND LTRIM(LOWER(m.unique_corp_id), '0') = LTRIM(LOWER(um.unique_corp_id), '0')
                AND (
                    m.work_state = um.work_state
                    OR (
                        m.work_state IS NULL
                        AND um.work_state IS NULL
                    )
                )
            )
            OR (
                -- Allow for unique_corp_id mismatch
                BTRIM(LOWER(m.first_name)) = BTRIM(LOWER(um.first_name))
                AND BTRIM(LOWER(m.last_name)) = BTRIM(LOWER(um.last_name))
                AND BTRIM(LOWER(m.email)) = BTRIM(LOWER(um.email))
                AND (
                    m.work_state = um.work_state
                    OR (
                        m.work_state IS NULL
                        AND um.work_state IS NULL
                    )
                )
            )
            OR (
                -- Allow for last_name mismatch
                BTRIM(LOWER(m.first_name)) = BTRIM(LOWER(um.first_name))
                AND LTRIM(LOWER(m.unique_corp_id), '0') = LTRIM(LOWER(um.unique_corp_id), '0')
                AND BTRIM(LOWER(m.email)) = BTRIM(LOWER(um.email))
                AND (
                    m.work_state = um.work_state
                    OR (
                        m.work_state IS NULL
                        AND um.work_state IS NULL
                    )
                )
            )
            OR (
                -- Allow for first_name mismatch
                BTRIM(LOWER(m.last_name)) = BTRIM(LOWER(um.last_name))
                AND LTRIM(LOWER(m.unique_corp_id), '0') = LTRIM(LOWER(um.unique_corp_id), '0')
                AND BTRIM(LOWER(m.email)) = BTRIM(LOWER(um.email))
                AND (
                    m.work_state = um.work_state
                    OR (
                        m.work_state IS NULL
                        AND um.work_state IS NULL
                    )
                )
            )
        )
), new_member_verifications AS (
    -- for each of the verifications in matched_verifications, create a member_verification
    INSERT INTO eligibility.member_verification (member_id, verification_id)
    SELECT
        mv.member_id as member_id,
        mv.verification_id as verification_id
    FROM matched mv
), update_pre_verified AS (
    UPDATE eligibility.member_versioned
    SET pre_verified = TRUE
    WHERE id IN (SELECT id FROM unprocessed_members)
)
SELECT COUNT(*) FROM unprocessed_members;

-- name: set_work_mem
-- Set the work_mem to a value in MB
SET work_mem='2000MB';

-- name: deactivate_verification_record_for_user<!
-- Deactivate verification record for the user
UPDATE eligibility.verification
SET deactivated_at = date_trunc('day', CURRENT_TIMESTAMP)
WHERE id =:verification_id
AND user_id = :user_id
RETURNING *;
