-- Mutations pertaining to the `eligibility.verification_2` table.

-- name: persist<!
-- Create or Update a Verification2 record for an organization.
INSERT INTO eligibility.verification_2
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
 member_id,
 member_version)
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
    :member_id,
    :member_version
)
RETURNING *;

-- name: deactivate_verification_2_record_for_user<!
-- Deactivate verification_2 record for the user
UPDATE eligibility.verification_2
SET deactivated_at = CURRENT_TIMESTAMP
WHERE id =:verification_id
AND user_id = :user_id
RETURNING *;

-- name: bulk_persist
-- Create or Update a series of verification records.
WITH verification_records AS (
    SELECT (unnest(:records::eligibility.verification_2[])::eligibility.verification_2).*
)
INSERT INTO eligibility.verification_2
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
    member_id,
    member_version
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
    vr.member_id,
    vr.member_version
FROM verification_records vr
RETURNING *;
