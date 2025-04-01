-- Mutations pertaining to the `eligibility.verification_attempt` table.

-- name: persist<!
-- Create or Update a verification_attempt record for an user.
INSERT INTO eligibility.verification_attempt(
    organization_id,
    unique_corp_id,
    dependent_id,
    first_name,
    last_name,
    email,
    date_of_birth,
    work_state,
    verification_type,
    policy_used,
    successful_verification,
    verification_id,
    verified_at,
    additional_fields,
    user_id
)
VALUES (
    :organization_id,
    :unique_corp_id,
    :dependent_id,
    :first_name,
    :last_name,
    :email,
    :date_of_birth,
    :work_state,
    :verification_type,
    :policy_used,
    :successful_verification,
    :verification_id,
    :verified_at,
    :additional_fields::jsonb,
    :user_id
)
RETURNING *;

-- name: bulk_persist
-- Create or Update a series of verification_attempt records.
WITH verification_attempt_record AS (
    SELECT (unnest(:records::eligibility.verification_attempt[])::eligibility.verification_attempt).*
)
INSERT INTO eligibility.verification_attempt
(
    organization_id,
    unique_corp_id,
    dependent_id,
    first_name,
    last_name,
    email,
    date_of_birth,
    work_state,
    verification_type,
    policy_used,
    successful_verification,
    verification_id,
    verified_at,
    additional_fields,
    user_id
)
SELECT
    vr.organization_id,
    vr.unique_corp_id,
    vr.dependent_id,
    vr.first_name,
    vr.last_name,
    vr.email,
    vr.date_of_birth,
    vr.work_state,
    vr.verification_type,
    vr.policy_used,
    vr.successful_verification,
    vr.verification_id,
    vr.verified_at,
    vr.additional_fields::jsonb,
    vr.user_id
FROM verification_attempt_record vr
RETURNING *;

-- name: delete<!
-- Delete a verification record.
DELETE FROM eligibility.verification_attempt
WHERE id = :id
RETURNING *;

-- name: bulk_delete!
-- Delete multiple verification_attempt records.
DELETE FROM eligibility.verification_attempt
WHERE id = any(:ids);

-- name: delete_all_for_org!
-- Delete all verification_attempt records for a given organization.
DELETE FROM eligibility.verification_attempt
WHERE organization_id = :organization_id;

