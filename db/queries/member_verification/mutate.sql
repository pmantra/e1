-- Mutations pertaining to the `eligibility.member_verification` table.

-- name: persist<!
-- Create or Update a member_verification record.
INSERT INTO eligibility.member_verification(
    member_id,
    verification_id,
    verification_attempt_id
)
VALUES (
    :member_id,
    :verification_id,
    :verification_attempt_id
)
RETURNING *;

-- name: bulk_persist
-- Create or Update a series of member_verification records.
WITH member_verification_records AS (
    SELECT (unnest(:records::eligibility.member_verification[])::eligibility.member_verification).*
)
INSERT INTO eligibility.member_verification
(
    member_id,
    verification_id,
    verification_attempt_id
)
SELECT
    mvr.member_id,
    mvr.verification_id,
    mvr.verification_attempt_id
FROM member_verification_records mvr
RETURNING *;

-- name: delete<!
-- Delete a member_verification record.
DELETE FROM eligibility.member_verification
WHERE id = :id
RETURNING *;

-- name: bulk_delete!
-- Delete multiple member_verification records.
DELETE FROM eligibility.member_verification
WHERE id = any(:ids);

