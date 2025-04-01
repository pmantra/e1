-- Queries pertaining to the `eligibility.verification_2` table.

-- name: get_for_member_id^
-- Get the most recent verification record for a memberId
SELECT v.*
FROM eligibility.verification_2 v
WHERE v.member_id = :member_id
AND (
    v.deactivated_at IS NULL
        OR
    v.deactivated_at > CURRENT_DATE
)
ORDER BY v.updated_at DESC
LIMIT 1;

-- name: get_verification_key_for_id^
-- Get the most recent verification record of 2.0 for a verification 2 ID
SELECT member_id, organization_id, created_at, id as verification_2_id, member_version
FROM eligibility.verification_2
WHERE id = :id
ORDER BY created_at Desc
LIMIT 1;

-- name: get_verification_key_for_user_and_org^
-- Get the most recent verification record for user and org ID
SELECT member_id, organization_id, created_at, id as verification_2_id, member_version
FROM eligibility.verification_2
WHERE user_id = :user_id AND organization_id = :organization_id
ORDER BY verification_2_id DESC
LIMIT 1
