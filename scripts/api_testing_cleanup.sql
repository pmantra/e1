-- Clean up v2
DELETE from eligibility.verification_2 where member_id = 3212635537415;

-- Clean up v1
WITH deleted_verification_ids AS (
    DELETE FROM eligibility.member_verification 
    WHERE member_id = 5406183
    RETURNING verification_id
),
temp AS (
    DELETE FROM eligibility.verification_attempt 
    WHERE verification_id in (SELECT verification_id FROM deleted_verification_ids)
    RETURNING *
)
DELETE from eligibility.verification WHERE id in (SELECT verification_id FROM deleted_verification_ids);