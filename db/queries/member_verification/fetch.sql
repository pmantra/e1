-- Queries pertaining to the `eligibility.member_verification` table.

-- name: all
-- Get all member_verification records.
SELECT * FROM eligibility.member_verification;

-- name: get^
-- Get an individual member_verification record;
SELECT * FROM eligibility.member_verification WHERE id = :id;

-- name: get_for_member_id^
-- Get the most recent verification record for a given member ID.
SELECT * FROM eligibility.member_verification WHERE member_id = :member_id
order by updated_at desc limit 1;

-- name: get_all_for_member_id
-- Get all the verification records for a given member ID.
SELECT * FROM eligibility.member_verification WHERE member_id = :member_id;


-- name: get_for_verification_id
-- Get all the verification records for a given verification ID.
SELECT * FROM eligibility.member_verification WHERE verification_id = :verification_id;

-- name: get_for_verification_attempt_id
-- Get all the verification records for a given verification_attempt ID
SELECT * FROM eligibility.member_verification WHERE verification_attempt_id = :verification_attempt_id;
