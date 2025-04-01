-- Queries pertaining to the `eligibility.verification_attempt` table.

-- name: all
-- Get all verification attempt records.
SELECT * FROM eligibility.verification_attempt;

-- name: get^
-- Get an individual verification_attempt record;
SELECT * FROM eligibility.verification_attempt WHERE id = :id;

-- name: get_for_org
-- Get all the verification_attempt records for a given organization ID.
SELECT * FROM eligibility.verification_attempt WHERE organization_id = :organization_id;

-- name: get_count_for_org$
-- Get the current count of verification_attempt records for a given org.
SELECT count(id) FROM eligibility.verification_attempt WHERE organization_id = :organization_id;

-- name: get_counts_for_orgs
-- Get the current count of verification_attempt records for a series of orgs.
SELECT organization_id, count(id) FROM eligibility.verification_attempt
WHERE organization_id = any(:organization_ids)
GROUP BY organization_id;

-- name: get_for_ids
-- Get verification_attempts for a given set of ids
SELECT * FROM eligibility.verification_attempt
WHERE id = any(:verification_attempt_ids);

-- name: get_successful_attempts
-- Get all successful attempts at verification
SELECT * FROM eligibility.verification_attempt
WHERE successful_verification = TRUE

-- name: get_failed_attempts
-- Get all failed attempts at verification
SELECT * FROM eligibility.verification_attempt
where successful_verification = FALSE