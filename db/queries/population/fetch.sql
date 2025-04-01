-- Queries pertaining to the `eligibility.population` table.

-- name: all
-- Get all population records.
SELECT * FROM eligibility.population;

-- name: get^
-- Get an individual population record;
SELECT * FROM eligibility.population WHERE id = :id;

-- name: get_all_active_populations
-- Get all the populations records for a given organization ID.
SELECT * FROM eligibility.population
WHERE (activated_at IS NOT NULL AND activated_at <= CURRENT_TIMESTAMP)
    AND (deactivated_at IS NULL OR deactivated_at > CURRENT_TIMESTAMP);

-- name: get_all_for_organization_id
-- Get all the populations records for a given organization ID.
SELECT * FROM eligibility.population WHERE organization_id = :organization_id;

-- name: get_active_population_for_organization_id^
-- Get the most recent active population record for a given population ID. There
-- should only be one active population record at a time, so the LIMIT 1 is
-- more of a precaution.
SELECT * FROM eligibility.population
WHERE organization_id = :organization_id
    AND (activated_at IS NOT NULL AND activated_at <= CURRENT_TIMESTAMP)
    AND (deactivated_at IS NULL OR deactivated_at > CURRENT_TIMESTAMP)
ORDER BY activated_at DESC LIMIT 1;

-- name: get_the_population_information_for_user_id^
-- Gets the active population ID and the sub population lookup keys for the specified user ID
SELECT pop.id, pop.sub_pop_lookup_keys_csv, pop.advanced, pop.organization_id
FROM eligibility.population pop
WHERE pop.organization_id = (
    SELECT organization_id
    FROM
    (
        SELECT organization_id, updated_at
        FROM eligibility.verification
        WHERE user_id = :user_id
            AND (deactivated_at IS NULL OR deactivated_at > CURRENT_TIMESTAMP)
        UNION
        SELECT organization_id, updated_at
        FROM eligibility.verification_2
        WHERE user_id = :user_id
            AND (deactivated_at IS NULL OR deactivated_at > CURRENT_TIMESTAMP)
        ORDER BY updated_at DESC
        LIMIT 1
    ) as org
)
AND (pop.activated_at IS NOT NULL AND pop.activated_at <= CURRENT_TIMESTAMP)
AND (pop.deactivated_at IS NULL OR pop.deactivated_at > CURRENT_TIMESTAMP)
ORDER BY pop.activated_at DESC
LIMIT 1;

-- name: get_the_population_information_for_user_and_org^
-- Gets the active population ID and the sub population lookup keys for the specified user and org
SELECT pop.id, pop.sub_pop_lookup_keys_csv, pop.advanced
FROM eligibility.population pop
WHERE pop.organization_id = (
    SELECT organization_id
    FROM
    (
        SELECT organization_id, updated_at
        FROM eligibility.verification
        WHERE user_id = :user_id
            AND organization_id = :organization_id
            AND (deactivated_at IS NULL OR deactivated_at > CURRENT_TIMESTAMP)
        UNION
        SELECT organization_id, updated_at
        FROM eligibility.verification_2
        WHERE user_id = :user_id
            AND organization_id = :organization_id
            AND (deactivated_at IS NULL OR deactivated_at > CURRENT_TIMESTAMP)
        ORDER BY updated_at DESC
        LIMIT 1
    ) as org
)
AND (pop.activated_at IS NOT NULL AND pop.activated_at <= CURRENT_TIMESTAMP)
AND (pop.deactivated_at IS NULL OR pop.deactivated_at > CURRENT_TIMESTAMP)
ORDER BY pop.activated_at DESC
LIMIT 1;