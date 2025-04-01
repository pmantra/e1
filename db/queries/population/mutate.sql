-- Mutations pertaining to the `eligibility.population` table.

-- name: persist<!
-- Create a population record.
INSERT INTO eligibility.population(
    organization_id,
    sub_pop_lookup_keys_csv,
    sub_pop_lookup_map_json,
    activated_at,
    deactivated_at,
    advanced
)
VALUES (
    :organization_id,
    :sub_pop_lookup_keys_csv,
    :sub_pop_lookup_map_json,
    :activated_at,
    :deactivated_at,
    :advanced
)
RETURNING *;

-- name: bulk_persist
-- Create a series of population records.
WITH population_records AS (
    SELECT (unnest(:records::eligibility.population[])::eligibility.population).*
)
INSERT INTO eligibility.population
(
    organization_id,
    sub_pop_lookup_keys_csv,
    sub_pop_lookup_map_json,
    activated_at,
    deactivated_at,
    advanced
)
SELECT
    popr.organization_id,
    popr.sub_pop_lookup_keys_csv,
    popr.sub_pop_lookup_map_json,
    popr.activated_at,
    popr.deactivated_at,
    popr.advanced
FROM population_records popr
RETURNING *;

-- name: set_sub_pop_lookup_info
-- Sets the sub-populations lookup keys and map for the population
UPDATE eligibility.population
    SET
        sub_pop_lookup_keys_csv = :sub_pop_lookup_keys_csv,
        sub_pop_lookup_map_json = :sub_pop_lookup_map_json
    WHERE id = :id;

-- name: activate_population
-- Activates a population record
UPDATE eligibility.population
    SET activated_at = COALESCE(:activated_at, CURRENT_TIMESTAMP)
    WHERE id = :id;

-- name: deactivate_population
-- Deactivates a population record
UPDATE eligibility.population
    SET deactivated_at = COALESCE(:deactivated_at, CURRENT_TIMESTAMP)
    WHERE id = :id;

-- name: deactivate_populations_for_organization_id
-- Deactivates population records for the specified organization
UPDATE eligibility.population
    SET deactivated_at = COALESCE(:deactivated_at, CURRENT_TIMESTAMP)
    WHERE organization_id = :organization_id
    AND deactivated_at IS NULL;

-- name: delete<!
-- Delete a population record.
DELETE FROM eligibility.population
WHERE id = :id
RETURNING *;

-- name: bulk_delete!
-- Delete multiple population records.
DELETE FROM eligibility.population
WHERE id = any(:ids);

