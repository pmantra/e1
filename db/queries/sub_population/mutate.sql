-- Mutations pertaining to the `eligibility.sub_population` table.

-- name: persist<!
-- Create or Update a sub_population record.
INSERT INTO eligibility.sub_population(
    population_id,
    feature_set_name,
    feature_set_details_json
)
VALUES (
    :population_id,
    :feature_set_name,
    :feature_set_details_json
)
RETURNING *;

-- name: bulk_persist
-- Create or Update a series of sub_population records.
WITH sub_population_records AS (
    SELECT (unnest(:records::eligibility.sub_population[])::eligibility.sub_population).*
)
INSERT INTO eligibility.sub_population
(
    population_id,
    feature_set_name,
    feature_set_details_json
)
SELECT
    spr.population_id,
    spr.feature_set_name,
    spr.feature_set_details_json
FROM sub_population_records spr
RETURNING *;

-- name: delete<!
-- Delete a sub_population record.
DELETE FROM eligibility.sub_population
WHERE id = :id
RETURNING *;

-- name: bulk_delete!
-- Delete multiple sub_population records.
DELETE FROM eligibility.sub_population
WHERE id = any(:ids);

