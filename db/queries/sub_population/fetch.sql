-- Queries pertaining to the `eligibility.sub_population` table.

-- name: all
-- Get all sub_population records.
SELECT * FROM eligibility.sub_population;

-- name: get^
-- Get an individual sub_population record;
SELECT * FROM eligibility.sub_population WHERE id = :id;

-- name: get_for_population_id
-- Get all the sub_population records for the the specified population
SELECT * FROM eligibility.sub_population WHERE population_id = :population_id;

-- name: get_for_active_population_for_organization_id
-- Get all the sub_population records for the most recent, active population for a given organization ID
SELECT * FROM eligibility.sub_population sub_pop
INNER JOIN (
    SELECT pop.id FROM eligibility.population pop
    WHERE pop.organization_id = :organization_id
        AND pop.deactivated_at IS NULL
    ORDER BY pop.activated_at DESC LIMIT 1
) AS latest_active_pop
ON sub_pop.population_id = latest_active_pop.id;

-- name: get_feature_list_of_type_for_id^
-- Gets the list of feature IDs of the specified type for the specified sub_population ID
SELECT sub_pop.feature_set_details_json->>:feature_type FROM eligibility.sub_population sub_pop
WHERE sub_pop.id = :id;
