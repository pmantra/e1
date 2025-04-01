-- Mutations pertaining to the `eligibility.member_sub_population` table.

-- name: persist<!
-- Create or Update a MemberSubPopulation record.
INSERT INTO eligibility.member_sub_population(
    member_id,
    sub_population_id
)
VALUES (
    :member_id,
    :sub_population_id
)
ON CONFLICT ON CONSTRAINT member_sub_population_member_id_key
    DO UPDATE SET
        sub_population_id = EXCLUDED.sub_population_id
RETURNING *;

-- name: bulk_persist
-- Create or Update an org configuration with the provided information.
WITH member_sub_population_records AS (
    SELECT (unnest(:records::eligibility.member_sub_population[])::eligibility.member_sub_population).*
)
INSERT INTO eligibility.member_sub_population
(
    member_id,
    sub_population_id
)
SELECT
    mem_sub_pop_recs.member_id,
    mem_sub_pop_recs.sub_population_id
FROM member_sub_population_records mem_sub_pop_recs
ON CONFLICT ON CONSTRAINT member_sub_population_member_id_key
    DO UPDATE SET
        sub_population_id = EXCLUDED.sub_population_id
    WHERE
        eligibility.member_sub_population.sub_population_id != EXCLUDED.sub_population_id
RETURNING *;
