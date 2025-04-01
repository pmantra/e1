-- Queries pertaining to the `eligibility.member_sub_population` table.

-- name: get_sub_population_id_for_member_id$
-- Get an individual member sub-population record;
SELECT sub_population_id FROM eligibility.member_sub_population WHERE member_id = :member_id ORDER BY updated_at DESC LIMIT 1;

-- name: get_all_member_ids_for_sub_population_id
-- Get all the member sub-population records for a given sub-population ID.
SELECT member_id FROM eligibility.member_sub_population WHERE sub_population_id = :sub_population_id;

-- name: get_all_active_member_ids_for_sub_population_id
-- Get all the member sub-population records for active members for a given sub-population ID.
SELECT member_id FROM eligibility.member_sub_population msp
    INNER JOIN eligibility.member_versioned mv ON msp.member_id = mv.id
    WHERE msp.sub_population_id = :sub_population_id
    AND mv.effective_range @> CURRENT_DATE;
