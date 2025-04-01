-- migrate:up
-- Remove unused index that was missed because of weird naming, update_at instead of updated_at
DROP INDEX IF EXISTS eligibility.idx_member_update_at;

-- Update default range to be consistent with (inc, exc) for (lower, upper)
CREATE OR REPLACE FUNCTION eligibility.default_range() RETURNS daterange
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT daterange((current_date - INTERVAL '1 day')::date, null, '[)');
$$;

create or replace function eligibility.merge_file_parse_results_members_for_file (
	file_identifier bigint
)
    returns TABLE(status text, file_id bigint, organization_id bigint, unique_corp_id eligibility.ilztext, dependent_id eligibility.citext)
    immutable
    language sql
as
$$
WITH parent_organization_id as (
    SELECT organization_id
    FROM eligibility.file f
    WHERE f.id = file_identifier::bigint
), file_records AS (
-- Only grab file records that are part of the file
    SELECT *
    FROM eligibility.file_parse_results fpr
    WHERE fpr.file_id = file_identifier::bigint
), member_records AS (
-- Only grab member records that are part of the organization attached to file
-- if we are looking at a data_provider org, grab member records from all sub-orgs
    SELECT *
    FROM eligibility.member m
    WHERE m.organization_id in (
        SELECT DISTINCT organization_id
        FROM eligibility.organization_external_id
        WHERE data_provider_organization_id = (SELECT organization_id FROM parent_organization_id)
        UNION
        SELECT organization_id
        FROM eligibility.configuration
        WHERE organization_id = (SELECT organization_id FROM parent_organization_id)
        AND data_provider = false
    )
)
-- Find the records that we want to update
SELECT
    'updated' as status,
    fr.file_id,
    (SELECT organization_id FROM parent_organization_id),
    fr.unique_corp_id,
    fr.dependent_id
FROM member_records m
INNER JOIN file_records fr
    USING(organization_id, unique_corp_id, dependent_id)
---------
    UNION
-- Find the records that are new
SELECT
    'new' as status,
    fr.file_id,
    (SELECT organization_id FROM parent_organization_id),
    fr.unique_corp_id,
    fr.dependent_id
FROM file_records fr
LEFT JOIN member_records m
    USING(organization_id, unique_corp_id, dependent_id)
WHERE m.organization_id IS NULL
    AND m.unique_corp_id IS NULL
    AND m.dependent_id IS NULL
---------
    UNION
-- Find the records that are missing
SELECT
    'expired' as status,
    file_identifier,
    (SELECT organization_id FROM parent_organization_id),
    m.unique_corp_id,
    m.dependent_id
FROM member_records m
LEFT JOIN file_records fr
    USING(organization_id, unique_corp_id, dependent_id)
-- only grab member records from the organization tied to this file
WHERE fr.organization_id IS NULL
    AND fr.unique_corp_id IS NULL
    AND fr.dependent_id IS NULL;
$$;

-- migrate:down
CREATE INDEX IF NOT EXISTS idx_member_update_at ON eligibility.member (updated_at);

CREATE OR REPLACE FUNCTION eligibility.default_range() RETURNS daterange
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT daterange((current_date - INTERVAL '1 day')::date, null, '[]');
$$;

create or replace function eligibility.merge_file_parse_results_members_for_file (
	file_identifier bigint
)
    returns TABLE(status text, file_id bigint, organization_id bigint, unique_corp_id eligibility.ilztext, dependent_id eligibility.citext)
    immutable
    language sql
as
$$
WITH file_records AS (
-- Only grab file records that are part of the file
    SELECT *
    FROM eligibility.file_parse_results fpr
    WHERE fpr.file_id = file_identifier::bigint
), member_records AS (
-- Only grab member records that are part of the organization attached to file
    SELECT *
    FROM eligibility.member m
    WHERE m.organization_id = (
        SELECT organization_id
        FROM eligibility.file f
        WHERE f.id = file_identifier::bigint
    )
)
-- Find the records that we want to update
SELECT
    'updated' as status,
    fr.file_id,
    fr.organization_id,
    fr.unique_corp_id,
    fr.dependent_id
FROM member_records m
INNER JOIN file_records fr
    USING(organization_id, unique_corp_id, dependent_id)
---------
    UNION
-- Find the records that are new
SELECT
    'new' as status,
    fr.file_id,
    fr.organization_id,
    fr.unique_corp_id,
    fr.dependent_id
FROM file_records fr
LEFT JOIN member_records m
    USING(organization_id, unique_corp_id, dependent_id)
WHERE m.organization_id IS NULL
    AND m.unique_corp_id IS NULL
    AND m.dependent_id IS NULL
---------
    UNION
-- Find the records that are missing
SELECT
    'expired' as status,
    file_identifier,
    m.organization_id,
    m.unique_corp_id,
    m.dependent_id
FROM member_records m
LEFT JOIN file_records fr
    USING(organization_id, unique_corp_id, dependent_id)
-- only grab member records from the organization tied to this file
WHERE fr.organization_id IS NULL
    AND fr.unique_corp_id IS NULL
    AND fr.dependent_id IS NULL;
$$;