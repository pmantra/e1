-- Mutations pertaining to the `eligibility.configuration` table

-- name: persist<!
-- Create or Update an org configuration with the provided information.
INSERT INTO eligibility.header_alias(organization_id, header, alias)
VALUES (:organization_id, :header, :alias)
ON CONFLICT (organization_id, header) DO UPDATE SET alias = excluded.alias
RETURNING *;

-- name: bulk_persist*!
-- Bulk Create or Update a series of header aliases.
INSERT INTO eligibility.header_alias(organization_id, header, alias)
VALUES (:organization_id, :header, :alias)
ON CONFLICT (organization_id, header) DO UPDATE SET alias = excluded.alias;


-- name: delete<!
-- Delete a header alias for an organization.
DELETE FROM eligibility.header_alias
WHERE id = :id
RETURNING *;

-- name: bulk_delete!
-- Bulk delete a series of header aliases.
DELETE FROM eligibility.header_alias
WHERE id = ANY(:ids);

-- name: delete_missing!
-- Delete any header aliases for an organization that are no longer configured.
-- This is optimized for guaranteeing the query planner uses indexes to perform the scans.
-- Equivalent query: delete from header_alias where organization_id = $1 and header <> ALL($2)
WITH missing AS (
    WITH existing AS (
        SELECT unnest(:headers::text[]) as header
    )
    SELECT id from eligibility.header_alias
    WHERE organization_id = :organization_id
        EXCEPT
    SELECT id from eligibility.header_alias
    INNER JOIN existing ON header_alias.header = existing.header
    WHERE organization_id = :organization_id
)
DELETE FROM eligibility.header_alias
WHERE
    organization_id = :organization_id
    AND id = ANY(SELECT id FROM missing)
;

-- name: bulk_refresh
-- Persist the provided header aliases and proactively delete any un-seen mappings.
WITH headers AS (
    SELECT *
    FROM jsonb_to_recordset(:headers::jsonb)
    AS t(organization_id bigint, header text, alias text)
-- Upsert all the header aliases, returning the result.
), persisted AS (
    INSERT INTO eligibility.header_alias (organization_id, header, alias)
        SELECT organization_id, header, alias FROM headers
    ON CONFLICT (organization_id, header) DO UPDATE SET alias = excluded.alias
    RETURNING *
-- Locate any headers not seen in the upsert.
), missing AS (
    SELECT header_alias.id
    FROM eligibility.header_alias
    INNER JOIN (select organization_id FROM persisted) AS given
        ON header_alias.organization_id = given.organization_id
    EXCEPT
        SELECT id FROM persisted
-- Delete those headers which were not sent in the upsert.
), _ AS (
    DELETE FROM eligibility.header_alias WHERE id = ANY(SELECT id FROM missing)
    RETURNING *
)
-- Return the result of the upsert.
SELECT * FROM persisted;
