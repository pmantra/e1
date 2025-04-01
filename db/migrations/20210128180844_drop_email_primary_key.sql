-- migrate:up
-- Drop the `primary_key` configuration column.
ALTER TABLE eligibility.configuration
    DROP COLUMN primary_key;

-- Remove `email` from org identity.
-- First drop the index.
DROP INDEX IF EXISTS eligibility.uidx_member_identity;
-- Drop the helper function.
DROP CAST (eligibility.member AS eligibility.org_identity);
DROP FUNCTION IF EXISTS eligibility.get_identity(eligibility.member);
-- Drop any duplicates.
-- https://www.postgresqltutorial.com/how-to-delete-duplicate-rows-in-postgresql/
DELETE FROM eligibility.member dupes
    USING eligibility.member
    WHERE dupes.id < member.id
        AND dupes.organization_id = member.organization_id
        AND dupes.unique_corp_id = member.unique_corp_id
        AND dupes.dependent_id = member.dependent_id;

-- Re-create the type, without email.
DROP TYPE IF EXISTS eligibility.org_identity;
CREATE TYPE eligibility.org_identity AS (
    organization_id BIGINT,
    unique_corp_id eligibility.ilztext,
    dependent_id TEXT COLLATE eligibility.ci
);
-- Re-define the `get_identity` function for the new type.
CREATE OR REPLACE FUNCTION eligibility.get_identity(eligibility.member)
RETURNS eligibility.org_identity AS $$
    SELECT (
        $1.organization_id, ltrim($1.unique_corp_id, '0'), $1.dependent_id
    )::eligibility.org_identity;
$$ LANGUAGE sql immutable;
CREATE CAST (eligibility.member AS eligibility.org_identity)
    WITH FUNCTION eligibility.get_identity(eligibility.member) AS IMPLICIT;
-- Add the identity index back in.
CREATE UNIQUE INDEX IF NOT EXISTS uidx_member_identity
    ON eligibility.member (organization_id, ltrim(unique_corp_id, '0'), dependent_id);


-- migrate:down

ALTER TABLE eligibility.configuration
    ADD COLUMN primary_key TEXT NOT NULL DEFAULT 'unique_corp_id';

-- Add `email` to the org identity.
-- First drop the index.
DROP INDEX IF EXISTS uidx_member_identity;
-- Then drop the column.
ALTER TABLE eligibility.member DROP COLUMN identity;
-- Drop the helper function.
DROP CAST (eligibility.member AS eligibility.org_identity);
DROP FUNCTION IF EXISTS eligibility.get_identity(eligibility.member);
-- Re-create the type, *with* email.
DROP TYPE IF EXISTS eligibility.org_identity;
CREATE TYPE eligibility.org_identity AS (
    organization_id BIGINT,
    email TEXT COLLATE eligibility.ci,
    unique_corp_id eligibility.ilztext,
    dependent_id TEXT COLLATE eligibility.ci
);
-- Re-define the `get_identity` function for the new type.
CREATE OR REPLACE FUNCTION eligibility.get_identity(eligibility.member)
RETURNS eligibility.org_identity AS $$
    SELECT (
        $1.organization_id, $1.email, ltrim($1.unique_corp_id, '0'), $1.dependent_id
    )::eligibility.org_identity;
$$ LANGUAGE sql immutable;
CREATE CAST (eligibility.member AS eligibility.org_identity)
    WITH FUNCTION eligibility.get_identity(eligibility.member) AS IMPLICIT;
-- Add the identity column back in.
CREATE UNIQUE INDEX IF NOT EXISTS uidx_member_identity
    ON eligibility.member (
        organization_id, email, ltrim(unique_corp_id, '0'), dependent_id
    );
