-- migrate:up

-- Update this type to mirror the field types in `eligibility.parsed_record`...
ALTER TYPE eligibility.external_record
    ALTER ATTRIBUTE record TYPE jsonb,
    ALTER ATTRIBUTE unique_corp_id TYPE eligibility.ilztext,
    ALTER ATTRIBUTE dependent_id TYPE eligibility.citext;

-- Migrating these fields to their newer alternatives so behavior is the same anywhere it's used...
--  Also so we can eventually drop the legacy collation and domain
ALTER TYPE eligibility.org_identity
    ALTER ATTRIBUTE unique_corp_id TYPE eligibility.ilztext,
    ALTER ATTRIBUTE dependent_id TYPE eligibility.citext;
ALTER TABLE eligibility.organization_external_id
    ALTER COLUMN external_id TYPE eligibility.citext,
    ALTER COLUMN source TYPE eligibility.citext;

-- migrate:down

ALTER TYPE eligibility.external_record
    ALTER ATTRIBUTE record TYPE jsonb,
    ALTER ATTRIBUTE unique_corp_id TYPE eligibility.ilztextci,
    ALTER ATTRIBUTE dependent_id TYPE text;

ALTER TYPE eligibility.org_identity
    ALTER ATTRIBUTE unique_corp_id TYPE eligibility.ilztextci,
    ALTER ATTRIBUTE dependent_id TYPE text COLLATE eligibility.ci;
ALTER TABLE eligibility.organization_external_id
    ALTER COLUMN external_id TYPE text COLLATE eligibility.ci,
    ALTER COLUMN source TYPE text COLLATE eligibility.ci;