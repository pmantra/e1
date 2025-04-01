-- migrate:up

CREATE TABLE IF NOT EXISTS eligibility.organization_external_id (
    id BIGSERIAL PRIMARY KEY NOT NULL,
    source TEXT COLLATE eligibility.ci NOT NULL,
    external_id TEXT COLLATE eligibility.ci NOT NULL,
    organization_id BIGINT REFERENCES eligibility.configuration(organization_id) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS uidx_organization_external_id
    ON eligibility.organization_external_id(source, external_id, organization_id);

ALTER TABLE eligibility.configuration ADD COLUMN IF NOT EXISTS
    email_domains TEXT[] NOT NULL default '{}'::TEXT[];

ALTER TABLE eligibility.header_alias ADD COLUMN IF NOT EXISTS
    is_eligibility_field BOOLEAN NOT NULL DEFAULT false;

-- migrate:down

DROP TABLE IF EXISTS eligibility.organization_external_id;
DROP INDEX IF EXISTS eligibility.uidx_external_org_id;
ALTER TABLE eligibility.configuration DROP COLUMN IF EXISTS email_domains;
ALTER TABLE eligibility.header_alias DROP COLUMN IF EXISTS is_eligibility_field;