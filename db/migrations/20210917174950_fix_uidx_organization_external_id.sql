-- migrate:up
DROP INDEX IF EXISTS eligibility.uidx_organization_external_id;
CREATE UNIQUE INDEX uidx_organization_external_id
    ON eligibility.organization_external_id (source, external_id);

-- migrate:down
DROP INDEX IF EXISTS eligibility.uidx_organization_external_id;
CREATE UNIQUE INDEX uidx_organization_external_id
    ON eligibility.organization_external_id (organization_id, source, external_id);


