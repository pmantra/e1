-- migrate:up
CREATE INDEX IF NOT EXISTS idx_header_organization_id ON eligibility.header_alias (organization_id);
CREATE INDEX IF NOT EXISTS idx_file_organization_id ON eligibility.file (organization_id);
CREATE INDEX IF NOT EXISTS idx_configuration_organization_id ON eligibility.configuration (organization_id);

-- migrate:down

DROP INDEX IF EXISTS eligibility.idx_header_organization_id;
DROP INDEX IF EXISTS eligibility.idx_file_organization_id;
DROP INDEX IF EXISTS eligibility.idx_configuration_organization_id;

