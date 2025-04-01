-- migrate:up
ALTER TABLE eligibility.organization_external_id ALTER COLUMN "source" DROP NOT NULL;
ALTER TABLE eligibility.organization_external_id ADD data_provider_organization_id int8 NULL;


-- migrate:down
ALTER TABLE eligibility.organization_external_id DROP COLUMN data_provider_organization_id;
ALTER TABLE eligibility.organization_external_id ALTER COLUMN "source" SET NOT NULL;

