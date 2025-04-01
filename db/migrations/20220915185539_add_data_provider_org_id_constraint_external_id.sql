-- migrate:up
CREATE UNIQUE INDEX uidx_data_provider_id_external_id ON eligibility.organization_external_id USING btree (data_provider_organization_id, external_id);

-- migrate:down
DROP INDEX IF EXISTS eligibility.uidx_data_provider_id_external_id;
