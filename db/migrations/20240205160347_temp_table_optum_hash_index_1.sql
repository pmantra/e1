-- migrate:up transaction:false
CREATE INDEX CONCURRENTLY IF NOT EXISTS tmp_optum_hash_ids_organization_id_updated_idx ON eligibility.tmp_optum_hash_ids (organization_id,updated_at);


-- migrate:down
DROP INDEX IF EXISTS eligibility.tmp_optum_hash_ids_organization_id_updated_idx;