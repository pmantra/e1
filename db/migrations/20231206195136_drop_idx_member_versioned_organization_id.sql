-- migrate:up transaction:false
DROP INDEX CONCURRENTLY IF EXISTS eligibility.idx_member_versioned_organization_id;

-- migrate:down transaction:false
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_member_versioned_organization_id ON eligibility.member_versioned USING btree (organization_id);
