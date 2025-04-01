-- migrate:up transaction:false
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_member_versioned_custom_attributes ON eligibility.member_versioned USING gin(id, custom_attributes);


-- migrate:down
DROP INDEX IF EXISTS eligibility.idx_member_versioned_custom_attributes;
