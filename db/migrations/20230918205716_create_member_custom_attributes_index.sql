-- migrate:up transaction:false
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_member_custom_attributes ON eligibility.member USING gin(id, custom_attributes);


-- migrate:down
DROP INDEX IF EXISTS eligibility.idx_member_custom_attributes;
