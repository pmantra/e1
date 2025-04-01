-- migrate:up transaction:false
DROP INDEX CONCURRENTLY IF EXISTS eligibility.idx_member_versioned_custom_attributes;


-- migrate:down transaction:false
CREATE INDEX CONCURRENTLY idx_member_versioned_custom_attributes ON eligibility.member_versioned USING gin (id, custom_attributes);

