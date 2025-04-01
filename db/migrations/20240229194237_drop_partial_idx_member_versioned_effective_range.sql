-- migrate:up transaction:false
DROP INDEX CONCURRENTLY IF EXISTS eligibility.partial_idx_member_versioned_effective_range;

-- migrate:down transaction:false
CREATE INDEX CONCURRENTLY IF NOT EXISTS partial_idx_member_versioned_effective_range ON eligibility.member_versioned USING gist (effective_range) WHERE eligibility.is_active(effective_range);
