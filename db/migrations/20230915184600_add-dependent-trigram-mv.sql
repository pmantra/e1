-- migrate:up transaction:false
CREATE INDEX CONCURRENTLY IF NOT EXISTS trgm_idx_member_versioned_dependent_id ON eligibility.member_versioned USING GIN (dependent_id gin_trgm_ops);

-- migrate:down
DROP INDEX IF EXISTS trgm_idx_member_versioned_dependent_id;

