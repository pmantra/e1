-- migrate:up transaction:false
CREATE INDEX CONCURRENTLY IF NOT EXISTS trgm_idx_member_versioned_email ON eligibility.member_versioned USING GIN (email gin_trgm_ops);

-- migrate:down
DROP INDEX IF EXISTS trgm_idx_member_versioned_email;

