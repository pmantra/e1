-- migrate:up transaction:false
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_member_versioned_pre_verified ON eligibility.member_versioned(pre_verified) WHERE pre_verified = FALSE;

-- migrate:down
DROP INDEX IF EXISTS eligibility.idx_member_versioned_pre_verified;
