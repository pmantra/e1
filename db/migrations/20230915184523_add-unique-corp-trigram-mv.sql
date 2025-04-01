-- migrate:up transaction:false
CREATE INDEX CONCURRENTLY IF NOT EXISTS trgm_idx_member_versioned_unique_corp_id ON eligibility.member_versioned USING GIN (unique_corp_id gin_trgm_ops);

-- migrate:down
DROP INDEX IF EXISTS trgm_idx_member_versioned_unique_corp_id;

