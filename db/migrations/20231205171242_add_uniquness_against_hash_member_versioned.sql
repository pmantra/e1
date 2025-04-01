-- migrate:up transaction:false
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_member_versioned_hash_value_and_version ON eligibility.member_versioned USING btree (hash_value, hash_version);

-- migrate:down
DROP INDEX IF EXISTS idx_member_versioned_hash_value_and_version;
