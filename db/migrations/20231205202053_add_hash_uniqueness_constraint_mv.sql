-- migrate:up
ALTER TABLE eligibility.member_versioned DROP CONSTRAINT IF EXISTS mv_unique_hash_value_and_version;
ALTER TABLE eligibility.member_versioned ADD CONSTRAINT
     mv_unique_hash_value_and_version UNIQUE USING INDEX idx_member_versioned_hash_value_and_version;

-- migrate:down
ALTER TABLE eligibility.member_versioned DROP CONSTRAINT IF EXISTS
     mv_unique_hash_value_and_version

