-- migrate:up
ALTER TABLE eligibility.member_versioned_historical DROP CONSTRAINT IF EXISTS member_versioned_historical_hash_value_hash_version_key;
ALTER TABLE eligibility.member_versioned_historical ADD CONSTRAINT  member_versioned_historical_hash_value_hash_version_key UNIQUE (hash_value, hash_version);

-- migrate:down
ALTER TABLE eligibility.member_versioned_historical DROP CONSTRAINT  IF EXISTS member_versioned_historical_hash_value_hash_version_key;
