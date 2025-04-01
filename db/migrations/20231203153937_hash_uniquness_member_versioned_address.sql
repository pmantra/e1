-- migrate:up
ALTER TABLE eligibility.member_address_versioned DROP CONSTRAINT IF EXISTS member_address_versioned_hash_value_hash_version_key;
ALTER TABLE eligibility.member_address_versioned ADD CONSTRAINT  member_address_versioned_hash_value_hash_version_key UNIQUE (hash_value, hash_version);


-- migrate:down
ALTER TABLE eligibility.member_address_versioned DROP CONSTRAINT IF EXISTS member_address_versioned_hash_value_hash_version_key;
