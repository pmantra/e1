-- migrate:up
ALTER TABLE eligibility.member_address_versioned DROP COLUMN IF EXISTS hash_value;
ALTER TABLE eligibility.member_address_versioned DROP COLUMN IF EXISTS hash_version;

-- migrate:down
ALTER TABLE eligibility.member_address_versioned ADD IF NOT EXISTS hash_value eligibility."iwstext" NULL;
ALTER TABLE eligibility.member_address_versioned ADD IF NOT EXISTS hash_version int NULL;
