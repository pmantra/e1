-- migrate:up
ALTER TABLE eligibility.member_versioned ADD IF NOT EXISTS hash_value eligibility."iwstext" NULL ;
ALTER TABLE eligibility.member_versioned ADD IF NOT EXISTS hash_version int NULL;

ALTER TABLE eligibility.member_versioned_historical ADD IF NOT EXISTS hash_value eligibility."iwstext" NULL;
ALTER TABLE eligibility.member_versioned_historical ADD IF NOT EXISTS hash_version int NULL;

ALTER TABLE eligibility.member_address_versioned ADD IF NOT EXISTS hash_value eligibility."iwstext" NULL;
ALTER TABLE eligibility.member_address_versioned ADD IF NOT EXISTS hash_version int NULL;



-- migrate:down
ALTER TABLE eligibility.member_versioned DROP COLUMN IF EXISTS hash_value;
ALTER TABLE eligibility.member_versioned DROP COLUMN IF EXISTS hash_version;

ALTER TABLE eligibility.member_versioned_historical DROP COLUMN IF EXISTS hash_value;
ALTER TABLE eligibility.member_versioned_historical DROP COLUMN IF EXISTS hash_version;

ALTER TABLE eligibility.member_address_versioned DROP COLUMN IF EXISTS hash_value;
ALTER TABLE eligibility.member_address_versioned DROP COLUMN IF EXISTS hash_version;


