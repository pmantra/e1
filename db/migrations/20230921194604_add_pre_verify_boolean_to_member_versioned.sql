-- migrate:up
ALTER TABLE eligibility.member_versioned ADD COLUMN IF NOT EXISTS pre_verified BOOLEAN NOT NULL DEFAULT FALSE;

-- migrate:down
ALTER TABLE eligibility.member_versioned DROP COLUMN IF EXISTS pre_verified CASCADE;
