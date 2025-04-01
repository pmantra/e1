-- migrate:up
ALTER TABLE eligibility.verification ADD COLUMN IF NOT EXISTS verification_2_id BIGINT DEFAULT NULL;

-- migrate:down
ALTER TABLE eligibility.verification DROP COLUMN IF EXISTS verification_2_id;
