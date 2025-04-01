-- migrate:up
ALTER TABLE eligibility.member ADD IF NOT EXISTS work_country eligibility.citext NULL DEFAULT NULL;
ALTER TABLE eligibility.member ADD IF NOT EXISTS custom_attributes JSONB NULL DEFAULT NULL;


-- migrate:down
ALTER TABLE eligibility.member DROP COLUMN IF EXISTS custom_attributes;
ALTER TABLE eligibility.member DROP COLUMN IF EXISTS work_country;