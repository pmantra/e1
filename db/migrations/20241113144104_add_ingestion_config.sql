-- migrate:up
ALTER TABLE eligibility.configuration ADD COLUMN IF NOT EXISTS ingestion_config jsonb DEFAULT NULL;

-- migrate:down
ALTER TABLE eligibility.configuration DROP COLUMN IF EXISTS ingestion_config;
