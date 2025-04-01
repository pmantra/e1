-- migrate:up
ALTER TABLE eligibility.file
ADD COLUMN IF NOT EXISTS success_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS failure_count INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS raw_count INTEGER DEFAULT 0;

-- migrate:down
ALTER TABLE eligibility.file
DROP COLUMN IF EXISTS success_count,
DROP COLUMN IF EXISTS failure_count,
DROP COLUMN IF EXISTS raw_count;
