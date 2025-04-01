-- migrate:up
CREATE TYPE eligibility.file_error AS ENUM ('missing', 'delimiter', 'unknown');

ALTER TABLE eligibility.file ADD COLUMN IF NOT EXISTS error eligibility.file_error NULL;

-- migrate:down
ALTER TABLE eligibility.file DROP COLUMN IF EXISTS error;

DROP TYPE IF EXISTS eligibility.file_error;


