-- migrate:up
ALTER TABLE eligibility.configuration ADD data_provider boolean DEFAULT false NOT NULL;

-- migrate:down
ALTER TABLE eligibility.configuration DROP COLUMN data_provider;
