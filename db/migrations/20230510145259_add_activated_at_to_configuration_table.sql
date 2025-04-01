-- migrate:up
ALTER TABLE eligibility.configuration ADD COLUMN activated_at date;


-- migrate:down
ALTER TABLE eligibility.configuration DROP COLUMN activated_at date;
