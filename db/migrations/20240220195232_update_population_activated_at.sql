-- migrate:up transaction:false
ALTER TABLE eligibility.population ALTER COLUMN activated_at DROP NOT NULL;
ALTER TABLE eligibility.population ALTER COLUMN activated_at SET DEFAULT NULL;

-- migrate:down transaction:false
ALTER TABLE eligibility.population ALTER COLUMN activated_at SET DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE eligibility.population ALTER COLUMN activated_at SET NOT NULL;
