-- migrate:up
ALTER TABLE eligibility.verification ADD verified_at timestamptz NULL;
ALTER TABLE eligibility.verification ADD additional_fields jsonb NULL;
ALTER TABLE eligibility.verification_attempt ADD verified_at timestamptz NULL;
ALTER TABLE eligibility.verification_attempt ADD additional_fields jsonb NULL;
ALTER TABLE eligibility.verification ALTER COLUMN date_of_birth DROP NOT NULL;

-- migrate:down
ALTER TABLE eligibility.verification DROP COLUMN verified_at;
ALTER TABLE eligibility.verification DROP COLUMN additional_fields;
ALTER TABLE eligibility.verification_attempt DROP COLUMN verified_at;
ALTER TABLE eligibility.verification_attempt DROP COLUMN additional_fields;
ALTER TABLE eligibility.verification ALTER COLUMN date_of_birth SET NOT NULL;
