-- migrate:up
ALTER TABLE eligibility.verification_attempt ADD user_id int NULL;


-- migrate:down
ALTER TABLE eligibility.verification_attempt DROP COLUMN user_id;

