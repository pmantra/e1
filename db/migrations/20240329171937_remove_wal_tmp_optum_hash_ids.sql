
-- migrate:up
ALTER TABLE eligibility.tmp_optum_hash_ids SET UNLOGGED;

-- migrate:down
ALTER TABLE eligibility.tmp_optum_hash_ids SET LOGGED;