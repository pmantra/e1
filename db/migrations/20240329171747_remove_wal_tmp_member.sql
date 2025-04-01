
-- migrate:up
ALTER TABLE eligibility.tmp_member SET UNLOGGED;

-- migrate:down
ALTER TABLE eligibility.tmp_member SET LOGGED;