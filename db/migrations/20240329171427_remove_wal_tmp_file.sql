-- migrate:up
ALTER TABLE eligibility.tmp_file SET UNLOGGED;

-- migrate:down
ALTER TABLE eligibility.tmp_file SET LOGGED;
