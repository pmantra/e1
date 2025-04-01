
-- migrate:up
ALTER TABLE eligibility.tmp_file_parse_errors SET UNLOGGED;

-- migrate:down
ALTER TABLE eligibility.tmp_file_parse_errors SET LOGGED;