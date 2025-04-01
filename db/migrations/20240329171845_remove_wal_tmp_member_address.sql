
-- migrate:up
ALTER TABLE eligibility.tmp_member_address SET UNLOGGED;

-- migrate:down
ALTER TABLE eligibility.tmp_member_address SET LOGGED;