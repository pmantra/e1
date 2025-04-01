-- migrate:up
ALTER TABLE eligibility.member_versioned_historical SET UNLOGGED;

-- migrate:down
ALTER TABLE eligibility.member_versioned_historical SET LOGGED;

