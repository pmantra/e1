-- migrate:up
DELETE FROM eligibility.member;
DELETE FROM eligibility.file;
DELETE FROM eligibility.configuration;

-- migrate:down

