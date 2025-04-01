-- migrate:up
-- We should never never never have a header entry with an empty alias.
DELETE FROM eligibility.header_alias WHERE alias = '' or alias IS NULL;

-- migrate:down

