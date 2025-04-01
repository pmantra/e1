-- migrate:up
CREATE INDEX trgm_idx_member_first_name ON eligibility.member USING GIN (first_name gin_trgm_ops);
CREATE INDEX trgm_idx_member_last_name ON eligibility.member USING GIN (last_name gin_trgm_ops);

-- migrate:down

DROP INDEX IF EXISTS trgm_idx_member_first_name;
DROP INDEX IF EXISTS trgm_idx_member_last_name;