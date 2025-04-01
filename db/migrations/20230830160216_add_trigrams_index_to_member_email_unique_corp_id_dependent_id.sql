-- migrate:up
CREATE INDEX IF NOT EXISTS trgm_idx_member_email ON eligibility.member USING GIN (email gin_trgm_ops);
CREATE INDEX IF NOT EXISTS trgm_idx_member_unique_corp_id ON eligibility.member USING GIN (unique_corp_id gin_trgm_ops);
CREATE INDEX IF NOT EXISTS trgm_idx_member_dependent_id ON eligibility.member USING GIN (dependent_id gin_trgm_ops);

-- migrate:down
DROP INDEX IF EXISTS trgm_idx_member_email;
DROP INDEX IF EXISTS trgm_idx_member_unique_corp_id;
DROP INDEX IF EXISTS trgm_idx_member_dependent_id;