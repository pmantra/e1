-- migrate:up transaction:false
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_member_verification_verification_id ON eligibility.member_verification USING btree (verification_id);

-- migrate:down
DROP INDEX IF EXISTS eligibility.idx_member_verification_verification_id;