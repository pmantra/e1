-- migrate:up transaction:false
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_verification_verification_2_id ON eligibility.verification (verification_2_id);


-- migrate:down transaction:false
DROP INDEX IF EXISTS eligibility.idx_verification_verification_2_id;
