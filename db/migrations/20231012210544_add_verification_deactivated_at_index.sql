-- migrate:up transaction:false
CREATE INDEX idx_verification_deactivated_at ON eligibility.verification USING btree (deactivated_at);


-- migrate:down
DROP INDEX IF EXISTS eligibility.idx_verification_deactivated_at;
