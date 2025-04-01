-- migrate:up transaction:false
CREATE INDEX idx_verification_organization_id ON eligibility.verification USING btree (organization_id);


-- migrate:down
DROP INDEX IF EXISTS eligibility.idx_verification_organization_id;