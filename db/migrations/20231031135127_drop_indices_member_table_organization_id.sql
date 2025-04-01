-- migrate:up transaction:false

DROP INDEX CONCURRENTLY IF EXISTS eligibility.idx_member_organization_id;

-- migrate:down transaction:false

CREATE INDEX CONCURRENTLY idx_member_organization_id ON eligibility.member USING btree (organization_id);