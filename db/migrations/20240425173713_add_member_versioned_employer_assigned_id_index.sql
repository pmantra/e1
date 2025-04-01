-- migrate:up transaction:false
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_member_versioned_employer_assigned_id ON eligibility.member_versioned USING btree (btrim(lower((employer_assigned_id)::text)) text_pattern_ops);


-- migrate:down transaction:false
DROP INDEX IF EXISTS eligibility.idx_member_versioned_employer_assigned_id;
