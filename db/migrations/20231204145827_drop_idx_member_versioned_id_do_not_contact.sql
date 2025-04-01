-- migrate:up transaction:false
DROP INDEX CONCURRENTLY IF EXISTS eligibility.idx_member_versioned_id_do_not_contact;

-- migrate:down transaction:false
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_member_versioned_id_do_not_contact ON eligibility.member_versioned USING btree (id, do_not_contact);
