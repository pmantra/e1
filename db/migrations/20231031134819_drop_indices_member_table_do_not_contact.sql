-- migrate:up transaction:false

DROP INDEX CONCURRENTLY IF EXISTS eligibility.idx_member_id_do_not_contact;


-- migrate:down transaction:false


CREATE INDEX CONCURRENTLY idx_member_id_do_not_contact ON eligibility.member USING btree (id, do_not_contact);