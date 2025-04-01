-- migrate:up transaction:false

DROP INDEX CONCURRENTLY  IF EXISTS eligibility.idx_member_email;

-- migrate:down transaction:false

CREATE INDEX CONCURRENTLY idx_member_email ON eligibility.member USING btree (btrim(lower((email)::text)) text_pattern_ops);