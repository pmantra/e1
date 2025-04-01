-- migrate:up transaction:false

DROP INDEX CONCURRENTLY IF EXISTS eligibility.idx_primary_verification;

-- migrate:down transaction:false

CREATE INDEX CONCURRENTLY idx_primary_verification ON eligibility.member USING btree (date_of_birth, btrim(lower((email)::text)) text_pattern_ops);