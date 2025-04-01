-- migrate:up transaction:false

DROP INDEX CONCURRENTLY IF EXISTS eligibility.idx_secondary_verification;

-- migrate:down transaction:false

CREATE INDEX CONCURRENTLY idx_secondary_verification ON eligibility.member USING btree (date_of_birth, btrim(lower((first_name)::text)), btrim(lower((last_name)::text)), btrim(lower((work_state)::text)) text_pattern_ops);
