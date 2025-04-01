-- migrate:up transaction:false

DROP INDEX CONCURRENTLY IF EXISTS eligibility.idx_unique_corp_id;

-- migrate:down transaction:false

CREATE INDEX CONCURRENTLY idx_unique_corp_id ON eligibility.member USING btree (ltrim(lower((unique_corp_id)::text), '0'::text) text_pattern_ops);
