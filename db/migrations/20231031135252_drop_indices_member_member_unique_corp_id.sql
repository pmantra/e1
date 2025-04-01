-- migrate:up transaction:false

DROP INDEX CONCURRENTLY IF EXISTS eligibility.trgm_idx_member_unique_corp_id;

-- migrate:down transaction:false

CREATE INDEX CONCURRENTLY trgm_idx_member_unique_corp_id ON eligibility.member USING gin (unique_corp_id public.gin_trgm_ops);
