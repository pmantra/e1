-- migrate:up transaction:false

DROP INDEX CONCURRENTLY IF EXISTS eligibility.trgm_idx_member_first_name;

-- migrate:down transaction:false

CREATE INDEX CONCURRENTLY trgm_idx_member_first_name ON eligibility.member USING gin (first_name public.gin_trgm_ops);
