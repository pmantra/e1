-- migrate:up transaction:false

DROP INDEX CONCURRENTLY IF EXISTS eligibility.trgm_idx_member_email;

-- migrate:down transaction:false

CREATE INDEX CONCURRENTLY trgm_idx_member_email ON eligibility.member USING gin (email public.gin_trgm_ops);