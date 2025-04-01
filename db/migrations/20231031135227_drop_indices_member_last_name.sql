-- migrate:up transaction:false

DROP INDEX CONCURRENTLY IF EXISTS eligibility.trgm_idx_member_last_name;


-- migrate:down transaction:false

CREATE INDEX CONCURRENTLY trgm_idx_member_last_name ON eligibility.member USING gin (last_name public.gin_trgm_ops);
