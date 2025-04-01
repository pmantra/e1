-- migrate:up transaction:false

DROP INDEX CONCURRENTLY IF EXISTS eligibility.trgm_idx_member_dependent_id;

-- migrate:down transaction:false


CREATE INDEX CONCURRENTLY trgm_idx_member_dependent_id ON eligibility.member USING gin (dependent_id public.gin_trgm_ops);