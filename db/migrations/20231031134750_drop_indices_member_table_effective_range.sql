-- migrate:up transaction:false

DROP INDEX CONCURRENTLY IF EXISTS eligibility.idx_member_effective_range;


-- migrate:down transaction:false

CREATE INDEX CONCURRENTLY idx_member_effective_range ON eligibility.member USING gist (effective_range);