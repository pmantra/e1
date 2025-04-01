-- migrate:up transaction:false

DROP INDEX CONCURRENTLY IF EXISTS eligibility.idx_member_name;

-- migrate:down transaction:false

CREATE INDEX CONCURRENTLY idx_member_name ON eligibility.member USING gin (first_name, last_name);