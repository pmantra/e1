-- migrate:up transaction:false
DROP INDEX CONCURRENTLY IF EXISTS eligibility.idx_member_custom_attributes;



-- migrate:down transaction:false

CREATE INDEX CONCURRENTLY idx_member_custom_attributes ON eligibility.member USING gin (id, custom_attributes);

