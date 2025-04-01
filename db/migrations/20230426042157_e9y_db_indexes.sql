-- migrate:up

CREATE EXTENSION IF NOT EXISTS btree_gin WITH SCHEMA public;
COMMENT ON EXTENSION btree_gin IS 'support for indexing common datatypes in GIN';

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;
COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams';

CREATE INDEX idx_address_country ON eligibility.member_address (country_code);
CREATE INDEX idx_member_name ON eligibility.member USING gin (first_name, last_name);

-- migrate:down

DROP EXTENSION IF EXISTS btree_gin;
DROP EXTENSION IF EXISTS pg_trgm;

DROP INDEX IF EXISTS eligibility.idx_address_country;
DROP INDEX IF EXISTS eligibility.idx_member_name;


