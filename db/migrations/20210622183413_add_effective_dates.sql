-- migrate:up
CREATE OR REPLACE function eligibility.default_range() RETURNS daterange AS $$
    SELECT daterange(CURRENT_DATE, null, '[]');
$$ LANGUAGE sql IMMUTABLE;

CREATE TYPE eligibility.id_to_range AS ("id" int, "range" daterange);

ALTER TABLE eligibility.member
    -- https://www.postgresql.org/docs/12/rangetypes.html
    ADD COLUMN IF NOT EXISTS effective_range daterange NOT NULL DEFAULT eligibility.default_range();

-- migrate:down

ALTER TABLE eligibility.member DROP COLUMN IF EXISTS effective_range;
DROP function IF EXISTS eligibility.default_range();
DROP TYPE IF EXISTS eligibility.id_to_range;
