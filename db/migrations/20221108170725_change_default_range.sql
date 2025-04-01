-- migrate:up
CREATE OR REPLACE FUNCTION eligibility.default_range() RETURNS daterange
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT daterange((current_date - INTERVAL '1 day')::date, null, '[]');
$$;


-- migrate:down
CREATE OR REPLACE FUNCTION eligibility.default_range() RETURNS daterange
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT daterange(CURRENT_DATE, null, '[]');
$$;

