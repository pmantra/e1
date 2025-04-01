-- migrate:up
CREATE OR REPLACE FUNCTION eligibility.is_active(date_range daterange) RETURNS bool IMMUTABLE AS $$
    BEGIN
        RETURN LOWER(date_range) <= CURRENT_DATE AND (UPPER(date_range) IS NULL OR UPPER(date_range) >= CURRENT_DATE);
    END;
$$ LANGUAGE plpgsql;

-- migrate:down
DROP FUNCTION IF EXISTS eligibility.is_active
