-- migrate:up
SET search_path TO eligibility;

DROP FUNCTION IF EXISTS get_header_mapping(header_alias);

CREATE OR REPLACE FUNCTION get_header_mapping(bigint) RETURNS jsonb AS
$$
    WITH headers AS (
        SELECT jsonb_object_agg(header, alias) AS headers
        FROM header_alias
        WHERE organization_id = $1
    )
    SELECT (
        CASE WHEN headers.headers IS NULL
            THEN '{}'::jsonb
            ELSE headers.headers END
        )
    FROM headers
$$ LANGUAGE sql IMMUTABLE;

SET search_path TO public;

-- migrate:down
SET search_path TO eligibility;

DROP FUNCTION IF EXISTS get_header_mapping(bigint);

CREATE OR REPLACE FUNCTION get_header_mapping(header_alias) RETURNS jsonb as $$
    SELECT (
        CASE WHEN $1.header IS NOT NULL
            THEN jsonb_build_object($1.header, $1.alias)
            ELSE '{}'::jsonb
        END
    )
$$ LANGUAGE sql immutable;

SET search_path TO public;