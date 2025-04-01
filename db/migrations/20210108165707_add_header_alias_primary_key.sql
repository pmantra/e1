-- migrate:up

ALTER TABLE eligibility.header_alias ADD COLUMN IF NOT EXISTS id BIGSERIAL PRIMARY KEY;
ALTER TABLE eligibility.header_alias DROP CONSTRAINT IF EXISTS org_header_alias;
-- We can assume that the earlier entry is outdated, so we should delete it.
-- This is okay and doesn't need to be undone, we should never have duplicated headers
-- for an org.
DELETE FROM eligibility.header_alias WHERE id = ANY (
    SELECT UNNEST(
        ARRAY (
            SELECT min(id)
            FROM eligibility.header_alias
            GROUP BY (organization_id, header)
            HAVING count(*) > 1
        )
    )
);
CREATE UNIQUE INDEX IF NOT EXISTS uidx_org_header ON eligibility.header_alias (organization_id, header);
ALTER TABLE eligibility.header_alias ADD CONSTRAINT org_header UNIQUE USING INDEX uidx_org_header;

-- migrate:down

ALTER TABLE eligibility.header_alias DROP CONSTRAINT IF EXISTS org_header;
DROP INDEX IF EXISTS eligibility.idx_org_header;
CREATE UNIQUE INDEX IF NOT EXISTS uidx_org_header_alias ON eligibility.header_alias (organization_id, header, alias);
ALTER TABLE eligibility.header_alias ADD CONSTRAINT org_header_alias UNIQUE USING INDEX uidx_org_header_alias;
ALTER TABLE eligibility.header_alias DROP CONSTRAINT IF EXISTS org_header;
ALTER TABLE eligibility.header_alias DROP CONSTRAINT IF EXISTS header_alias_pkey;
ALTER TABLE eligibility.header_alias DROP COLUMN IF EXISTS id;
