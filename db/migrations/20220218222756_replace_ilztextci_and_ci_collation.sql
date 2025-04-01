-- migrate:up
DROP INDEX IF EXISTS eligibility.uidx_member_identity;
DROP INDEX IF EXISTS eligibility.idx_unique_corp_id;
DROP MATERIALIZED VIEW IF EXISTS eligibility.wallet_enablement CASCADE;

ALTER TABLE eligibility.member
    ALTER COLUMN unique_corp_id TYPE eligibility.ilztext,
    ALTER COLUMN dependent_id TYPE eligibility.citext;

CREATE INDEX IF NOT EXISTS idx_unique_corp_id ON eligibility.member (
    ltrim(lower(unique_corp_id), '0')

    text_pattern_ops
);
CREATE UNIQUE INDEX uidx_member_identity on eligibility.member(
    organization_id,
    ltrim(lower(unique_corp_id), '0'),
    lower(dependent_id)

    text_pattern_ops
);
CREATE MATERIALIZED VIEW IF NOT EXISTS eligibility.wallet_enablement AS (
SELECT
    id as member_id,
    organization_id,
    unique_corp_id,
    dependent_id,
    record->>'insurance_plan' as insurance_plan,
    bool(record->>'wallet_enabled') as enabled,
    coalesce((record->>'employee_start_date')::date, created_at)::date as start_date,
    coalesce((record->>'employee_eligibility_date')::date, created_at)::date as eligibility_date,
    effective_range,
    created_at,
    updated_at
FROM eligibility.member
WHERE bool(record->>'wallet_enabled') is not null
);
CREATE UNIQUE INDEX IF NOT EXISTS uidx_wallet_member_id
    ON eligibility.wallet_enablement(member_id);
CREATE UNIQUE INDEX uidx_wallet_member_identity on eligibility.wallet_enablement(
    organization_id,
    ltrim(lower(unique_corp_id), '0'),
    lower(dependent_id)

    text_pattern_ops
);


-- migrate:down
DROP INDEX IF EXISTS eligibility.uidx_member_identity;
DROP INDEX IF EXISTS eligibility.idx_unique_corp_id;
DROP MATERIALIZED VIEW IF EXISTS eligibility.wallet_enablement CASCADE;

ALTER TABLE eligibility.member
    ALTER COLUMN unique_corp_id TYPE eligibility.ilztextci,
    ALTER COLUMN dependent_id TYPE text COLLATE eligibility.ci;


CREATE INDEX IF NOT EXISTS idx_unique_corp_id ON eligibility.member(
    ltrim(lower(unique_corp_id), '0')
);
CREATE UNIQUE INDEX uidx_member_identity on eligibility.member(
    organization_id,
    ltrim(lower(unique_corp_id), '0'),
    dependent_id
);
CREATE MATERIALIZED VIEW IF NOT EXISTS eligibility.wallet_enablement AS (
SELECT
    id as member_id,
    organization_id,
    unique_corp_id,
    dependent_id,
    record->>'insurance_plan' as insurance_plan,
    bool(record->>'wallet_enabled') as enabled,
    coalesce((record->>'employee_start_date')::date, created_at)::date as start_date,
    coalesce((record->>'employee_eligibility_date')::date, created_at)::date as eligibility_date,
    effective_range,
    created_at,
    updated_at
FROM eligibility.member
WHERE bool(record->>'wallet_enabled') is not null
);
CREATE UNIQUE INDEX IF NOT EXISTS uidx_wallet_member_id
    ON eligibility.wallet_enablement(member_id);
CREATE UNIQUE INDEX uidx_wallet_member_identity on eligibility.wallet_enablement(
    organization_id,
    ltrim(unique_corp_id, '0'),
    dependent_id
);
