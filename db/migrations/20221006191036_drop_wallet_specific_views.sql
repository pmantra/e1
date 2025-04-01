-- migrate:up
DROP MATERIALIZED VIEW IF EXISTS eligibility.wallet_enablement CASCADE;


-- migrate:down
DROP MATERIALIZED VIEW IF EXISTS eligibility.wallet_enablement CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS eligibility.wallet_enablement AS (
SELECT
    id as member_id,
    organization_id,
    unique_corp_id,
    dependent_id,
    record->>'insurance_plan' as insurance_plan,
    bool(record->>'wallet_enabled') as enabled,
    coalesce(nullif(record->>'wallet_eligibility_start_date', '')::date, nullif(record->>'employee_start_date', '')::date)::date as start_date,
    coalesce(nullif(record->>'employee_eligibility_date', '')::date, created_at)::date as eligibility_date,
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

