-- migrate:up
CREATE MATERIALIZED VIEW IF NOT EXISTS eligibility.wallet_enablement AS (
SELECT
    id as member_id,
    organization_id,
    unique_corp_id,
    dependent_id,
    record->>'insurance_plan' as insurance_plan,
    bool(record->>'wallet_enabled') as enabled,
    (record->>'employee_start_date')::date as start_date,
    (record->>'employee_eligibility_date')::date as eligibility_date,
    effective_range,
    created_at,
    updated_at
FROM eligibility.member
WHERE bool(record->>'wallet_enabled') is not null
);
CREATE UNIQUE INDEX IF NOT EXISTS uidx_wallet_member_id
    ON eligibility.wallet_enablement(member_id);
CREATE UNIQUE INDEX IF NOT EXISTS uidx_wallet_member_identity
    ON eligibility.wallet_enablement(
        organization_id, ltrim(unique_corp_id, '0'), dependent_id
    );

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
    (record->>'employee_start_date')::date as start_date,
    (record->>'employee_eligibility_date')::date as eligibility_date,
    effective_range,
    created_at,
    updated_at
FROM eligibility.member
WHERE bool(record->>'wallet_enabled') is not null
);
CREATE UNIQUE INDEX IF NOT EXISTS uidx_wallet_member_id
    ON eligibility.wallet_enablement(member_id);
CREATE UNIQUE INDEX IF NOT EXISTS uidx_wallet_member_identity
    ON eligibility.wallet_enablement(
        organization_id, ltrim(unique_corp_id, '0'), dependent_id
    );
