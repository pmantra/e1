-- Queries pertaining to the `eligibility.member_2` table.

-- name: get_by_dob_and_email
-- Get a member record using the "primary" verification method. ((previously get_by_primary_verification)
WITH params as (
  SELECT
    :email::text AS email,
    :date_of_birth::date AS date_of_birth
)
SELECT * FROM eligibility.member_2
INNER JOIN params ON
    member_2.email = params.email
    AND member_2.date_of_birth = params.date_of_birth
    AND effective_range @> CURRENT_DATE;

-- name: get_by_secondary_verification
-- Get a member record using the "secondary" verification method.
WITH params as (
  -- NOTE: there's currently a bug in aiosql which causes multiple references
  -- to the same parameter to not be parsed correctly,
  -- so we need to select our params into a temporary table first,
  -- then reference them in our comparison using a JOIN
  SELECT
    :first_name::text AS first_name,
    :last_name::text AS last_name,
    :date_of_birth::date AS date_of_birth,
    nullif(:work_state::text, '') AS work_state
)
SELECT member_2.* FROM eligibility.member_2
INNER JOIN params ON
    member_2.first_name = params.first_name
    AND member_2.last_name = params.last_name
    AND member_2.date_of_birth = params.date_of_birth
    AND (params.work_state IS null OR member_2.work_state = params.work_state)
    AND member_2.effective_range @> CURRENT_DATE;

-- name: get_by_tertiary_verification
-- Get a member using the "tertiary" verification method of unique_corp_id and date_of_birth
WITH params as (
    SELECT :date_of_birth::date as date_of_birth, :unique_corp_id::text as unique_corp_id
)
SELECT DISTINCT member_2.* from eligibility.member_2
LEFT JOIN eligibility.organization_external_id oei
    ON member_2.organization_id = oei.organization_id
INNER JOIN params ON
    member_2.date_of_birth = params.date_of_birth
    AND member_2.effective_range @> CURRENT_DATE
    AND (
        member_2.unique_corp_id = params.unique_corp_id
        -- FIXME [Optum]: This is a temporary HACK to mitigate high failure rates for health-plans.
        --   As of this writing, entries which we received from Optum are lacking a 1-to-3-digit
        --   prefix which members are providing during sign-up. An analysis of the corp ids we have
        --   from Optum shows that 99.9% of entries are 9 digits long.
        --   The heuristic:
        --       If we have no matches and were provided a unique_corp_id
        --       and prospective record is from optum,
        --       try extracting 9 digits from the right side of the provided corp id to join.
        OR
            CASE
                --  TODO: This needs to be updated to reference oei.data_provider_organization_id once we have one for optum
                WHEN oei.source = 'optum'
                   -- Optum's extraction of the member ID from the prefix is faulty,
                   --  They appear to simply trim the first three characters,
                   --  so some member IDs are left with 1-3 alpha characters.
                   --  This is the most permissive way to capture them.
                   THEN member_2.unique_corp_id = right(params.unique_corp_id, 9)
                ELSE false
            END
        );

-- name: get_by_client_specific_verification^
-- Query for member records with the given organization ID, unique_corp_id, and DoB
SELECT * FROM eligibility.member_2
WHERE organization_id = :organization_id
  AND unique_corp_id = :unique_corp_id
  AND date_of_birth = :date_of_birth
  AND effective_range @> CURRENT_DATE
ORDER BY id DESC
LIMIT 1;

-- name: get_by_org_identity^
-- Query for a member record with the full unique constraint.
SELECT * FROM eligibility.member_2
WHERE organization_id = :organization_id
  AND unique_corp_id = :unique_corp_id
  AND dependent_id = :dependent_id;

-- name: get^
-- Get an individual member record;
SELECT * FROM eligibility.member_2 WHERE id = :id;

-- name: get_wallet_enablement_by_identity^
-- Query for a member's wallet enablement data via an org identity, if that have any.
SELECT
    id as member_id,
    organization_id,
    unique_corp_id,
    dependent_id,
    record->>'insurance_plan' as insurance_plan,
    bool(record->>'wallet_enabled') as enabled,
    coalesce(nullif(record->>'wallet_eligibility_start_date', '')::date, nullif(record->>'employee_start_date', '')::date)::date as start_date,
    nullif(record->>'employee_eligibility_date', '')::date as eligibility_date,
    effective_range as effective_range,
    created_at as created_at,
    updated_at as updated_at
FROM eligibility.member_2
WHERE
    organization_id=:organization_id
    AND unique_corp_id=:unique_corp_id
    AND dependent_id=:dependent_id
ORDER BY updated_at DESC
LIMIT 1;

-- name: get_wallet_enablement^
-- Query for a member's wallet enablement data, if they have any.
SELECT
    id as member_id,
    organization_id,
    unique_corp_id,
    dependent_id,
    record->>'insurance_plan' as insurance_plan,
    bool(record->>'wallet_enabled') as enabled,
    coalesce(nullif(record->>'wallet_eligibility_start_date', '')::date, nullif(record->>'employee_start_date', '')::date)::date as start_date,
    nullif(record->>'employee_eligibility_date', '')::date as eligibility_date,
    effective_range as effective_range,
    created_at as created_at,
    updated_at as updated_at
FROM eligibility.member_2
WHERE
    id=:member_id;

-- name: get_other_user_ids_in_family
-- Query for the user_ids associated to a user's "family"
WITH family_identifier AS (
    SELECT
        organization_id,
        unique_corp_id,
        user_id
    FROM eligibility.verification_2
    WHERE
        user_id = :user_id
        -- Active verifications only
        AND (
            deactivated_at IS NULL
                OR
            deactivated_at > CURRENT_DATE
        )
    ORDER BY
        verified_at DESC,
        created_at DESC
    LIMIT 1
)
SELECT DISTINCT
    v.user_id
FROM eligibility.verification_2 v
INNER JOIN family_identifier f_id
ON f_id.organization_id=v.organization_id
AND f_id.unique_corp_id=v.unique_corp_id
AND f_id.user_id<>v.user_id
WHERE
-- Active verifications only
v.deactivated_at IS NULL
    OR
v.deactivated_at > CURRENT_DATE;

-- name: get_by_email_and_name
-- Get member records using email, first_name and last_name and filter out inactive orgs
SELECT m.*
FROM eligibility.member_2 m
INNER JOIN eligibility.configuration c ON m.organization_id = c.organization_id
WHERE m.effective_range @> CURRENT_DATE
    AND c.activated_at IS NOT NULL
    AND c.activated_at <= CURRENT_DATE
    AND (c.terminated_at IS NULL OR c.terminated_at > CURRENT_DATE)
    AND m.email = :email
    AND LOWER(m.first_name) = LOWER(:first_name)
    AND LOWER(m.last_name) = LOWER(:last_name)
ORDER BY m.organization_id, m.updated_at DESC, m.id DESC;

-- name: get_by_overeligibility
WITH params as (
  -- NOTE: there's currently a bug in aiosql which causes multiple references
  -- to the same parameter to not be parsed correctly,
  -- so we need to select our params into a temporary table first,
  -- then reference them in our comparison using a JOIN
  SELECT
    :first_name::text AS first_name,
    :last_name::text AS last_name,
    :date_of_birth::date AS date_of_birth
)
SELECT * FROM (
    SELECT m2.*, RANK()
    OVER (PARTITION BY m2.organization_id, m2.unique_corp_id, m2.dependent_id, m2.first_name, m2.last_name, m2.date_of_birth ORDER BY m2.updated_at DESC, m2.created_at DESC, m2.id DESC)
    FROM eligibility.member_2 m2
    INNER JOIN params ON
        m2.first_name = params.first_name
        AND m2.last_name = params.last_name
        AND m2.date_of_birth = params.date_of_birth
    ) AS ranked
WHERE ranked.effective_range @> CURRENT_DATE
    AND ranked.rank = 1;

-- name: get_by_member_versioned^
-- Get a member 1 record using member 2 record.
-- Default value for date_of_birth is different so add some special handling.
WITH params as (
  -- NOTE: there's currently a bug in aiosql which causes multiple references
  -- to the same parameter to not be parsed correctly,
  -- so we need to select our params into a temporary table first,
  -- then reference them in our comparison using a JOIN
  SELECT
    :organization_id::int AS organization_id,
    :email::text AS email,
    :first_name::text AS first_name,
    :last_name::text AS last_name,
    :unique_corp_id::text AS unique_corp_id,
    :date_of_birth::date AS date_of_birth,
    :work_state::text AS work_state
)
SELECT * FROM (
    SELECT m2.*, RANK()
    OVER (PARTITION BY m2.organization_id, m2.unique_corp_id, m2.email, m2.date_of_birth, m2.first_name, m2.last_name, m2.work_state ORDER BY m2.updated_at DESC, m2.created_at DESC, m2.id DESC)
    FROM eligibility.member_2 m2
    INNER JOIN params ON
        m2.organization_id = params.organization_id
        AND m2.email = params.email
        AND m2.first_name = params.first_name
        AND m2.last_name = params.last_name
        AND m2.unique_corp_id = params.unique_corp_id
        AND (coalesce(m2.work_state, '') = coalesce(params.work_state, ''))
        AND ( (m2.date_of_birth = '1900-01-01' and params.date_of_birth = '0001-01-01' ) OR (m2.date_of_birth = params.date_of_birth) )
    ) AS ranked
WHERE ranked.rank = 1
LIMIT 1;

-- name: get_all_by_name_and_date_of_birth
WITH params as (
  SELECT
    :first_name::text AS first_name,
    :last_name::text AS last_name,
    :date_of_birth::date AS date_of_birth
)
SELECT m2.*
FROM eligibility.member_2 m2
INNER JOIN params ON
    m2.first_name = params.first_name
    AND m2.last_name = params.last_name
    AND m2.date_of_birth = params.date_of_birth
WHERE
    m2.effective_range @> CURRENT_DATE
ORDER BY m2.updated_at DESC, m2.created_at DESC, m2.id DESC

-- name: get_by_dob_name_and_work_state
-- Get a member record by dob, name and work state(required)
-- differe from get_by_secondary_verification: work_state is not empty or null
WITH params as (
  SELECT
    :first_name::text AS first_name,
    :last_name::text AS last_name,
    :date_of_birth::date AS date_of_birth,
    :work_state::text AS work_state
)
SELECT m2.*
FROM eligibility.member_2 m2
INNER JOIN params ON
    m2.first_name = params.first_name
    AND m2.last_name = params.last_name
    AND m2.date_of_birth = params.date_of_birth
    AND m2.work_state = params.work_state
WHERE
    m2.effective_range @> CURRENT_DATE
ORDER BY m2.updated_at DESC, m2.created_at DESC, m2.id DESC

-- name: get_by_name_and_unique_corp_id^
WITH params AS (
    SELECT
        :first_name::text AS first_name,
        :last_name::text AS last_name,
        :unique_corp_id::text AS unique_corp_id
)
SELECT m2.*
FROM eligibility.member_2 m2
INNER JOIN params ON
    m2.first_name = params.first_name
    AND m2.last_name = params.last_name
    AND m2.unique_corp_id = params.unique_corp_id
WHERE
    m2.effective_range @> CURRENT_DATE
ORDER BY m2.updated_at DESC, m2.created_at DESC, m2.id DESC
LIMIT 1;

-- name: get_by_date_of_birth_and_unique_corp_id^
WITH params AS (
    SELECT
        :date_of_birth::date AS date_of_birth,
        :unique_corp_id::text AS unique_corp_id
)
SELECT m2.*
FROM eligibility.member_2 m2
INNER JOIN params ON
    m2.date_of_birth = params.date_of_birth
    AND m2.unique_corp_id = params.unique_corp_id
WHERE
    m2.effective_range @> CURRENT_DATE
ORDER BY m2.updated_at DESC, m2.created_at DESC, m2.id DESC
LIMIT 1;

