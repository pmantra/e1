-- Queries pertaining to the `eligibility.member` table.


-- name: all
-- Get all member records.
SELECT * FROM eligibility.member_versioned;

-- name: all_historical
-- Get all member records.
SELECT * FROM eligibility.member_versioned_historical;

-- name: all_historical_addresses
-- Get all historical addresses
SELECT * FROM eligibility.member_address_versioned_historical;

-- name: get^
-- Get an individual member record;
SELECT * FROM eligibility.member_versioned WHERE id = :id;

-- name: get_for_org
-- Get all the member records for a given organization ID.
SELECT * FROM eligibility.member_versioned WHERE organization_id = :organization_id;

-- name: get_count_for_org$
-- Get the current count of member records for a given org.
SELECT count(id) FROM eligibility.member_versioned WHERE organization_id = :organization_id;

-- name: get_counts_for_orgs
-- Get the current count of member records for a series of orgs.
SELECT organization_id, count(id) FROM eligibility.member_versioned
WHERE organization_id = any(:organization_ids)
GROUP BY organization_id;

-- name: get_for_file
-- Get all the member records for a given file ID.
SELECT * FROM eligibility.member_versioned WHERE file_id = :file_id;

-- name: get_for_files
-- Get all the member records for a given file ID.
SELECT * FROM eligibility.member_versioned WHERE file_id = ANY(:file_ids) ORDER BY file_id;

-- name: get_count_for_file$
-- Get the current count of member records for a given org.
SELECT count(id) FROM eligibility.member_versioned WHERE file_id = :file_id;

-- name: get_by_dob_and_email
-- Get a member record using the "primary" verification method. (previously get_by_primary_verification)
-- 1. Find all eligible/non-eligible records that match the criteria, then rank them by groupings
-- 2. Then query for a record where the latest (ranked.rank = 1) is valid
SELECT * FROM (
    SELECT mv.*, RANK()
    OVER (PARTITION BY mv.organization_id, mv.unique_corp_id, mv.dependent_id, mv.email, mv.date_of_birth ORDER BY mv.file_id DESC, mv.updated_at DESC, mv.created_at DESC, mv.id DESC)
    FROM eligibility.member_versioned mv
    WHERE email = :email
        AND date_of_birth = :date_of_birth
    ) AS ranked
WHERE ranked.effective_range @> CURRENT_DATE
  AND ranked.rank = 1;

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
SELECT * FROM (
    SELECT mv.*, RANK()
    OVER (PARTITION BY mv.organization_id, mv.unique_corp_id, mv.dependent_id, mv.first_name, mv.last_name, mv.date_of_birth, mv.work_state ORDER BY mv.file_id DESC, mv.updated_at DESC, mv.created_at DESC, mv.id DESC)
    FROM eligibility.member_versioned mv
    INNER JOIN params ON
        mv.first_name = params.first_name
        AND mv.last_name = params.last_name
        AND mv.date_of_birth = params.date_of_birth
        AND (params.work_state IS null OR mv.work_state = params.work_state)
    ) AS ranked
WHERE ranked.effective_range @> CURRENT_DATE
    AND ranked.rank = 1;

-- name: get_by_dob_name_and_work_state
-- Get a member record by dob, name and work state(required)
-- differe from get_by_secondary_verification: work_state is not empty or null
SELECT * FROM (
    SELECT mv.*, RANK()
    OVER (PARTITION BY mv.organization_id, mv.unique_corp_id, mv.dependent_id, mv.first_name, mv.last_name, mv.date_of_birth, mv.work_state ORDER BY mv.file_id DESC, mv.updated_at DESC, mv.created_at DESC, mv.id DESC)
    FROM eligibility.member_versioned mv
    WHERE
        mv.first_name = :first_name
        AND mv.last_name = :last_name
        AND mv.date_of_birth = :date_of_birth
        AND mv.work_state = :work_state
    ) AS ranked
WHERE ranked.effective_range @> CURRENT_DATE
    AND ranked.rank = 1;

-- name: get_by_tertiary_verification
-- Get a member using the "tertiary" verification method of unique_corp_id and date_of_birth
WITH params as (
    SELECT :date_of_birth::date as date_of_birth, :unique_corp_id::text as unique_corp_id
)
SELECT * FROM (
    SELECT mv.*, RANK()
    OVER (PARTITION BY mv.organization_id, mv.unique_corp_id, mv.date_of_birth ORDER BY mv.file_id DESC, mv.updated_at DESC, mv.created_at DESC, mv.id DESC)
    FROM eligibility.member_versioned mv
    INNER JOIN params ON
        mv.date_of_birth = params.date_of_birth
        AND
        mv.unique_corp_id = params.unique_corp_id
    ) AS ranked
WHERE ranked.effective_range @> CURRENT_DATE
    AND ranked.rank = 1
LIMIT 1;

-- name: get_by_any_verification^
-- Get a member record using either primary or secondary verification.
SELECT * FROM (
    SELECT mv.*, RANK()
    OVER (PARTITION BY mv.organization_id, mv.unique_corp_id, mv.email, mv.date_of_birth, mv.first_name, mv.last_name, mv.work_state ORDER BY mv.file_id DESC, mv.updated_at DESC, mv.created_at DESC, mv.id DESC)
    FROM eligibility.member_versioned mv
    WHERE
        (
            email = :email
            AND date_of_birth = :date_of_birth
        )
            OR
        (
            first_name = :first_name
            AND last_name = :last_name
            AND work_state = :work_state
            AND date_of_birth = :date_of_birth
        )

    ) AS ranked
WHERE ranked.effective_range @> CURRENT_DATE
  AND ranked.rank = 1
LIMIT 1;

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
    SELECT mv.*, RANK()
    OVER (PARTITION BY mv.organization_id, mv.unique_corp_id, mv.dependent_id, mv.first_name, mv.last_name, mv.date_of_birth ORDER BY mv.file_id DESC, mv.updated_at DESC, mv.created_at DESC, mv.id DESC)
    FROM eligibility.member_versioned mv
    INNER JOIN params ON
        mv.first_name = params.first_name
        AND mv.last_name = params.last_name
        AND mv.date_of_birth = params.date_of_birth
    ) AS ranked
WHERE ranked.effective_range @> CURRENT_DATE
    AND ranked.rank = 1;


-- name: get_by_client_specific_verification^
-- Query for member records with the given organization ID, unique_corp_id, and DoB
SELECT * FROM eligibility.member_versioned
WHERE organization_id = :organization_id
  AND unique_corp_id = :unique_corp_id
  AND date_of_birth = :date_of_birth
  AND effective_range @> CURRENT_DATE
ORDER BY id DESC
LIMIT 1;

-- name: get_by_org_identity^
-- Query for a member record with the full unique constraint.
SELECT * FROM eligibility.member_versioned
WHERE organization_id = :organization_id
  AND unique_corp_id = :unique_corp_id
  AND dependent_id = :dependent_id
ORDER BY id DESC
LIMIT 1;

-- name: get_by_org_email
-- Query for member records with the given organization ID and email.
-- TODO: How to handle updated records?
SELECT * FROM eligibility.member_versioned
WHERE organization_id = :organization_id AND email = :email;

-- name: get_difference_by_org_corp_id
-- Query for all entries which are *not* provided within the identities.
-- TODO: How to handle updated records?
WITH missing AS (
    WITH existing AS (
        SELECT unnest(:corp_ids::text[]) as unique_corp_id
    )
    SELECT id from eligibility.member_versioned
    WHERE organization_id = :organization_id
        EXCEPT
    SELECT id from eligibility.member_versioned
    INNER JOIN existing ON member_versioned.unique_corp_id = existing.unique_corp_id
)
SELECT member_versioned.* from eligibility.member_versioned
INNER JOIN missing ON member_versioned.id = missing.id;

-- name: get_by_name_and_date_of_birth
-- Get member records using first_name, last_name and date_of_birth and filter out inactive orgs
SELECT m_final.* FROM eligibility.member_versioned m_final
    INNER JOIN (
        SELECT DISTINCT ON (m.organization_id) m.organization_id, m.id
            FROM eligibility.member_versioned m
            INNER JOIN eligibility.configuration c ON m.organization_id = c.organization_id
            WHERE m.effective_range @> CURRENT_DATE
            AND c.activated_at IS NOT NULL
            AND c.activated_at <= CURRENT_DATE
            AND (c.terminated_at IS NULL OR c.terminated_at > CURRENT_DATE)
            AND LOWER(m.first_name) = LOWER(:first_name)
            AND LOWER(m.last_name) = LOWER(:last_name)
            AND m.date_of_birth = :date_of_birth
            ORDER BY m.organization_id, m.updated_at, m.id desc
    ) m_filtered ON m_final.id = m_filtered.id;


-- name: get_wallet_enablement^
-- Query for a member's wallet enablement data, if they have any.
SELECT
    mv.id as member_id,
    mv.organization_id as organization_id,
    mv.unique_corp_id as unique_corp_id,
    mv.dependent_id as dependent_id,
    mv.record->>'insurance_plan' as insurance_plan,
    bool(mv.record->>'wallet_enabled') as enabled,
    coalesce(nullif(mv.record->>'wallet_eligibility_start_date', '')::date, nullif(mv.record->>'employee_start_date', '')::date)::date as start_date,
    nullif(mv.record->>'employee_eligibility_date', '')::date as eligibility_date,
    mv.effective_range as effective_range,
    m.created_at as created_at,
    mv.updated_at as updated_at
FROM eligibility.member_versioned mv
    LEFT JOIN eligibility.member m
    ON mv.organization_id=m.organization_id
    AND mv.unique_corp_id=m.unique_corp_id
    AND mv.dependent_id=m.dependent_id
WHERE
    mv.id=:member_id
ORDER BY mv.file_id DESC, mv.updated_at DESC
LIMIT 1;



-- name: get_wallet_enablement_by_identity^
-- Query for a member's wallet enablement data via an org identity, if that have any.
SELECT
    mv.id as member_id,
    mv.organization_id as organization_id,
    mv.unique_corp_id as unique_corp_id,
    mv.dependent_id as dependent_id,
    mv.record->>'insurance_plan' as insurance_plan,
    bool(mv.record->>'wallet_enabled') as enabled,
    coalesce(nullif(mv.record->>'wallet_eligibility_start_date', '')::date, nullif(mv.record->>'employee_start_date', '')::date)::date as start_date,
    nullif(mv.record->>'employee_eligibility_date', '')::date as eligibility_date,
    mv.effective_range as effective_range,
    m.created_at as created_at,
    mv.updated_at as updated_at
FROM eligibility.member_versioned mv
    LEFT JOIN eligibility.member m
    ON mv.organization_id=m.organization_id
    AND mv.unique_corp_id=m.unique_corp_id
    AND mv.dependent_id=m.dependent_id
WHERE
    mv.organization_id=:organization_id
    AND mv.unique_corp_id=:unique_corp_id
    AND mv.dependent_id=:dependent_id
ORDER BY mv.file_id DESC, mv.updated_at DESC
LIMIT 1;

-- name: get_address_for_member^
-- Query for a member's address data, if they have any.
SELECT * FROM eligibility.member_address_versioned
WHERE member_id = :member_id
order by id DESC
LIMIT 1;

-- name: get_members_for_unique_corp_id
-- Query for all members' records that relate to given unique corp id
SELECT *
FROM eligibility.member_versioned
WHERE unique_corp_id = :unique_corp_id;

-- name: get_unique_corp_id_for_member^
-- Query for the unique corp id given a member id
SELECT
    unique_corp_id
FROM eligibility.member_versioned
WHERE id = :member_id;

-- name: get_members_for_pre_verification
-- Query for member_versioned records that still need to be pre-verified by org
SELECT m.*
FROM eligibility.member_versioned m
LEFT JOIN eligibility.member_verification mv
ON m.id=mv.member_id
WHERE
    m.organization_id=:organization_id
AND mv.member_id IS NULL
AND m.pre_verified=FALSE
AND (
            m.effective_range @> CURRENT_DATE
        OR
            m.effective_range @> (CURRENT_DATE + INTERVAL '1 day')::date
    );



-- name: get_values_to_hash_for_org
-- Query for member_versioned records that need to be hashed for an org and don't have a verification associated
WITH rows_to_hash as (
SELECT
    mv.*,
    mva.id as address_id,
    mva.address_1,
    mva.address_2,
    mva.city,
    mva.state,
    mva.postal_code,
    mva.postal_code_suffix,
    mva.country_code
    FROM eligibility.member_versioned mv
    LEFT JOIN eligibility.member_address_versioned mva
        ON mv.id = mva.member_id
    LEFT JOIN eligibility.member_verification m_verif
        ON mv.id = m_verif.member_id
    WHERE mv.file_id IS NULL
        AND mv.hash_value IS NULL
        AND m_verif.id IS NULL
        AND mv.organization_id = :organization_id
    LIMIT 100000
)
SELECT * FROM rows_to_hash;

-- name: get_other_user_ids_in_family
-- Query for the user_ids associated to a user's "family"
WITH family_identifier AS (
    SELECT
        m.organization_id as organization_id,
        m.unique_corp_id as unique_corp_id,
        v.user_id as user_id
    FROM eligibility.verification v
    INNER JOIN eligibility.member_verification mv
    ON v.id=mv.verification_id
    INNER JOIN eligibility.member_versioned m
    ON mv.member_id=m.id
    WHERE
        v.user_id = :user_id
        -- Active verifications only
        AND (
            v.deactivated_at IS NULL
                OR
            v.deactivated_at > CURRENT_DATE
        )
    ORDER BY
        v.verified_at DESC,
        m.created_at DESC
    LIMIT 1
)
SELECT DISTINCT
    v.user_id
FROM eligibility.verification v
INNER JOIN eligibility.member_verification mv
ON v.id=mv.verification_id
INNER JOIN eligibility.member_versioned m
ON mv.member_id=m.id
INNER JOIN family_identifier f_id
ON f_id.organization_id=m.organization_id
AND f_id.unique_corp_id=m.unique_corp_id
AND f_id.user_id<>v.user_id
WHERE
-- Active verifications only
v.deactivated_at IS NULL
    OR
v.deactivated_at > CURRENT_DATE;

-- name: get_by_email_and_name
-- Get member records using email, first_name and last_name and filter out inactive orgs
SELECT m_final.*
FROM eligibility.member_versioned m_final
INNER JOIN (
    SELECT DISTINCT ON (m.organization_id)
        m.organization_id,
        m.id
    FROM eligibility.member_versioned m
    INNER JOIN eligibility.configuration c ON m.organization_id = c.organization_id
    WHERE m.effective_range @> CURRENT_DATE
    AND c.activated_at IS NOT NULL
    AND c.activated_at <= CURRENT_DATE
    AND (c.terminated_at IS NULL OR c.terminated_at > CURRENT_DATE)
    AND m.email = :email
    AND LOWER(m.first_name) = LOWER(:first_name)
    AND LOWER(m.last_name) = LOWER(:last_name)
    ORDER BY m.organization_id, m.updated_at DESC, m.id DESC
) m_filtered ON m_final.organization_id = m_filtered.organization_id AND m_final.id = m_filtered.id;

-- name: get_by_member_2^
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
    nullif(:work_state::text, '') AS work_state
)
SELECT * FROM (
    SELECT mv.*, RANK()
    OVER (PARTITION BY mv.organization_id, mv.unique_corp_id, mv.email, mv.date_of_birth, mv.first_name, mv.last_name, mv.work_state ORDER BY mv.file_id DESC, mv.updated_at DESC, mv.created_at DESC, mv.id DESC)
    FROM eligibility.member_versioned mv
    INNER JOIN params ON
        mv.organization_id = params.organization_id
        AND mv.email = params.email
        AND mv.first_name = params.first_name
        AND mv.last_name = params.last_name
        AND mv.unique_corp_id = params.unique_corp_id
        AND ( (mv.work_state is null and params.work_state = '') OR (mv.work_state  = params.work_state) )
        AND ( (mv.date_of_birth = '1900-01-01' and params.date_of_birth = '0001-01-01' ) OR (mv.date_of_birth = params.date_of_birth) )
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
SELECT * FROM (
    SELECT mv.*, RANK()
    OVER (PARTITION BY mv.organization_id, mv.unique_corp_id, mv.dependent_id, mv.first_name, mv.last_name, mv.date_of_birth ORDER BY mv.file_id DESC, mv.updated_at DESC, mv.created_at DESC, mv.id DESC)
    FROM eligibility.member_versioned mv
    INNER JOIN params ON
        mv.first_name = params.first_name
        AND mv.last_name = params.last_name
        AND mv.date_of_birth = params.date_of_birth
    ) AS ranked
WHERE ranked.effective_range @> CURRENT_DATE
    AND ranked.rank = 1;

-- name: get_by_name_and_unique_corp_id^
WITH params AS (
    SELECT
        :first_name::text AS first_name,
        :last_name::text AS last_name,
        :unique_corp_id::text AS unique_corp_id
)
SELECT ranked.*
FROM (
    SELECT
        mv.*,
        RANK() OVER (
            PARTITION BY mv.organization_id, mv.unique_corp_id, mv.first_name, mv.last_name
            ORDER BY mv.file_id DESC, mv.updated_at DESC, mv.created_at DESC, mv.id DESC
        ) AS rank
    FROM eligibility.member_versioned mv
    INNER JOIN params
        ON mv.first_name = params.first_name
        AND mv.last_name = params.last_name
        AND mv.unique_corp_id = params.unique_corp_id
) AS ranked
WHERE
    ranked.effective_range @> CURRENT_DATE
    AND ranked.rank = 1
LIMIT 1;

-- name: get_by_date_of_birth_and_unique_corp_id^
WITH params AS (
    SELECT
        :date_of_birth::date AS date_of_birth,
        :unique_corp_id::text AS unique_corp_id
)
SELECT ranked.*
FROM (
    SELECT
        mv.*,
        RANK() OVER (
            PARTITION BY mv.organization_id, mv.unique_corp_id, mv.first_name, mv.last_name
            ORDER BY mv.file_id DESC, mv.updated_at DESC, mv.created_at DESC, mv.id DESC
        ) AS rank
    FROM eligibility.member_versioned mv
    INNER JOIN params
        ON mv.date_of_birth = params.date_of_birth
        AND mv.unique_corp_id = params.unique_corp_id
) AS ranked
WHERE
    ranked.effective_range @> CURRENT_DATE
    AND ranked.rank = 1
LIMIT 1;
