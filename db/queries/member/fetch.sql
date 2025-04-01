-- Queries pertaining to the `eligibility.member` table.

-- name: all
-- Get all member records.
SELECT * FROM eligibility.member;

-- name: get^
-- Get an individual member record;
SELECT * FROM eligibility.member WHERE id = :id;

-- name: get_for_org
-- Get all the member records for a given organization ID.
SELECT * FROM eligibility.member WHERE organization_id = :organization_id;

-- name: get_count_for_org$
-- Get the current count of member records for a given org.
SELECT count(id) FROM eligibility.member WHERE organization_id = :organization_id;

-- name: get_counts_for_orgs
-- Get the current count of member records for a series of orgs.
SELECT organization_id, count(id) FROM eligibility.member
WHERE organization_id = any(:organization_ids)
GROUP BY organization_id;

-- name: get_for_file
-- Get all the member records for a given file ID.
SELECT * FROM eligibility.member WHERE file_id = :file_id;

-- name: get_for_files
-- Get all the member records for a given file ID.
SELECT * FROM eligibility.member WHERE file_id = ANY(:file_ids) ORDER BY file_id;

-- name: get_count_for_file$
-- Get the current count of member records for a given org.
SELECT count(id) FROM eligibility.member WHERE file_id = :file_id;

-- name: get_by_dob_and_email^
-- Get a member record using the dob and email
-- previously known ad get_by_primary_verification.
SELECT * FROM eligibility.member
WHERE email = :email
  AND date_of_birth = :date_of_birth
  AND effective_range @> CURRENT_DATE;
;

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
SELECT member.* FROM eligibility.member
INNER JOIN params ON
    member.first_name = params.first_name
    AND member.last_name = params.last_name
    AND member.date_of_birth = params.date_of_birth
    AND (params.work_state IS null OR member.work_state = params.work_state)
    AND member.effective_range @> CURRENT_DATE;

-- name: get_by_tertiary_verification
-- Get a member using the "tertiary" verification method of unique_corp_id and date_of_birth
WITH params as (
    SELECT :date_of_birth::date as date_of_birth, :unique_corp_id::text as unique_corp_id
)
SELECT DISTINCT member.* from eligibility.member
LEFT JOIN eligibility.organization_external_id oei
    ON member.organization_id = oei.organization_id
INNER JOIN params ON
    member.date_of_birth = params.date_of_birth
    AND member.effective_range @> CURRENT_DATE
    AND (
        member.unique_corp_id = params.unique_corp_id
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
                   THEN member.unique_corp_id = right(params.unique_corp_id, 9)
                ELSE false
            END
        );

-- name: get_by_any_verification^
-- Get a member record using either primary or secondary verification.
SELECT * FROM eligibility.member
WHERE (
          (email = :email AND date_of_birth = :date_of_birth)
          OR (
                first_name = :first_name
            AND last_name = :last_name
            AND work_state = :work_state
            AND date_of_birth = :date_of_birth
         )
      )
      AND effective_range @> CURRENT_DATE;

-- name: get_by_client_specific_verification^
-- Query for member records with the given organization ID, unique_corp_id, and DoB
SELECT * FROM eligibility.member
WHERE organization_id = :organization_id
  AND unique_corp_id = :unique_corp_id
  AND date_of_birth = :date_of_birth
  AND effective_range @> CURRENT_DATE;

-- name: get_by_org_identity^
-- Query for a member record with the full unique constraint.
SELECT * FROM eligibility.member
WHERE organization_id = :organization_id
  AND unique_corp_id = :unique_corp_id
  AND dependent_id = :dependent_id;

-- name: get_by_org_email
-- Query for member records with the given organization ID and email.
SELECT * FROM eligibility.member
WHERE organization_id = :organization_id AND email = :email;

-- name: get_difference_by_org_corp_id
-- Query for all entries which are *not* provided within the identities.
WITH missing AS (
    WITH existing AS (
        SELECT unnest(:corp_ids::text[]) as unique_corp_id
    )
    SELECT id from eligibility.member
    WHERE organization_id = :organization_id
        EXCEPT
    SELECT id from eligibility.member
    INNER JOIN existing ON member.unique_corp_id = existing.unique_corp_id
)
SELECT member.* from eligibility.member
INNER JOIN missing ON member.id = missing.id;

-- name: get_by_name_and_date_of_birth
-- Get member records using first_name, last_name and date_of_birth and filter out inactive orgs
SELECT m.* FROM eligibility.member m
    INNER JOIN eligibility.configuration c on m.organization_id = c.organization_id
    WHERE m.effective_range @> CURRENT_DATE
    AND c.activated_at IS NOT NULL
    AND c.activated_at <= CURRENT_DATE
    AND (c.terminated_at IS NULL OR c.terminated_at > CURRENT_DATE)
    AND LOWER(m.first_name) = LOWER(:first_name)
    AND LOWER(m.last_name) = LOWER(:last_name)
    AND m.date_of_birth = :date_of_birth;

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
    coalesce(nullif(record->>'employee_eligibility_date', '')::date, created_at)::date as eligibility_date,
    effective_range,
    created_at,
    updated_at
FROM eligibility.member
WHERE id = :member_id;

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
    coalesce(nullif(record->>'employee_eligibility_date', '')::date, created_at)::date as eligibility_date,
    effective_range,
    created_at,
    updated_at
FROM eligibility.member
WHERE organization_id = :organization_id
  AND unique_corp_id = :unique_corp_id
  AND dependent_id = :dependent_id;

-- name: get_address_for_member^
-- Query for a member's address data, if they have any.
SELECT * FROM eligibility.member_address
WHERE member_id = :member_id;

-- name: get_kafka_record_count_for_org$
-- Query the count of an organization's kafka records
SELECT COUNT(*)
FROM eligibility.member
WHERE organization_id = :organization_id
    AND file_id IS NULL;

-- name: get_file_record_count_for_org$
-- Query the count of an organization's file records
SELECT COUNT(*)
FROM eligibility.member
WHERE organization_id = :organization_id
    AND file_id IS NOT NULL;

-- name: get_by_email_and_name
-- Get member records using email, first_name and last_name and filter out inactive orgs
SELECT m.*
FROM eligibility.member m
INNER JOIN eligibility.configuration c ON m.organization_id = c.organization_id
WHERE m.effective_range @> CURRENT_DATE
    AND c.activated_at IS NOT NULL
    AND c.activated_at <= CURRENT_DATE
    AND (c.terminated_at IS NULL OR c.terminated_at > CURRENT_DATE)
    AND m.email = :email
    AND LOWER(m.first_name) = LOWER(:first_name)
    AND LOWER(m.last_name) = LOWER(:last_name)
ORDER BY m.organization_id, m.updated_at DESC, m.id DESC;
