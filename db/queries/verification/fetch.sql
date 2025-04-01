-- Queries pertaining to the `eligibility.verification` table.

-- name: all
-- Get all verification records.
SELECT * FROM eligibility.verification;

-- name: get^
-- Get an individual verification record;
SELECT * FROM eligibility.verification WHERE id = :id;

-- name: get_for_ids
-- Get verification records for any matching ID
SELECT * FROM eligibility.verification
WHERE id = any(:verification_ids);

-- name: get_for_member_id^
-- Get the most recent verification record for a memberId
SELECT v.*
FROM eligibility.verification v
INNER JOIN eligibility.member_verification mv
ON v.id = mv.verification_id
WHERE mv.member_id = :member_id
AND (
    v.deactivated_at IS NULL
        OR
    v.deactivated_at > CURRENT_DATE
)
ORDER BY mv.updated_at DESC
LIMIT 1;

-- name: get_all_for_member_id
-- Get all verification records for a memberId
SELECT * from eligibility.verification where id in (
	    SELECT verification_id FROM eligibility.member_verification
	    WHERE
		    member_id = :member_id
		    AND verification_id IS NOT NULL
	    ORDER BY
		    updated_at DESC
    )

-- name: get_user_ids_for_eligibility_member_id
SELECT distinct(user_id) from eligibility.verification where id in (
    	    SELECT verification_id FROM eligibility.member_verification
	    WHERE
		    member_id = :member_id
		    AND verification_id IS NOT NULL
	    ORDER BY
		    updated_at DESC
)

-- name: get_for_org
-- Get all the verification records for a given organization ID.
SELECT * FROM eligibility.verification WHERE organization_id = :organization_id;

-- name: get_member_id_for_user_id^
-- Get the corresponding member_id for a given user_id.
SELECT member_id
FROM eligibility.member_verification mv
where mv.verification_id =  (
	SELECT id
	FROM eligibility.verification
	WHERE user_id = :user_id
        ORDER BY id DESC
        LIMIT 1
)
ORDER BY mv.id DESC
LIMIT 1;


-- name: get_member_id_for_user_and_org^
-- Get the corresponding member_id for a given user_id in the org.
SELECT member_id
FROM eligibility.member_verification mv
where mv.verification_id =  (
	SELECT id
	FROM eligibility.verification
	WHERE user_id = :user_id
	      AND organization_id = :organization_id
        ORDER BY id DESC
        LIMIT 1
)
ORDER BY mv.id DESC
LIMIT 1;


-- name: get_count_for_org$
-- Get the current count of verification records for a given org.
SELECT count(id) FROM eligibility.verification WHERE organization_id = :organization_id;

-- name: get_counts_for_orgs
-- Get the current count of verification records for a series of orgs.
SELECT organization_id, count(id) FROM eligibility.verification
WHERE organization_id = any(:organization_ids)
GROUP BY organization_id;

-- name: get_verification_key_for_user^
-- Get the most recent verification record of 1.0 for a userId
SELECT mv.member_id, v.organization_id, mv.created_at, v.id as verification_1_id, v.verification_2_id
FROM eligibility.member_verification mv
JOIN eligibility.verification v ON v.id = mv.verification_id
WHERE v.user_id = :user_id
ORDER BY mv.created_at Desc
LIMIT 1;

-- name: get_verification_key_for_verification_2_id^
-- Get the most recent verification record of 1.0 for a userId
SELECT mv.member_id, v.organization_id, mv.created_at, v.id as verification_1_id, v.verification_2_id
FROM eligibility.member_verification mv
JOIN eligibility.verification v ON v.id = mv.verification_id
WHERE v.verification_2_id = :verification_2_id
ORDER BY mv.created_at Desc
LIMIT 1;

-- name: get_eligibility_verification_record_for_user
-- Get the latest eligibility verification record for a user
-- Case statements are used for assignments here, as different types of verification will require different values to be populated
-- for the sake of returning populated values, we prefer values entered during verification, otherwise we default to the e9y member record
select
	v.id as verification_id,
	v.user_id as user_id,
	v.organization_id as organization_id,
	m.id as eligibility_member_id,
	case
		when m.id is not null
        then m.first_name
		else v.first_name
	end as first_name,
	case
		when m.id is not null
        then m.last_name
		else v.last_name
	end as last_name,
	case
	    when m.id is not null
        then m.date_of_birth
		else v.date_of_birth
	end as date_of_birth,
	case
	    when m.id is not null
        then m.unique_corp_id
		else v.unique_corp_id
	end as unique_corp_id,
	case
		when m.id is not null
        then m.dependent_id
		else v.dependent_id
	end as dependent_id,
	case
		when m.id is not null
        then m.work_state
		else v.work_state
	end as work_state,
	case
		when m.id is not null
        then m.email
		else v.email
	end as email,
	m.record as record,
	v.verification_type as verification_type,
	m.employer_assigned_id as employer_assigned_id,
	m.effective_range as effective_range,
	v.created_at as verification_created_at,
	v.updated_at as verification_updated_at,
	v.deactivated_at as verification_deactivated_at,
	m.gender_code as gender_code,
	m.do_not_contact as do_not_contact,
	v.verified_at as verified_at,
	v.additional_fields as additional_fields,
	v.verification_session as verification_session,
	null as eligibility_member_version,
    v.verification_2_id,
    v2.member_id as eligibility_member_2_id,
    v2.member_version as eligibility_member_2_version
from
	eligibility.verification v
left join eligibility.member_verification mv on
	mv.verification_id = v.id
left join eligibility.member_versioned m on
	mv.member_id = m.id
left join eligibility.verification_2 v2 on
    v.verification_2_id = v2.id
where
	v.user_id = :user_id
    and
    COALESCE(v.deactivated_at, CURRENT_TIMESTAMP + INTERVAL '1 DAY') > CURRENT_TIMESTAMP
order by
	v.verified_at desc,
    m.created_at desc
limit 1;

-- name: get_all_eligibility_verification_records_for_user
-- Get the latest eligibility verification record for a user per organization
-- Case statements are used for assignments here, as different types of verification will require different values to be populated
-- for the sake of returning populated values, we prefer values entered during verification, otherwise we default to the e9y member record
-- this query will return multiple records for members with overeligibility
WITH RankedVerifications AS (
    SELECT
        v.id as verification_id,
        v.user_id as user_id,
        v.organization_id as organization_id,
        m.id as eligibility_member_id,
        CASE
            WHEN m.id IS NOT NULL THEN m.first_name
            ELSE v.first_name
        END as first_name,
        CASE
            WHEN m.id IS NOT NULL THEN m.last_name
            ELSE v.last_name
        END as last_name,
        CASE
            WHEN m.id IS NOT NULL THEN m.date_of_birth
            ELSE v.date_of_birth
        END as date_of_birth,
        CASE
            WHEN m.id IS NOT NULL THEN m.unique_corp_id
            ELSE v.unique_corp_id
        END as unique_corp_id,
        CASE
            WHEN m.id IS NOT NULL THEN m.dependent_id
            ELSE v.dependent_id
        END as dependent_id,
        CASE
            WHEN m.id IS NOT NULL THEN m.work_state
            ELSE v.work_state
        END as work_state,
        CASE
            WHEN m.id IS NOT NULL THEN m.email
            ELSE v.email
        END as email,
        m.record as record,
        v.verification_type as verification_type,
        m.employer_assigned_id as employer_assigned_id,
        m.effective_range as effective_range,
        v.created_at as verification_created_at,
        v.updated_at as verification_updated_at,
        v.deactivated_at as verification_deactivated_at,
        m.gender_code as gender_code,
        m.do_not_contact as do_not_contact,
        v.verified_at as verified_at,
        v.additional_fields as additional_fields,
        v.verification_session as verification_session,
        null as eligibility_member_version,
        v.verification_2_id as verification_2_id,
        v2.member_id as eligibility_member_2_id,
        v2.member_version as eligibility_member_2_version,
        ROW_NUMBER() OVER (PARTITION BY v.organization_id
                           ORDER BY v.verified_at DESC, m.created_at DESC) AS row_num
    FROM eligibility.verification v
    LEFT JOIN eligibility.member_verification mv ON mv.verification_id = v.id
    LEFT JOIN eligibility.member_versioned m ON mv.member_id = m.id
    LEFT JOIN eligibility.verification_2 v2 on v.verification_2_id = v2.id
    WHERE v.user_id = :user_id
        AND COALESCE(v.deactivated_at, CURRENT_TIMESTAMP + INTERVAL '1 DAY') > CURRENT_TIMESTAMP
)

SELECT *
FROM RankedVerifications
WHERE row_num = 1;


-- name: get_e9y_data_for_member_track_backfill
-- given user_id, returns all verification_id, member_id, member created_at
-- including expired members and deactivated verifications
SELECT v.user_id
     , v.id AS verification_id
     , v.organization_id as verification_organization_id
     , v.created_at as verification_created_at
     , mv.member_id
     , mv2.organization_id as member_organization_id
     , mv.created_at AS member_created_at
FROM eligibility.verification v
LEFT JOIN eligibility.member_verification mv ON v.id = mv.verification_id
LEFT JOIN eligibility.member_versioned mv2 ON mv2.id = mv.member_id
WHERE user_id = :user_id