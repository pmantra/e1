-- Queries which point to the mono database

-- name: set_concat_length^
SET SESSION group_concat_max_len = 100000;

-- name: reset_concat_length^
SET SESSION group_concat_max_len = 1024;

-- name: get_org_external_ids_for_org
-- Get external id's for an organization
SELECT
    id,
    LOWER(idp) as source,
    data_provider_organization_id,
    external_id,
    organization_id
FROM maven.organization_external_id oei
WHERE :org_id = oei.organization_id
OR :data_provider_org_id = oei.data_provider_organization_id;

-- name: get_org_from_directory^
-- Get organization metadata from the name of a file's directory.
SELECT
    org.id,
    name,
    directory_name,
    data_provider,
    json,
    COALESCE(
        CONCAT(
            '[',
            GROUP_CONCAT(DISTINCT CONCAT('"', oed.domain, '"') SEPARATOR ','),
            ']'
        ),
        '[]'
    )  AS email_domains,
    activated_at,
    terminated_at,
    employee_only,
    medical_plan_only
FROM
    maven.organization org
    LEFT JOIN maven.organization_email_domain oed ON org.id = oed.organization_id
WHERE directory_name = :directory_name
GROUP BY org.id, name, directory_name, json;

-- name: get_org_from_id^
-- Get organization metadata from an org's ID
SELECT
    org.id,
    name,
    directory_name,
    data_provider,
    json,
    COALESCE(
        CONCAT(
            '[',
            GROUP_CONCAT(DISTINCT CONCAT('"', oed.domain, '"') SEPARATOR ','),
            ']'
        ),
        '[]'
    )  AS email_domains,
    activated_at,
    terminated_at,
    employee_only,
    medical_plan_only
FROM
    maven.organization org
    LEFT JOIN maven.organization_email_domain oed ON org.id = oed.organization_id
WHERE org.id = :id;

-- name: get_orgs_for_sync
-- Get all org configs, including email domains.
SELECT
    org.id,
    name,
    directory_name,
    data_provider,
    json,
    COALESCE(
        CONCAT(
            '[',
            GROUP_CONCAT(DISTINCT CONCAT('"', oed.domain, '"') SEPARATOR ','),
            ']'
        ),
        '[]'
    )  AS email_domains,
    activated_at,
    terminated_at,
    employee_only,
    medical_plan_only,
    eligibility_type
FROM
    maven.organization org
    LEFT JOIN maven.organization_email_domain oed ON org.id = oed.organization_id
GROUP BY org.id, name, directory_name, json;

-- name: get_external_ids_for_sync
-- Get an org's external ID mappings for various external data sources
SELECT
    LOWER(idp) as source,
    data_provider_organization_id,
    external_id,
    organization_id
FROM organization_external_id;

-- name: get_non_ended_track_information_for_organization_id
-- Get information for an org's tracks that haven't been explicitly ended yet. This is to
-- allow the configuration of sub-populations with tracks that are not yet active.
SELECT
    id, CONCAT(track, ' (', length_in_days, ' days)') as descriptor
FROM maven.client_track ct
WHERE ct.organization_id = :organization_id
AND (
    ended_at IS NULL
    OR
    ended_at > CURRENT_TIMESTAMP
);

-- name: get_non_ended_reimbursement_organization_settings_information_for_organization_id
-- Get an org's reimbursement organization settings information
-- Get information for an org's reimbursement organization settings that haven't been
-- explicitly ended yet. This is to allow the configuration of sub-populations with
-- reimbursement organization settings that are not yet active.
SELECT
    ros.id, COALESCE(ros.name, CAST(ros.id AS CHAR(50))) as descriptor
FROM maven.reimbursement_organization_settings ros
WHERE ros.organization_id = :organization_id
AND (
    ended_at IS NULL
    OR
    ended_at > CURRENT_TIMESTAMP
);

-- name: get_credit_back_fill_requests
-- Get credit_id, oe_id needs to be back filled
SELECT c.id,
       c.user_id,
       oe.organization_id,
       c.organization_employee_id,
       oe.eligibility_member_id as oe_e9y_member_id
FROM maven.credit c
JOIN maven.organization_employee oe on c.organization_employee_id = oe.id
WHERE c.organization_employee_id IS NOT NULL
AND c.eligibility_verification_id IS NULL
AND c.eligibility_member_id IS NULL
AND c.created_at >= '2023-11-01'
AND c.id > :last_id
ORDER BY ID ASC LIMIT :batch_size


-- name: backfill_credit_record
-- Backfill e9y ids for a credit record
UPDATE maven.credit c
SET eligibility_member_id = :e9y_member_id,
    eligibility_verification_id = :e9y_verification_id
WHERE id = :id


-- name: get_member_track_back_fill_requests
-- Get member track to be back filled
select mt.id as member_track_id,
       mt.user_id,
       mt.organization_employee_id,
       mt.created_at,
       ct.organization_id
       from maven.member_track mt
join maven.client_track ct on ct.id = mt.client_track_id
where mt.organization_employee_id is NOT NULL
and (mt.eligibility_member_id is NULL and mt.eligibility_verification_id is NULL)
AND mt.id > :last_id
ORDER BY mt.id ASC LIMIT :batch_size


-- name: backfill_member_track_record
-- Backfill e9y ids for a member_track record
UPDATE maven.member_track
SET eligibility_member_id = :e9y_member_id,
    eligibility_verification_id = :e9y_verification_id
WHERE id = :id

-- name: get_oed_back_fill_requests
-- Get oed_id needs to be back filled
SELECT id
FROM maven.organization_employee_dependent
WHERE reimbursement_wallet_id IS NULL
AND id > :last_id
ORDER BY ID ASC LIMIT :batch_size

-- name: backfill_oed_record
-- Backfill oed record
UPDATE maven.organization_employee_dependent
SET reimbursement_wallet_id = :reimbursement_wallet_id
WHERE id = :id

-- name: get_rw_id_for_oed^
-- Get reimbursement wallet id for an oed record
SELECT rw.id as reimbursement_wallet_id
FROM maven.reimbursement_wallet rw
JOIN maven.organization_employee_dependent oed ON rw.organization_employee_id = oed.organization_employee_id
WHERE oed.id = :id
LIMIT 1

-- name: get_member_track_back_fill_requests_for_v2
-- Get member track to be back filled for v2
SELECT mt.id AS member_track_id,
       mt.user_id,
       mt.organization_employee_id,
       mt.created_at,
       ct.organization_id,
       mt.eligibility_verification_id AS existing_e9y_verification_id,
       mt.eligibility_member_id AS existing_e9y_member_id
FROM maven.member_track mt
JOIN maven.client_track ct ON ct.id = mt.client_track_id
WHERE mt.eligibility_member_id IS NULL AND mt.eligibility_verification_id IS NOT NULL
AND ct.organization_id = :organization_id
AND mt.id > :last_id
ORDER BY mt.id ASC LIMIT :batch_size

-- name: get_member_track_back_fill_requests_for_billing
-- Get member track to be back filled for billing by given list of member track ids
SELECT mt.id AS member_track_id,
       mt.user_id,
       mt.organization_employee_id,
       mt.created_at,
       ct.organization_id,
       mt.eligibility_verification_id AS existing_e9y_verification_id,
       mt.eligibility_member_id AS existing_e9y_member_id
FROM maven.member_track mt
JOIN maven.client_track ct ON ct.id = mt.client_track_id
WHERE mt.id in :member_track_ids
ORDER BY mt.id ASC

-- name: get_all_records_with_optum_idp
-- Get all records that have Optum as their IDP
SELECT
    id,
    organization_id,
    external_id,
    data_provider_organization_id,
    idp
FROM organization_external_id
WHERE idp = :optum_idp;

-- name: update_org_provider
-- Update organization external ID provider
UPDATE organization_external_id
SET data_provider_organization_id = :new_provider_org_id
WHERE id = :record_id;