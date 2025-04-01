-- Mutations pertaining to the `eligibility.member_versioned` table.


-- name: persist<!
-- Create or Update a Member record for an organization.
INSERT INTO eligibility.member_versioned(
    organization_id,
    first_name,
    last_name,
    email,
    unique_corp_id,
    dependent_id,
    date_of_birth,
    work_state,
    work_country,
    record,
    custom_attributes,
    file_id,
    effective_range,
    do_not_contact,
    gender_code,
    employer_assigned_id,
    pre_verified,
    hash_value,
    hash_version
)
VALUES (
    :organization_id,
    :first_name,
    :last_name,
    :email,
    :unique_corp_id,
    :dependent_id,
    :date_of_birth,
    :work_state,
    :work_country,
    :record,
    :custom_attributes,
    :file_id,
    coalesce(:effective_range, eligibility.default_range()),
    :do_not_contact,
    :gender_code,
    :employer_assigned_id,
    :pre_verified,
    :hash_value,
    :hash_version
)
ON CONFLICT (hash_value, hash_version) DO NOTHING
RETURNING *;

-- name: bulk_persist
-- Create or Update a series of Member records.
WITH parsed_records AS (
    SELECT (unnest(:records::eligibility.parsed_record[])::eligibility.parsed_record).*
)
INSERT INTO eligibility.member_versioned(
        organization_id,
        first_name,
        last_name,
        email,
        unique_corp_id,
        dependent_id,
        date_of_birth,
        work_state,
        work_country,
        record,
        custom_attributes,
        file_id,
        effective_range,
        do_not_contact,
        gender_code,
        employer_assigned_id,
        pre_verified,
        hash_value,
        hash_version
)
SELECT
    pr.organization_id,
    pr.first_name,
    pr.last_name,
    pr.email,
    pr.unique_corp_id,
    coalesce(pr.dependent_id, ''),
    pr.date_of_birth,
    pr.work_state,
    pr.work_country,
    coalesce(pr.record, '{}')::jsonb,
    coalesce(pr.custom_attributes, '{}')::jsonb,
    pr.file_id,
    coalesce(pr.effective_range, eligibility.default_range()),
    pr.do_not_contact,
    pr.gender_code,
    pr.employer_assigned_id,
    pr.pre_verified,
    pr.hash_value,
    pr.hash_version
FROM parsed_records pr
ON CONFLICT (hash_value, hash_version) DO NOTHING
RETURNING *;

-- name: upsert_parsed_records_for_files$
-- Upsert all the parsed records for a given file ID.
-- This acts as an atomic "move" operation - delete the source and copy to the destination.
SELECT count(*) FROM eligibility.migrate_file_parse_results(:files::bigint[]);

-- name: clear_parse_results_for_files$
-- Clear all parse results for a given file from their tables.
DELETE FROM eligibility.file_parse_results WHERE file_id = ANY (:files::bigint[]);

-- name: clear_parse_errors_for_files$
-- Clear all parse results for a given file from their tables.
DELETE FROM eligibility.file_parse_errors WHERE file_id = ANY (:files::bigint[]);


-- name: delete<!
-- Delete a member record.
DELETE FROM eligibility.member_versioned
WHERE id = :id
RETURNING *;

-- name: bulk_delete!
-- Delete multiple member records.
DELETE FROM eligibility.member_versioned
WHERE id = any(:ids);

-- name: delete_all_for_org!
-- Delete all member records for a given organization.
DELETE FROM eligibility.member_versioned
WHERE organization_id = :organization_id;

-- name: set_effective_range<!
UPDATE eligibility.member_versioned
SET effective_range = :range::daterange
WHERE id = :id
RETURNING effective_range;

-- name: bulk_set_effective_range!
UPDATE eligibility.member_versioned
SET effective_range = tmp.range
FROM (SELECT (unnest(:ranges::eligibility.id_to_range[])::eligibility.id_to_range).*) tmp
WHERE member_versioned.id = tmp.id;

-- name: set_do_not_contact<!
-- Set the do_not_contact column for a record
UPDATE eligibility.member_versioned
SET do_not_contact = :value::eligibility.iwstext
WHERE id = :id
RETURNING do_not_contact;

-- name: set_dependent_id_for_member<!
-- Set the set_dependent_id_for_member column for a record
UPDATE eligibility.member_versioned
SET dependent_id = :dependent_id::eligibility.iwstext
WHERE id = :id
RETURNING dependent_id;

-- name: bulk_set_do_not_contact!
-- Bulk set the do_not_contact column for many records
UPDATE eligibility.member_versioned
SET do_not_contact = tmp.text::eligibility.iwstext
FROM (SELECT (unnest(:records::eligibility.id_to_text[])::eligibility.id_to_text).*) tmp
WHERE member_versioned.id = tmp.id;



--name: update_optum_rows_with_hash
with updated_rows as (
UPDATE eligibility.member_versioned
SET hash_value = tmp.text::text, hash_version = 2
FROM (SELECT (unnest(:records::eligibility.id_to_text[])::eligibility.id_to_text).*) tmp
WHERE member_versioned.id = tmp.id
  AND NOT EXISTS (
    SELECT * FROM eligibility.member_versioned mv
    WHERE mv.hash_value = tmp.text::text
  AND mv.organization_id = :organization_id
    )
RETURNING eligibility.member_versioned.*
)
SELECT id FROM updated_rows;



-- -- name: remove_optum_hash_duplicates
with removed_rows as (
DELETE FROM eligibility.member_versioned
WHERE id = any(:ids)
RETURNING eligibility.member_versioned.*
)
SELECT id FROM removed_rows;


-- name: disable_timestamp_trigger
-- This should only be used for testing, so we can manually set the time a record was created
ALTER TABLE eligibility.member_versioned DISABLE TRIGGER set_member_versioned_timestamp;


-- name: reenable_timestamp_trigger
-- This should only be used for testing, so we can manually set the time a record was created
ALTER TABLE eligibility.member_versioned ENABLE TRIGGER set_member_versioned_timestamp;


--name: set_updated_at<!
-- Set the updated_at column for a single record
-- This should only be used for testing, so we can manually set the time a record was created
UPDATE eligibility.member_versioned
SET updated_at = :updated_at
where id = :id;


--name: set_created_at<!
-- Set the created_at column for a single record
-- This should only be used for testing, so we can manually set the time a record was created
UPDATE eligibility.member_versioned
SET created_at = :created_at
where id = :id;


--name: set_pre_verified<!
-- Set the pre_verified column for a single record
UPDATE eligibility.member_versioned
SET pre_verified = :pre_verified
where id = :id
RETURNING *;


-- name: bulk_persist_external_records
-- Bulk persist external records to the member database.
WITH external_records AS (
    SELECT (unnest(:records::eligibility.external_record[])::eligibility.external_record).*
    ORDER BY received_ts DESC
)
INSERT INTO eligibility.member_versioned (
    organization_id,
    first_name,
    last_name,
    email,
    unique_corp_id,
    dependent_id,
    date_of_birth,
    work_state,
    work_country,
    record,
    custom_attributes,
    effective_range,
    gender_code,
    do_not_contact,
    employer_assigned_id,
    hash_value,
    hash_version
)
SELECT
    er.organization_id,
    er.first_name,
    er.last_name,
    er.email,
    er.unique_corp_id,
    coalesce(er.dependent_id, ''),
    er.date_of_birth,
    er.work_state,
    er.work_country,
    coalesce(er.record, '{}')::jsonb,
    coalesce(er.custom_attributes, '{}')::jsonb,
    coalesce(er.effective_range, eligibility.default_range()),
    er.gender_code,
    er.do_not_contact,
    coalesce(er.employer_assigned_id, ''),
    er.hash_value,
    er.hash_version
FROM external_records er
ON CONFLICT (hash_value, hash_version) DO UPDATE SET record = excluded.record
RETURNING *;


-- name: bulk_persist_member_address_versioned
-- Bulk persist external address records to the member database.
with addresses AS (
    select (unnest(:addresses::eligibility.member_address_versioned[])::eligibility.member_address_versioned).*
)
INSERT INTO eligibility.member_address_versioned (
    member_id,
    address_1,
    address_2,
    city,
    state,
    postal_code,
    postal_code_suffix,
    country_code,
    address_type
)
SELECT
    a.member_id,
    a.address_1,
    a.address_2,
    a.city,
    a.state,
    a.postal_code,
    a.postal_code_suffix,
    a.country_code,
    a.address_type
FROM addresses a
ON CONFLICT DO NOTHING
RETURNING *;

-- name: bulk_delete_member_address_by_member_id!
-- Delete multiple member records.
DELETE FROM eligibility.member_address_versioned
WHERE member_id = any(:member_ids);

-- name: get_count_of_member_address$
SELECT COUNT(*) FROM eligibility.member_address_versioned;



-- name: purge_expired_records
-- Delete all unused or expired records
WITH purged_rows AS (
    DELETE
    FROM eligibility.member_versioned
    WHERE
    -- exclude records tied to a verification
    id NOT IN (SELECT distinct(member_id)
                       FROM eligibility.member_verification)
    -- --  do not consider optum rows (fileID = none) as we don't mark those as terminated
    -- --  exclude records that are still 'valid' or could be pre-verified
    AND id NOT IN (SELECT id
                     FROM eligibility.member_versioned mv
                     WHERE (file_id IS NULL OR (file_id IS NOT NULL AND effective_range @> CURRENT_DATE))
                     AND organization_id = :organization_id)
    -- -- exclude our first wallet records
    AND id NOT IN (SELECT Min(id)
                     FROM eligibility.member_versioned
                     WHERE organization_id = :organization_id
                     GROUP BY organization_id,
                              unique_corp_id,
                              dependent_id)
    -- filter down to our organization
    AND organization_id = :organization_id
    RETURNING eligibility.member_versioned.*
), purged_ids AS (
    INSERT INTO eligibility.member_versioned_historical
    SELECT * FROM purged_rows
)
SELECT COUNT(1) AS purged_count FROM purged_rows;



-- name: purge_duplicate_non_hash_optum
-- Delete all records for an optum org which are A) not hashed and B) duplicates of records that *will be hashed and C) not the most original value for a record to be hashed
WITH purged_address_rows as (
    DELETE FROM eligibility.member_address_versioned
    WHERE member_id  = ANY (:member_ids::bigint[])
    RETURNING eligibility.member_address_versioned.*
),
purged_member_rows as (
    DELETE FROM eligibility.member_versioned
    WHERE ID  = ANY (:member_ids::bigint[])
    RETURNING eligibility.member_versioned.*
)
SELECT COUNT(1) FROM purged_member_rows;