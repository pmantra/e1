-- Mutations pertaining to the `eligibility.member` table.

-- name: persist<!
-- Create or Update a Member record for an organization.
INSERT INTO eligibility.member(
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
    employer_assigned_id
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
    :employer_assigned_id
)
ON CONFLICT (
    organization_id, ltrim(lower(unique_corp_id), '0'), lower(dependent_id)
    )
    DO UPDATE SET
        organization_id = excluded.organization_id,
        first_name = excluded.first_name,
        last_name = excluded.last_name,
        email = excluded.email,
        unique_corp_id = excluded.unique_corp_id,
        dependent_id = excluded.dependent_id,
        date_of_birth = excluded.date_of_birth,
        work_state = excluded.work_state,
        work_country = excluded.work_country,
        record = excluded.record,
        custom_attributes = excluded.custom_attributes,
        file_id = excluded.file_id,
        effective_range = excluded.effective_range,
        do_not_contact = excluded.do_not_contact,
        gender_code = excluded.gender_code,
        employer_assigned_id = excluded.employer_assigned_id
RETURNING *;

-- name: bulk_persist
-- Create or Update a series of Member records.
WITH parsed_records AS (
    SELECT (unnest(:records::eligibility.parsed_record[])::eligibility.parsed_record).*
)
INSERT INTO eligibility.member(
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
        employer_assigned_id
)
SELECT DISTINCT ON (
        pr.organization_id, ltrim(lower(pr.unique_corp_id), '0'), lower(pr.dependent_id)
    )
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
    pr.employer_assigned_id
FROM parsed_records pr
ON CONFLICT (
    organization_id, ltrim(lower(unique_corp_id), '0'), lower(dependent_id)
    )
    DO UPDATE SET
        organization_id = excluded.organization_id,
        first_name = excluded.first_name,
        last_name = excluded.last_name,
        email = excluded.email,
        unique_corp_id = excluded.unique_corp_id,
        dependent_id = excluded.dependent_id,
        date_of_birth = excluded.date_of_birth,
        work_state = excluded.work_state,
        work_country = excluded.work_country,
        record = excluded.record,
        custom_attributes = excluded.custom_attributes,
        file_id = excluded.file_id,
        effective_range = excluded.effective_range,
        do_not_contact = excluded.do_not_contact,
        gender_code = excluded.gender_code,
        employer_assigned_id = excluded.employer_assigned_id
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
DELETE FROM eligibility.member
WHERE id = :id
RETURNING *;

-- name: bulk_delete!
-- Delete multiple member records.
DELETE FROM eligibility.member
WHERE id = any(:ids);

-- name: delete_all_for_org!
-- Delete all member records for a given organization.
DELETE FROM eligibility.member
WHERE organization_id = :organization_id;

-- name: set_effective_range<!
UPDATE eligibility.member
SET effective_range = :range::daterange
WHERE id = :id
RETURNING effective_range;

-- name: bulk_set_effective_range!
UPDATE eligibility.member
SET effective_range = tmp.range
FROM (SELECT (unnest(:ranges::eligibility.id_to_range[])::eligibility.id_to_range).*) tmp
WHERE member.id = tmp.id;

-- name: set_do_not_contact<!
-- Set the do_not_contact column for a record
UPDATE eligibility.member
SET do_not_contact = :value::eligibility.iwstext
WHERE id = :id
RETURNING do_not_contact;

-- name: bulk_set_do_not_contact!
-- Bulk set the do_not_contact column for many records
UPDATE eligibility.member
SET do_not_contact = tmp.text::eligibility.iwstext
FROM (SELECT (unnest(:records::eligibility.id_to_text[])::eligibility.id_to_text).*) tmp
WHERE member.id = tmp.id;

-- name: bulk_persist_external_records
-- Bulk persist external records to the member database.
WITH external_records AS (
    SELECT (unnest(:records::eligibility.external_record[])::eligibility.external_record).*
    ORDER BY received_ts DESC
)
INSERT INTO eligibility.member (
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
    employer_assigned_id
)
SELECT DISTINCT ON (er.organization_id, ltrim(lower(er.unique_corp_id), '0'), coalesce(er.dependent_id, ''))
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
    coalesce(er.employer_assigned_id, '')
FROM external_records er
ON CONFLICT (
    organization_id, ltrim(lower(unique_corp_id), '0'), lower(dependent_id)
    )
    DO UPDATE SET
        organization_id = excluded.organization_id,
        first_name = excluded.first_name,
        last_name = excluded.last_name,
        email = excluded.email,
        unique_corp_id = excluded.unique_corp_id,
        dependent_id = excluded.dependent_id,
        date_of_birth = excluded.date_of_birth,
        work_state = excluded.work_state,
        work_country = excluded.work_country,
        record = excluded.record,
        custom_attributes = excluded.custom_attributes,
        file_id = excluded.file_id,
        effective_range = excluded.effective_range,
        gender_code = excluded.gender_code,
        do_not_contact = excluded.do_not_contact,
        employer_assigned_id = excluded.employer_assigned_id
RETURNING *;


-- name: bulk_persist_member_address
-- Bulk persist external address records to the member database.
with addresses AS (
    select (unnest(:addresses::eligibility.member_address[])::eligibility.member_address).*
)
INSERT INTO eligibility.member_address(
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
SELECT DISTINCT ON (a.member_id)
    a.member_id,
    a.address_1,
    a.address_2,
    a.city,
    a.state,
    a.postal_code,
    a.postal_code_suffix,
    a.country_code,
    a.address_type
from addresses a
ON CONFLICT (member_id)
    DO UPDATE SET
        address_1 = excluded.address_1,
        address_2 = excluded.address_2,
        city = excluded.city,
        state = excluded.state,
        postal_code = excluded.postal_code,
        postal_code_suffix = excluded.postal_code_suffix,
        country_code = excluded.country_code,
        address_type = excluded.address_type
RETURNING *;

-- name: bulk_delete_member_address_by_member_id!
-- Delete multiple member records.
DELETE FROM eligibility.member_address
WHERE member_id = any(:member_ids);

-- name: tmp_bulk_persist_external_records
-- Bulk persist external records to the member database.
WITH external_records AS (
    SELECT (unnest(:records::eligibility.external_record[])::eligibility.external_record).*
    ORDER BY received_ts DESC
)
INSERT INTO eligibility.tmp_member (
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
    employer_assigned_id
)
SELECT DISTINCT ON (er.organization_id, ltrim(lower(er.unique_corp_id), '0'), coalesce(er.dependent_id, ''))
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
    coalesce(er.employer_assigned_id, '')
FROM external_records er
ON CONFLICT (
    organization_id, ltrim(lower(unique_corp_id), '0'), lower(dependent_id)
    )
    DO UPDATE SET
        organization_id = excluded.organization_id,
        first_name = excluded.first_name,
        last_name = excluded.last_name,
        email = excluded.email,
        unique_corp_id = excluded.unique_corp_id,
        dependent_id = excluded.dependent_id,
        date_of_birth = excluded.date_of_birth,
        work_state = excluded.work_state,
        work_country = excluded.work_country,
        record = excluded.record,
        custom_attributes = excluded.custom_attributes,
        file_id = excluded.file_id,
        effective_range = excluded.effective_range,
        gender_code = excluded.gender_code,
        do_not_contact = excluded.do_not_contact,
        employer_assigned_id = excluded.employer_assigned_id
RETURNING *;


-- name: tmp_bulk_persist_member_address
-- Bulk persist external address records to the member database.
with addresses AS (
    select (unnest(:addresses::eligibility.member_address[])::eligibility.member_address).*
)
INSERT INTO eligibility.tmp_member_address(
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
SELECT DISTINCT ON (a.member_id)
    a.member_id,
    a.address_1,
    a.address_2,
    a.city,
    a.state,
    a.postal_code,
    a.postal_code_suffix,
    a.country_code,
    a.address_type
from addresses a
ON CONFLICT (member_id)
    DO UPDATE SET
        address_1 = excluded.address_1,
        address_2 = excluded.address_2,
        city = excluded.city,
        state = excluded.state,
        postal_code = excluded.postal_code,
        postal_code_suffix = excluded.postal_code_suffix,
        country_code = excluded.country_code,
        address_type = excluded.address_type
RETURNING *;

-- name: tmp_bulk_delete_member_address_by_member_id!
-- Delete multiple member records.
DELETE FROM eligibility.tmp_member_address
WHERE member_id = any(:member_ids);

-- name: get_id_range_for_member^
-- Get the min and max IDs for member
SELECT
    MIN(id) as min_id,
    MAX(id) as max_id
FROM eligibility.member;

-- name: get_id_range_for_member_address^
-- Get the min and max IDs for member_address
SELECT
    MIN(id) as min_id,
    MAX(id) as max_id
FROM eligibility.member_address;

-- name: migrate_member_for_range
SELECT eligibility.batch_migrate_member(:min_id, :max_id);

-- name: migrate_member_address_for_range
SELECT eligibility.batch_migrate_member_address(:min_id, :max_id);

--name: set_created_at<!
-- Set the created_at column for a single record
-- This should only be used for testing, so we can manually set the time a record was created
UPDATE eligibility.member
SET created_at = :created_at
where id = :id;