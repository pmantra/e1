-- Mutations pertaining to the `eligibility.file_parse_results` table.

-- name: tmp_bulk_persist_file_parse_results$
-- Create or Update a series of FileParseResult records.
WITH records AS (
    WITH parsed_records AS (
        SELECT (unnest(:results::eligibility.file_parse_results[])::eligibility.file_parse_results).*
    )
    INSERT INTO eligibility.tmp_file_parse_results(
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
            errors,
            warnings,
            file_id,
            effective_range,
            do_not_contact,
            gender_code,
            employer_assigned_id,
            hash_value,
            hash_version
    )
    SELECT DISTINCT ON (
            pr.organization_id, pr.file_id,  ltrim(lower(pr.unique_corp_id), '0'), lower(pr.dependent_id)
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
        pr.errors,
        pr.warnings,
        pr.file_id,
        coalesce(pr.effective_range, eligibility.default_range()),
        pr.do_not_contact,
        pr.gender_code,
        pr.employer_assigned_id,
        pr.hash_value,
        pr.hash_version
    FROM parsed_records pr
    RETURNING *
)
SELECT COUNT(*) FROM records;

-- name: tmp_bulk_persist_file_parse_errors$
-- Create or Update a series of FileParseErrors records.
WITH records AS (
    WITH parsed_errors AS (
        SELECT (unnest(:errors::eligibility.file_parse_errors[])).*
    )
    INSERT INTO eligibility.tmp_file_parse_errors(
            file_id,
            organization_id,
            record,
            errors,
            warnings
    )
    SELECT
        pr.file_id,
        pr.organization_id,
        coalesce(pr.record, '{}')::jsonb,
        pr.errors,
        pr.warnings
    FROM parsed_errors pr
    RETURNING *
)
SELECT COUNT(*) FROM records;

-- name: tmp_delete_file_parse_results_for_files$
-- Delete all parse results for a given file from their tables.
WITH deleted as (
    DELETE FROM eligibility.tmp_file_parse_results
    WHERE file_id = ANY (:files::bigint[])
    RETURNING *
)
SELECT count(*) FROM deleted;

-- name: tmp_delete_file_parse_errors_for_files$
-- Delete all parse results for a given file from their tables.
WITH deleted as (
    DELETE FROM eligibility.tmp_file_parse_errors
    WHERE file_id = ANY (:files::bigint[])
    RETURNING *
)
SELECT count(*) FROM deleted;

-- name: tmp_expire_missing_records_for_file$
-- Expire all records which weren't present in the given file.
UPDATE eligibility.tmp_member m SET
    effective_range = daterange(coalesce(lower(effective_range), (current_date - INTERVAL '1 day')::date), current_date::date)
WHERE
    -- Member records from an old file
    m.file_id < :file_id
    -- Expire records that are either part of a non-data-provider org
    -- OR expire records that are associated to a data_provider org
    -- BUT not both. Data providers currently do not have their own records in the file.
    AND m.organization_id in (
        SELECT DISTINCT organization_id
        FROM eligibility.organization_external_id
        WHERE data_provider_organization_id = :organization_id
        UNION
        SELECT organization_id
        FROM eligibility.configuration
        WHERE organization_id = :organization_id
        AND data_provider = false
    )
    -- Find the member records that are not expired already
    AND m.effective_range @> CURRENT_DATE;

-- name: tmp_bulk_persist_parsed_records_for_files$
-- Upsert all the parsed records for a given file ID.
-- This acts as an atomic "move" operation - delete the source and copy to the destination.
SELECT count(*) FROM eligibility.tmp_migrate_file_parse_results(:files::bigint[]);

-- name: bulk_persist_file_parse_results$
-- Create or Update a series of FileParseResult records.
WITH records AS (
    WITH parsed_records AS (
        SELECT (unnest(:results::eligibility.file_parse_results[])::eligibility.file_parse_results).*
    )
    INSERT INTO eligibility.file_parse_results(
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
            errors,
            warnings,
            file_id,
            effective_range,
            do_not_contact,
            gender_code,
            employer_assigned_id,
            hash_value,
            hash_version
    )
    SELECT DISTINCT ON (
            pr.organization_id, pr.file_id,  ltrim(lower(pr.unique_corp_id), '0'), lower(pr.dependent_id)
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
        pr.errors,
        pr.warnings,
        pr.file_id,
        coalesce(pr.effective_range, eligibility.default_range()),
        pr.do_not_contact,
        pr.gender_code,
        pr.employer_assigned_id,
        pr.hash_value,
        pr.hash_version
    FROM parsed_records pr
    RETURNING *
)
SELECT COUNT(*) FROM records;

-- name: bulk_persist_file_parse_errors$
-- Create or Update a series of FileParseErrors records.
WITH records AS (
    WITH parsed_errors AS (
        SELECT (unnest(:errors::eligibility.file_parse_errors[])).*
    )
    INSERT INTO eligibility.file_parse_errors(
            file_id,
            organization_id,
            record,
            errors,
            warnings
    )
    SELECT
        pr.file_id,
        pr.organization_id,
        coalesce(pr.record, '{}')::jsonb,
        pr.errors,
        pr.warnings
    FROM parsed_errors pr
    RETURNING *
)
SELECT COUNT(*) FROM records;

-- name: delete_file_parse_results_for_files$
-- Delete all parse results for a given file from their tables.
WITH deleted as (
    DELETE FROM eligibility.file_parse_results
    WHERE file_id = ANY (:files::bigint[])
    RETURNING *
)
SELECT count(*) FROM deleted;

-- name: delete_file_parse_errors_for_files$
-- Delete all parse results for a given file from their tables.
WITH deleted as (
    DELETE FROM eligibility.file_parse_errors
    WHERE file_id = ANY (:files::bigint[])
    RETURNING *
)
SELECT count(*) FROM deleted;

-- name: expire_missing_records_for_file$
-- Expire all records which weren't present in the given file.
UPDATE eligibility.member m SET
    effective_range = daterange(coalesce(lower(effective_range), (current_date - INTERVAL '1 day')::date), current_date::date)
WHERE
    -- Member records from an old file
    m.file_id < :file_id
    -- Expire records that are either part of a non-data-provider org
    -- OR expire records that are associated to a data_provider org
    -- BUT not both. Data providers currently do not have their own records in the file.
    AND m.organization_id in (
        SELECT DISTINCT organization_id
        FROM eligibility.organization_external_id
        WHERE data_provider_organization_id = :organization_id
        UNION
        SELECT organization_id
        FROM eligibility.configuration
        WHERE organization_id = :organization_id
        AND data_provider = false
    )
    -- Find the member records that are not expired already
    AND m.effective_range @> CURRENT_DATE;

-- name: expire_missing_records_for_file_versioned$
-- Expire all records which weren't present in the given file.
-- This is necessary for file based populations because a single file contains the full
-- population of an organization. So we must expire all previous files, as the most recent
-- file now represents the full population.
with expired_rows as (
UPDATE eligibility.member_versioned m SET
    effective_range = daterange(coalesce(lower(effective_range), (current_date - INTERVAL '1 day')::date), current_date::date),
    hash_value = null,
    hash_version = null
WHERE
    -- Member records from an old file
    m.file_id IN (
        SELECT id
        FROM eligibility.file f
        WHERE f.organization_id = :organization_id
        AND f.id < :file_id
        ORDER BY f.id DESC
        LIMIT 3
    )
    -- Expire records that are either part of a non-data-provider org
    -- OR expire records that are associated to a data_provider org
    -- BUT not both. Data providers currently do not have their own records in the file.
    AND m.organization_id in (
        SELECT DISTINCT organization_id
        FROM eligibility.organization_external_id
        WHERE data_provider_organization_id = :organization_id
        UNION
        SELECT organization_id
        FROM eligibility.configuration
        WHERE organization_id = :organization_id
        AND data_provider = false
    )
    -- Find the member records that are not expired already
    AND m.effective_range @> CURRENT_DATE
    returning m.id
    )
select count(*) from expired_rows;

-- name: bulk_persist_parsed_records_for_files$
-- Upsert all the parsed records for a given file ID to member.
-- This acts as an atomic "move" operation - delete the source and copy to the destination.
WITH records AS (
    DELETE FROM eligibility.file_parse_results
    WHERE file_id = ANY (:files::bigint[])
    RETURNING
        organization_id,
        first_name,
        last_name,
        email,
        unique_corp_id,
        dependent_id,
        date_of_birth,
        work_state,
        record,
        file_id,
        effective_range
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
    record,
    file_id,
    effective_range
)
SELECT DISTINCT ON (
        pr.organization_id, lower(ltrim(pr.unique_corp_id, '0')), lower(pr.dependent_id)
    )
    pr.organization_id,
    pr.first_name,
    pr.last_name,
    pr.email,
    pr.unique_corp_id,
    pr.dependent_id,
    pr.date_of_birth,
    pr.work_state,
    coalesce(pr.record, '{}')::jsonb,
    pr.file_id,
    coalesce(pr.effective_range, eligibility.default_range())
FROM records pr
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
        record = excluded.record,
        file_id = excluded.file_id,
        effective_range = excluded.effective_range;

-- name: bulk_persist_parsed_records_for_files_dual_write$
-- Upsert all the parsed records for a given file ID to member and inserts to member_versioned.
-- This acts as an atomic "move" operation - delete the source and copy to the destination.
WITH records AS (
    DELETE FROM eligibility.file_parse_results
    WHERE file_id = ANY (:files::bigint[])
    RETURNING
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
        gender_code,
        effective_range
), member_insert AS (
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
        gender_code,
        effective_range
    )
    SELECT DISTINCT ON (
            pr.organization_id, lower(ltrim(pr.unique_corp_id, '0')), lower(pr.dependent_id)
        )
        pr.organization_id,
        pr.first_name,
        pr.last_name,
        pr.email,
        pr.unique_corp_id,
        pr.dependent_id,
        pr.date_of_birth,
        pr.work_state,
        pr.work_country,
        coalesce(pr.record, '{}')::jsonb,
        coalesce(pr.custom_attributes, '{}')::jsonb,
        pr.file_id,
        pr.gender_code,
        coalesce(pr.effective_range, eligibility.default_range())
    FROM records pr
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
            gender_code = excluded.gender_code,
            effective_range = excluded.effective_range
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
    gender_code,
    effective_range
)
SELECT DISTINCT ON (
        pr.organization_id, lower(ltrim(pr.unique_corp_id, '0')), lower(pr.dependent_id)
    )
    pr.organization_id,
    pr.first_name,
    pr.last_name,
    pr.email,
    pr.unique_corp_id,
    pr.dependent_id,
    pr.date_of_birth,
    pr.work_state,
    pr.work_country,
    coalesce(pr.record, '{}')::jsonb,
    coalesce(pr.custom_attributes, '{}')::jsonb,
    pr.file_id,
    pr.gender_code,
    coalesce(pr.effective_range, eligibility.default_range())
FROM records pr;


-- name: bulk_persist_parsed_records_for_files_dual_write_hash
-- Upsert all the parsed records for a given file ID to member and inserts to member_versioned.
-- This acts as an atomic "move" operation - delete the source and copy to the destination.
WITH records AS (
    DELETE FROM eligibility.file_parse_results -- grab the records from the file_parse_results
    WHERE file_id = ANY (:files::bigint[])
    RETURNING
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
        gender_code,
        employer_assigned_id,
        hash_value,
        hash_version
), member_insert AS (
    INSERT INTO eligibility.member(  -- insert unique records from e9y.member and return what was inserted
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
        gender_code
    )
    SELECT DISTINCT ON (
            pr.organization_id, lower(ltrim(pr.unique_corp_id, '0')), lower(pr.dependent_id)
        )
        pr.organization_id,
        pr.first_name,
        pr.last_name,
        pr.email,
        pr.unique_corp_id,
        pr.dependent_id,
        pr.date_of_birth,
        pr.work_state,
        pr.work_country,
        coalesce(pr.record, '{}')::jsonb,
        coalesce(pr.custom_attributes, '{}')::jsonb,
        pr.file_id,
        coalesce(pr.effective_range, eligibility.default_range()),
        pr.gender_code
    FROM records pr
    ON CONFLICT (  -- update any records with unique keys we have seen before
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
            gender_code = excluded.gender_code
)
INSERT INTO eligibility.member_versioned( -- write everything to member_versioned
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
        gender_code,
        employer_assigned_id,
        hash_value,
        hash_version
)
SELECT DISTINCT ON (  -- return the first record we have returned from file_parse_results
        pr.organization_id, lower(ltrim(pr.unique_corp_id, '0')), lower(pr.dependent_id)
        )
        pr.organization_id,
        pr.first_name,
        pr.last_name,
        pr.email,
        pr.unique_corp_id,
        pr.dependent_id,
        pr.date_of_birth,
        pr.work_state,
        pr.work_country,
        coalesce(pr.record, '{}')::jsonb,
        coalesce(pr.custom_attributes, '{}')::jsonb,
        pr.file_id,
        coalesce(pr.effective_range, eligibility.default_range()),
        pr.gender_code,
        pr.employer_assigned_id,
        hash_value,
        hash_version
FROM records pr
ON CONFLICT (hash_value, hash_version)
    DO UPDATE
            -- if we have seen this row before
            -- 1. Update file_id to the latest file
            -- 2. Set the effective_range upper to infinity and lower to the previous lower
        SET file_id = excluded.file_id,
            effective_range = daterange(lower(eligibility.member_versioned.effective_range), null);


-- name: bulk_persist_parsed_records_for_file_and_org_dual_write_hash
-- Upsert all the parsed records for a given file ID and organization ID to member and inserts to member_versioned.
-- This acts as an atomic "move" operation - delete the source and copy to the destination.
WITH records AS (
    DELETE FROM eligibility.file_parse_results -- grab the records from the file_parse_results
    WHERE file_id = :file_id AND organization_id = :organization_id
    RETURNING
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
        gender_code,
        employer_assigned_id,
        hash_value,
        hash_version
), member_insert AS (
    INSERT INTO eligibility.member(  -- insert unique records from e9y.member and return what was inserted
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
        gender_code
    )
    SELECT DISTINCT ON (
            pr.organization_id, lower(ltrim(pr.unique_corp_id, '0')), lower(pr.dependent_id)
        )
        pr.organization_id,
        pr.first_name,
        pr.last_name,
        pr.email,
        pr.unique_corp_id,
        pr.dependent_id,
        pr.date_of_birth,
        pr.work_state,
        pr.work_country,
        coalesce(pr.record, '{}')::jsonb,
        coalesce(pr.custom_attributes, '{}')::jsonb,
        pr.file_id,
        coalesce(pr.effective_range, eligibility.default_range()),
        pr.gender_code
    FROM records pr
    ON CONFLICT (  -- update any records with unique keys we have seen before
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
            gender_code = excluded.gender_code
)
INSERT INTO eligibility.member_versioned( -- write everything to member_versioned
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
        gender_code,
        employer_assigned_id,
        hash_value,
        hash_version
)
SELECT DISTINCT ON (  -- return the first record we have returned from file_parse_results
        pr.organization_id, lower(ltrim(pr.unique_corp_id, '0')), lower(pr.dependent_id)
        )
        pr.organization_id,
        pr.first_name,
        pr.last_name,
        pr.email,
        pr.unique_corp_id,
        pr.dependent_id,
        pr.date_of_birth,
        pr.work_state,
        pr.work_country,
        coalesce(pr.record, '{}')::jsonb,
        coalesce(pr.custom_attributes, '{}')::jsonb,
        pr.file_id,
        coalesce(pr.effective_range, eligibility.default_range()),
        pr.gender_code,
        pr.employer_assigned_id,
        hash_value,
        hash_version
FROM records pr
ON CONFLICT (hash_value, hash_version)
    DO UPDATE
            -- if we have seen this row before
            -- 1. Update file_id to the latest file
            -- 2. Set the effective_range upper to infinity and lower to the previous lower
        SET file_id = excluded.file_id,
            effective_range = daterange(lower(eligibility.member_versioned.effective_range), null);


-- name: set_work_mem
-- Set the work_mem to a value in MB
SET work_mem='2000MB'