-- migrate:up
CREATE OR REPLACE FUNCTION eligibility.migrate_file_parse_results_versioned(files bigint[]) RETURNS SETOF eligibility.member_versioned
    LANGUAGE sql
    AS $$
WITH records AS (
    DELETE FROM eligibility.file_parse_results
    WHERE file_id = ANY (files)
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
        effective_range
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
    coalesce(pr.effective_range, eligibility.default_range())
FROM records pr
RETURNING *
$$;


-- migrate:down
CREATE OR REPLACE FUNCTION eligibility.migrate_file_parse_results_versioned(files bigint[]) RETURNS SETOF eligibility.member_versioned
    LANGUAGE sql
    AS $$
WITH records AS (
    DELETE FROM eligibility.file_parse_results
    WHERE file_id = ANY (files)
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
INSERT INTO eligibility.member_versioned(
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
RETURNING *
$$;
