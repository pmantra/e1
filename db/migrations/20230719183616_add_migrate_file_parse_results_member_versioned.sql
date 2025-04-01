-- migrate:up

CREATE FUNCTION eligibility.migrate_file_parse_results_dual_write(files bigint[]) RETURNS SETOF eligibility.member
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
            effective_range = excluded.effective_range
    RETURNING *
), member_versioned_insert AS (
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
)
SELECT * FROM member_insert;
$$;

CREATE FUNCTION eligibility.migrate_file_parse_results_versioned(files bigint[]) RETURNS SETOF eligibility.member_versioned
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

-- migrate:down

DROP FUNCTION IF EXISTS eligibility.migrate_file_parse_results_dual_write;
DROP FUNCTION IF EXISTS eligibility.migrate_file_parse_results_versioned;