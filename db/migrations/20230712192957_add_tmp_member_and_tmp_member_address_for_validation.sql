-- migrate:up

create table eligibility.tmp_file (like eligibility.file including all);
create table eligibility.tmp_member (like eligibility.member including all);
create table eligibility.tmp_member_address (like eligibility.member_address including all);
create table eligibility.tmp_file_parse_results (like eligibility.file_parse_results including all);
create table eligibility.tmp_file_parse_errors (like eligibility.file_parse_errors including all);

create function eligibility.tmp_migrate_file_parse_results(files bigint[]) returns SETOF eligibility.tmp_member
    language sql
as
$$
WITH records AS (
    DELETE FROM eligibility.tmp_file_parse_results
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
INSERT INTO eligibility.tmp_member(
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
$$;

alter function eligibility.tmp_migrate_file_parse_results(bigint[]) owner to postgres;

-- migrate:down

DROP FUNCTION IF EXISTS eligibility.tmp_migrate_file_parse_results;
DROP TABLE IF EXISTS eligibility.tmp_file CASCADE;
DROP TABLE IF EXISTS eligibility.tmp_member_address CASCADE;
DROP TABLE IF EXISTS eligibility.tmp_file_parse_results CASCADE;
DROP TABLE IF EXISTS eligibility.tmp_file_parse_errors CASCADE;
DROP TABLE IF EXISTS eligibility.tmp_member CASCADE;

