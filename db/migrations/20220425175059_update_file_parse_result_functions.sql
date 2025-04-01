-- migrate:up
-- Generate what we expect the resulting member data to look like once all pending files are finished processing
-- this is done combining existing member data with what we have in a pending state
-- take in an array/set of file_ids and a show_missing flag
CREATE OR REPLACE FUNCTION eligibility.merge_file_parse_results_members(
    "files" bigint[], "show_missing" bool
) RETURNS TABLE (
    id bigint,
    organization_id bigint,
    first_name eligibility.iwstext,
    last_name eligibility.iwstext,
    email eligibility.iwstext,
    unique_corp_id eligibility.ilztext,
    dependent_id eligibility.citext,
    date_of_birth date,
    work_state eligibility.iwstext,
    record jsonb,
    file_id bigint,
    effective_range daterange,
    errors eligibility.citext[],
    warnings eligibility.citext[],
    created_at timestamptz,
    updated_at timestamptz,
    is_missing bool
) LANGUAGE sql IMMUTABLE
AS
$$
    SELECT
        existing.id,
        (
            -- apply our 'pending' changes to the record, otherwise display the existing member info
            CASE WHEN parsed.id IS NULL
                THEN existing.parsed_record
                ELSE parsed.parsed_record
                END
        ).*,
        COALESCE(parsed.errors, '{}')::eligibility.citext[]   AS errors,
        COALESCE(parsed.warnings, '{}')::eligibility.citext[] AS warnings,
        existing.created_at,
        existing.updated_at,
        CASE WHEN parsed.id IS NULL THEN TRUE ELSE FALSE END AS is_missing
    FROM (
        -- grab all the members that correspond to the organizations we care about. we don't filter just on file_id, in the
        -- case that we get multiple orgs in a single file.
        SELECT id,
            organization_id ,
            unique_corp_id ,
            dependent_id ,
            m::eligibility.parsed_record AS parsed_record,
            created_at,
            updated_at
        FROM   eligibility."member" m
        WHERE  m.organization_id = ANY (
            SELECT organization_id
            FROM   eligibility.FILE
            WHERE  id = ANY (files)
        )
    ) AS existing
    FULL JOIN (
        -- grab all parsed records for the  fileIDs we care about
        SELECT
            id,
            organization_id,
            unique_corp_id,
            dependent_id,
            errors,
            warnings,
            fpr::eligibility.parsed_record AS parsed_record
        FROM
            eligibility.file_parse_results fpr
        WHERE
            file_id = ANY (files)
    ) AS parsed
    ON        existing.organization_id = parsed.organization_id
    AND       existing.unique_corp_id = parsed.unique_corp_id
    AND       existing.dependent_id = parsed.dependent_id
    WHERE
        coalesce(show_missing, TRUE)  OR parsed.id IS NOT NULL
$$;


-- migrate:down


CREATE OR REPLACE FUNCTION eligibility.merge_file_parse_results_members(
    "files" bigint[], "show_missing" bool
) RETURNS TABLE (
    id bigint,
    organization_id bigint,
    first_name eligibility.iwstext,
    last_name eligibility.iwstext,
    email eligibility.iwstext,
    unique_corp_id eligibility.ilztext,
    dependent_id eligibility.citext,
    date_of_birth date,
    work_state eligibility.iwstext,
    record jsonb,
    file_id bigint,
    effective_range daterange,
    errors eligibility.citext[],
    warnings eligibility.citext[],
    created_at timestamptz,
    updated_at timestamptz,
    is_missing bool
) LANGUAGE sql IMMUTABLE
AS
$$
    SELECT
        member.id,
        (
            CASE WHEN fpr.id IS NULL
                THEN member::eligibility.parsed_record
                ELSE fpr::eligibility.parsed_record
                END
        ).*,
        coalesce(fpr.errors, '{}')::eligibility.citext[] as errors,
        coalesce(fpr.warnings, '{}')::eligibility.citext[] as warnings,
        member.created_at,
        member.updated_at,
        CASE WHEN fpr.id IS NULL THEN TRUE ELSE FALSE END AS is_missing
    FROM eligibility.member
    INNER JOIN eligibility.file f
        ON member.organization_id = f.organization_id
        AND f.id = ANY (files)
    LEFT JOIN eligibility.file_parse_results fpr
        ON fpr.file_id = f.id
        AND member.organization_id = fpr.organization_id
        AND member.unique_corp_id = fpr.unique_corp_id
        AND member.dependent_id = fpr.dependent_id
    WHERE
        coalesce(show_missing, TRUE) OR fpr.id IS NOT NULL
$$;

