-- migrate:up
DROP VIEW eligibility.file_parse_results_members_merged CASCADE;

DROP FUNCTION IF EXISTS eligibility.merge_file_parse_results_members_for_file (
	file bigint
);

DROP FUNCTION IF EXISTS eligibility.merge_file_parse_results_members_for_files (
	files bigint[]
);

create or replace function eligibility.merge_file_parse_results_members_for_file (
	file_identifier bigint
)
    returns TABLE(status text, file_id bigint, organization_id bigint, unique_corp_id eligibility.ilztext, dependent_id eligibility.citext)
    immutable
    language sql
as
$$
WITH file_records AS (
-- Only grab file records that are part of the file
    SELECT *
    FROM eligibility.file_parse_results fpr
    WHERE fpr.file_id = file_identifier::bigint
), member_records AS (
-- Only grab member records that are part of the organization attached to file
    SELECT *
    FROM eligibility.member m
    WHERE m.organization_id = (
        SELECT organization_id
        FROM eligibility.file f
        WHERE f.id = file_identifier::bigint
    )
)
-- Find the records that we want to update
SELECT
    'updated' as status,
    fr.file_id,
    fr.organization_id,
    fr.unique_corp_id,
    fr.dependent_id
FROM member_records m
INNER JOIN file_records fr
    USING(organization_id, unique_corp_id, dependent_id)
---------
    UNION
-- Find the records that are new
SELECT
    'new' as status,
    fr.file_id,
    fr.organization_id,
    fr.unique_corp_id,
    fr.dependent_id
FROM file_records fr
LEFT JOIN member_records m
    USING(organization_id, unique_corp_id, dependent_id)
WHERE m.organization_id IS NULL
    AND m.unique_corp_id IS NULL
    AND m.dependent_id IS NULL
---------
    UNION
-- Find the records that are missing
SELECT
    'expired' as status,
    file_identifier,
    m.organization_id,
    m.unique_corp_id,
    m.dependent_id
FROM member_records m
LEFT JOIN file_records fr
    USING(organization_id, unique_corp_id, dependent_id)
-- only grab member records from the organization tied to this file
WHERE fr.organization_id IS NULL
    AND fr.unique_corp_id IS NULL
    AND fr.dependent_id IS NULL;
$$;


create or replace function eligibility.merge_file_parse_results_members_for_files (
	file_identifiers bigint[]
)
returns TABLE(status text, file_id bigint, organization_id bigint, unique_corp_id eligibility.ilztext, dependent_id eligibility.citext)
language plpgsql
as $$
declare
    file_id bigint;
begin
    for file_id in
        select file.id
        from eligibility.file
        where file.id = ANY(file_identifiers::bigint[])
    loop
        RETURN QUERY select * from eligibility.merge_file_parse_results_members_for_file(file_id::bigint);
    end loop;
end; $$;


CREATE VIEW eligibility.file_parse_results_members_merged AS
    SELECT merge_file_parse_results_members_for_files.*
    FROM eligibility.merge_file_parse_results_members_for_files(
        ARRAY(
            SELECT DISTINCT file_parse_results.file_id
            FROM eligibility.file_parse_results)::bigint[]
        )
        merge_file_parse_results_members_for_files(status, file_id, organization_id, unique_corp_id, dependent_id);

CREATE VIEW eligibility.incomplete_files_by_org AS (
    -- First: get all the e9y records which aren't present in the parsed records.
    -- Grab all our file records that have *not* been marked as completed  and still have records in our temp parsing tables
    WITH pending AS (
        SELECT * FROM eligibility.file
        WHERE completed_at IS NULL
        AND file.id = ANY (
            (
                SELECT DISTINCT file_id
                FROM eligibility.file_parse_errors
                UNION DISTINCT
                SELECT DISTINCT file_id FROM eligibility.file_parse_results
            )
        )
    ),
    --   get the resulting information on for our members if we finished processing all our pending results
    merged_counts AS (
        SELECT
            file_id,
            organization_id,
            count(*) FILTER ( WHERE status = 'new' ) as new_total,
            count(*) FILTER ( WHERE status = 'updated' ) as updated_total,
            count(*) FILTER ( WHERE status = 'expired' ) as expired_total
        FROM eligibility.file_parse_results_members_merged
        GROUP BY 1, 2
    ),
    --    if a file has parse issues: grab files and the # of places we encounter an error in those files
    error_counts AS (
        SELECT file_id, count(*) as total
        FROM eligibility.file_parse_errors
        GROUP BY 1
    ),
    --    grab all members for a given organization
    total_members AS (
        SELECT member.organization_id, count(*) as total_members
        FROM eligibility.member
        INNER JOIN pending f ON f.organization_id = member.organization_id
        GROUP BY 1
    ),
    -- for each file, get the file's info, the number of total pending (missing), parsed, and errored out records
    f AS (
        SELECT
            p as file,
            coalesce(merged.expired_total, 0) as total_missing,
            coalesce(merged.new_total + merged.updated_total, 0) as total_parsed,
            coalesce(ec.total, 0) as total_errors
        FROM pending p
        LEFT JOIN (
            SELECT *
            FROM merged_counts
        ) merged on p.id = merged.file_id
            AND p.organization_id = merged.organization_id
        LEFT JOIN error_counts ec ON ec.file_id = p.id
    )
    -- Then pull in the organization's information
    SELECT
        c.organization_id as id,
        coalesce(tm.total_members, 0) as total_members,
        to_jsonb(c) as config,
        to_jsonb(array_agg(f)) as incomplete
    FROM eligibility.configuration c
    INNER JOIN f ON c.organization_id = (f.file).organization_id
    LEFT JOIN total_members tm on c.organization_id = tm.organization_id
    GROUP BY 1,2
    ORDER BY 2 DESC, 1
);


-- migrate:down

DROP VIEW eligibility.file_parse_results_members_merged CASCADE;

DROP FUNCTION IF EXISTS eligibility.merge_file_parse_results_members_for_file;

DROP FUNCTION IF EXISTS eligibility.merge_file_parse_results_members_for_files (
	file_identifiers bigint[]
);

CREATE VIEW eligibility.file_parse_results_members_merged AS
    SELECT merge_file_parse_results_members.*
        FROM eligibility.merge_file_parse_results_members(
            (
                SELECT array_agg(
                    (
                        SELECT DISTINCT file_parse_results.file_id
                        FROM eligibility.file_parse_results
                    )
                ) AS array_agg
            ),
            true
        ) merge_file_parse_results_members(
            id,
            organization_id,
            first_name,
            last_name,
            email,
            unique_corp_id,
            dependent_id,
            date_of_birth,
            work_state,
            gender_code,
            do_not_contact,
            employer_assigned_id,
            record,
            file_id,
            effective_range,
            errors,
            warnings,
            created_at,
            updated_at,
            is_missing
        );

CREATE VIEW eligibility.incomplete_files_by_org AS (
    -- First: get all the e9y records which aren't present in the parsed records.
    -- Grab all our file records that have *not* been marked as completed and still have records in our temp parsing tables
    WITH pending AS (
        SELECT * FROM eligibility.file
        WHERE completed_at IS NULL
        AND file.id = ANY (
            (
                SELECT DISTINCT file_id
                FROM eligibility.file_parse_errors
                UNION DISTINCT
                SELECT DISTINCT file_id FROM eligibility.file_parse_results
            )
        )
    ),
    --   get the resulting information on for our members if we finished processing all our pending results
    merged_counts AS (
        SELECT
            file_id, organization_id, is_missing, count(*) as total
        FROM eligibility.file_parse_results_members_merged
        GROUP BY 1, 2, 3
    ),
    --    if a file has parse issues: grab files and the # of places we encounter an error in those files
    error_counts AS (
        SELECT file_id, count(*) as total
        FROM eligibility.file_parse_errors
        GROUP BY 1
    ),
    --    grab all members for a given organization
    total_members AS (
        SELECT member.organization_id, count(*) as total_members
        FROM eligibility.member
        INNER JOIN pending f ON f.organization_id = member.organization_id
        GROUP BY 1
    ),
    -- for each file, get the file's info, the number of total pending (missing), parsed, and errored out records
    f AS (
        SELECT
            p as file,
            coalesce(mc.total, 0) as total_missing,
            coalesce(pc.total, 0) as total_parsed,
            coalesce(ec.total, 0) as total_errors
        FROM pending p
        LEFT JOIN (
            SELECT organization_id, sum(total) as total
            FROM merged_counts
            WHERE is_missing
            GROUP BY 1
        ) mc on p.organization_id = mc.organization_id
        LEFT JOIN (
            SELECT file_id, total
            FROM merged_counts
            WHERE is_missing IS FALSE
        ) pc on p.id = pc.file_id
        LEFT JOIN error_counts ec ON ec.file_id = p.id
    )
    -- Then pull in the organization's information
    SELECT
        c.organization_id as id,
        coalesce(tm.total_members, 0) as total_members,
        to_jsonb(c) as config,
        to_jsonb(array_agg(f)) as incomplete
    FROM eligibility.configuration c
    INNER JOIN f ON c.organization_id = (f.file).organization_id
    LEFT JOIN total_members tm on c.organization_id = tm.organization_id
    GROUP BY 1,2
    ORDER BY 2 DESC, 1
);
