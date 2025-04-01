-- migrate:up transaction:false
CREATE OR REPLACE VIEW eligibility.incomplete_files_by_org AS
    -- Determine incomplete files by the records in file_parse_results and file_parse_errors
    WITH incomplete_file_info AS (
        SELECT file_id, organization_id
        FROM
            (
            SELECT file_id, organization_id FROM eligibility.file_parse_results fpr
            UNION
            SELECT file_id, organization_id FROM eligibility.file_parse_errors fpe
            ) AS incomplete_file_info_superset
    ),
    -- Gets the data provider org IDs of each organization in incompete_file_info
    -- If the org does not have a data provider, its own org ID is returned instead
    -- This gives us a complete list of org IDs that should actually have files associated with it
    org_id_map AS (
        SELECT DISTINCT ifi.organization_id AS child_org_id, COALESCE(oei.data_provider_organization_id, ifi.organization_id) AS parent_org_id FROM incomplete_file_info ifi
        LEFT JOIN eligibility.organization_external_id oei ON ifi.organization_id = oei.organization_id
    ),
    -- Gets the set of most recently completed files for orgs identified in the org_id_map
    latest_files AS (
        SELECT file_3.organization_id, file_3.success_count FROM eligibility.file file_3 INNER JOIN (
            SELECT file_2.organization_id, max(file_2.completed_at) AS completed_at FROM eligibility.file file_2 WHERE file_2.organization_id IN (SELECT DISTINCT parent_org_id FROM org_id_map) GROUP BY file_2.organization_id
        ) latest_completion_timestamps ON file_3.organization_id = latest_completion_timestamps.organization_id AND file_3.completed_at = latest_completion_timestamps.completed_at
    ),
    -- Gets information about each incomplete file identified in incomplete_file_info and puts it into the expected format
    incomplete_files AS (
        SELECT file_1.*::record AS file, file_1.failure_count AS total_errors, file_1.success_count AS total_parsed,
            (CASE WHEN COALESCE(latest_files.success_count, 0) > file_1.success_count THEN (latest_files.success_count - file_1.success_count) ELSE 0 END) AS total_missing, file_1.created_at
        FROM eligibility.file AS file_1
        LEFT JOIN latest_files ON file_1.organization_id = latest_files.organization_id
        WHERE file_1.id IN (SELECT file_id FROM incomplete_file_info)
        ORDER BY file_1.created_at DESC
    )
    -- Groups the incomplete_files data by org ID and puts it into the expected format
    -- We need to include the success_count in the grouping criteria to be able to return it, but as
    -- we only get a single "latest file" per org, it should be the same as grouping only by org ID
    SELECT c.organization_id AS id, COALESCE(l_files.success_count, 0)::BIGINT AS total_members, to_jsonb(c.*) AS config, to_jsonb(array_agg(i_files.*)) AS incomplete
    FROM eligibility.configuration c
    INNER JOIN incomplete_files i_files ON c.organization_id = (i_files.file).organization_id
    LEFT JOIN latest_files l_files ON c.organization_id = l_files.organization_id
    GROUP BY (c.organization_id, l_files.success_count);
DROP VIEW IF EXISTS eligibility.file_parse_results_members_merged;
DROP FUNCTION IF EXISTS eligibility.merge_file_parse_results_members_for_files;
DROP FUNCTION IF EXISTS eligibility.merge_file_parse_results_members_for_file;

-- migrate:down transaction:false
CREATE OR REPLACE FUNCTION eligibility.merge_file_parse_results_members_for_file(file_identifier bigint) RETURNS TABLE(status text, file_id bigint, organization_id bigint, unique_corp_id eligibility.ilztext, dependent_id eligibility.citext)
    LANGUAGE sql IMMUTABLE
    AS $$
WITH parent_organization_id as (
    SELECT organization_id
    FROM eligibility.file f
    WHERE f.id = file_identifier::bigint
), file_records AS (
-- Only grab file records that are part of the file
    SELECT *
    FROM eligibility.file_parse_results fpr
    WHERE fpr.file_id = file_identifier::bigint
), member_records AS (
-- Only grab member records that are part of the organization attached to file
-- if we are looking at a data_provider org, grab member records from all sub-orgs
    SELECT *
    FROM eligibility.member m
    WHERE m.organization_id in (
        SELECT DISTINCT organization_id
        FROM eligibility.organization_external_id
        WHERE data_provider_organization_id = (SELECT organization_id FROM parent_organization_id)
        UNION
        SELECT organization_id
        FROM eligibility.configuration
        WHERE organization_id = (SELECT organization_id FROM parent_organization_id)
        AND data_provider = false
    )
)
-- Find the records that we want to update
SELECT
    'updated' as status,
    fr.file_id,
    (SELECT organization_id FROM parent_organization_id),
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
    (SELECT organization_id FROM parent_organization_id),
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
    (SELECT organization_id FROM parent_organization_id),
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

CREATE OR REPLACE FUNCTION eligibility.merge_file_parse_results_members_for_files(file_identifiers bigint[]) RETURNS TABLE(status text, file_id bigint, organization_id bigint, unique_corp_id eligibility.ilztext, dependent_id eligibility.citext)
    LANGUAGE plpgsql
    AS $$
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

CREATE OR REPLACE VIEW eligibility.file_parse_results_members_merged AS
 SELECT merge_file_parse_results_members_for_files.status,
    merge_file_parse_results_members_for_files.file_id,
    merge_file_parse_results_members_for_files.organization_id,
    merge_file_parse_results_members_for_files.unique_corp_id,
    merge_file_parse_results_members_for_files.dependent_id
   FROM eligibility.merge_file_parse_results_members_for_files(ARRAY( SELECT DISTINCT file_parse_results.file_id
           FROM eligibility.file_parse_results)) merge_file_parse_results_members_for_files(status, file_id, organization_id, unique_corp_id, dependent_id);

CREATE OR REPLACE VIEW eligibility.incomplete_files_by_org AS
 WITH pending AS (
         SELECT file.id,
            file.organization_id,
            file.name,
            file.encoding,
            file.started_at,
            file.completed_at,
            file.created_at,
            file.updated_at
           FROM eligibility.file
          WHERE ((file.completed_at IS NULL) AND (file.id IN ( SELECT DISTINCT file_parse_errors.file_id
                   FROM eligibility.file_parse_errors
                UNION
                 SELECT DISTINCT file_parse_results.file_id
                   FROM eligibility.file_parse_results)))
        ), merged_counts AS (
         SELECT file_parse_results_members_merged.file_id,
            file_parse_results_members_merged.organization_id,
            count(*) FILTER (WHERE (file_parse_results_members_merged.status = 'new'::text)) AS new_total,
            count(*) FILTER (WHERE (file_parse_results_members_merged.status = 'updated'::text)) AS updated_total,
            count(*) FILTER (WHERE (file_parse_results_members_merged.status = 'expired'::text)) AS expired_total
           FROM eligibility.file_parse_results_members_merged
          GROUP BY file_parse_results_members_merged.file_id, file_parse_results_members_merged.organization_id
        ), error_counts AS (
         SELECT file_parse_errors.file_id,
            count(*) AS total
           FROM eligibility.file_parse_errors
          GROUP BY file_parse_errors.file_id
        ), total_members AS (
         SELECT member.organization_id,
            count(*) AS total_members
           FROM (eligibility.member
             JOIN pending f_1 ON ((f_1.organization_id = member.organization_id)))
          GROUP BY member.organization_id
        ), f AS (
         SELECT p.*::record AS file,
            COALESCE(merged.expired_total, (0)::bigint) AS total_missing,
            COALESCE((merged.new_total + merged.updated_total), (0)::bigint) AS total_parsed,
            COALESCE(ec.total, (0)::bigint) AS total_errors
           FROM ((pending p
             LEFT JOIN ( SELECT merged_counts.file_id,
                    merged_counts.organization_id,
                    merged_counts.new_total,
                    merged_counts.updated_total,
                    merged_counts.expired_total
                   FROM merged_counts) merged ON (((p.id = merged.file_id) AND (p.organization_id = merged.organization_id))))
             LEFT JOIN error_counts ec ON ((ec.file_id = p.id)))
        )
 SELECT c.organization_id AS id,
    COALESCE(tm.total_members, (0)::bigint) AS total_members,
    to_jsonb(c.*) AS config,
    to_jsonb(array_agg(f.*)) AS incomplete
   FROM ((eligibility.configuration c
     JOIN f ON ((c.organization_id = (f.file).organization_id)))
     LEFT JOIN total_members tm ON ((c.organization_id = tm.organization_id)))
  GROUP BY c.organization_id, COALESCE(tm.total_members, (0)::bigint)
  ORDER BY COALESCE(tm.total_members, (0)::bigint) DESC, c.organization_id;
