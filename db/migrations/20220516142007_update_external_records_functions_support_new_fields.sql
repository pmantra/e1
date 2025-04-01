-- region migrateup
-- migrate:up

DROP TYPE eligibility.external_record;
CREATE TYPE eligibility.external_record AS (
    first_name TEXT,
    last_name TEXT,
    email TEXT,
	unique_corp_id eligibility.ilztextci,
    dependent_id TEXT collate eligibility.ci,
    date_of_birth DATE,
    work_state TEXT,
    record TEXT,
    effective_range DATERANGE,
    source TEXT,
    external_id TEXT,
    external_name TEXT,
    received_ts BIGINT,
    do_not_contact eligibility.iwstext,
    gender_code eligibility.iwstext

);


-- Remove the functions/casts that depend on parsed_record
DROP VIEW eligibility.incomplete_files_by_org; -- done
DROP VIEW eligibility.file_parse_results_members_merged; -- done
DROP FUNCTION eligibility.merge_file_parse_results_members; -- done
DROP CAST (eligibility.member AS eligibility.parsed_record); -- done
DROP FUNCTION eligibility.get_parsed_record_from_member; -- done
DROP CAST  (eligibility.file_parse_results AS eligibility.parsed_record); -- done
DROP  FUNCTION eligibility.get_parsed_record_from_file; -- done


--region Actually update parsed record

DROP TYPE eligibility.parsed_record;
CREATE TYPE eligibility.parsed_record AS (
	organization_id bigint,
	first_name eligibility.iwstext,
	last_name eligibility.iwstext,
	email eligibility.iwstext,
	unique_corp_id eligibility.ilztext,
	dependent_id eligibility.citext,
	date_of_birth date,
	work_state eligibility.iwstext,
    do_not_contact eligibility.iwstext,
    gender_code eligibility.iwstext,
	record jsonb,
	file_id bigint,
	effective_range daterange
);


ALTER TABLE eligibility.file_parse_results
    ADD do_not_contact eligibility.iwstext,
    ADD gender_code eligibility.iwstext;

ALTER TABLE eligibility.member
    DROP provided_gender_code,
    ADD gender_code eligibility.iwstext;

--endregion

-- region Recreate functions for parsed_record - need to apply them in reverse order they were dropped in


CREATE OR REPLACE FUNCTION eligibility.get_parsed_record_from_file(
    "record" eligibility.file_parse_results
    ) RETURNS eligibility.parsed_record LANGUAGE sql IMMUTABLE AS
$$
    SELECT (
         record.organization_id::bigint,
         record.first_name::eligibility.iwstext,
         record.last_name::eligibility.iwstext,
         record.email::eligibility.iwstext,
         record.unique_corp_id::eligibility.ilztext,
         record.dependent_id::eligibility.citext,
         record.date_of_birth::date,
         record.work_state::eligibility.iwstext,
         record.do_not_contact::eligibility.iwstext,
         record.gender_code::eligibility.iwstext,
         record.record::jsonb,
         record.file_id::bigint,
         record.effective_range::daterange
    )::eligibility.parsed_record
$$;




CREATE CAST (eligibility.file_parse_results AS eligibility.parsed_record) WITH FUNCTION eligibility.get_parsed_record_from_file(eligibility.file_parse_results) AS IMPLICIT;

CREATE FUNCTION eligibility.get_parsed_record_from_member(record eligibility.member) RETURNS eligibility.parsed_record
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT (
         record.organization_id::bigint,
         record.first_name::eligibility.iwstext,
         record.last_name::eligibility.iwstext,
         record.email::eligibility.iwstext,
         record.unique_corp_id::eligibility.ilztext,
         record.dependent_id::eligibility.citext,
         record.date_of_birth::date,
         record.work_state::eligibility.iwstext,
         record.do_not_contact::eligibility.iwstext,
         record.gender_code::eligibility.iwstext,
         record.record::jsonb,
         record.file_id::bigint,
         record.effective_range::daterange

    )::eligibility.parsed_record
$$;

CREATE CAST (eligibility.member AS eligibility.parsed_record) WITH FUNCTION eligibility.get_parsed_record_from_member(eligibility.member) AS IMPLICIT;

CREATE FUNCTION eligibility.merge_file_parse_results_members(files bigint[], show_missing boolean) RETURNS TABLE(id bigint, organization_id bigint, first_name eligibility.iwstext,
    last_name eligibility.iwstext, email eligibility.iwstext, unique_corp_id eligibility.ilztext, dependent_id eligibility.citext, date_of_birth date, work_state eligibility.iwstext,
    do_not_contact eligibility.iwstext, gender_code eligibility.iwstext,
    record jsonb, file_id bigint, effective_range daterange, errors eligibility.citext[], warnings eligibility.citext[], created_at timestamp with time zone, updated_at timestamp with time zone, is_missing boolean)
    LANGUAGE sql IMMUTABLE
    AS $$
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

CREATE VIEW eligibility.file_parse_results_members_merged AS
 SELECT merge_file_parse_results_members.*
   FROM eligibility.merge_file_parse_results_members(( SELECT array_agg(( SELECT DISTINCT file_parse_results.file_id
                   FROM eligibility.file_parse_results)) AS array_agg), true) merge_file_parse_results_members(id, organization_id, first_name, last_name, email, unique_corp_id, dependent_id, date_of_birth, work_state, gender_code, do_not_contact, record, file_id, effective_range, errors, warnings, created_at, updated_at, is_missing);


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

-- endregion

ALTER TABLE eligibility.member_address ADD UNIQUE (member_id);

-- endregion migrateup



-- migrate:down
-- region migratedown

DROP TYPE eligibility.external_record;
CREATE TYPE eligibility.external_record AS (
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    unique_corp_id eligibility.ilztextci,
    dependent_id TEXT collate eligibility.ci,
    date_of_birth DATE,
    work_state TEXT,
    record TEXT,
    effective_range DATERANGE,
    source TEXT,
    external_id TEXT,
    external_name TEXT,
    received_ts BIGINT

);


-- region remove functions
-- Remove the functions/casts that depend on parsed_record
DROP VIEW eligibility.incomplete_files_by_org; -- done
DROP VIEW eligibility.file_parse_results_members_merged; -- done
DROP FUNCTION eligibility.merge_file_parse_results_members; -- done
DROP CAST (eligibility.member AS eligibility.parsed_record); -- done
DROP FUNCTION eligibility.get_parsed_record_from_member; -- done
DROP CAST  (eligibility.file_parse_results AS eligibility.parsed_record); -- done
DROP  FUNCTION eligibility.get_parsed_record_from_file; -- done
-- endregion remove functions


--region Actually update parsed record

DROP TYPE eligibility.parsed_record;
CREATE TYPE eligibility.parsed_record AS (
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
	effective_range daterange
);


ALTER TABLE eligibility.file_parse_results
    DROP do_not_contact,
    DROP gender_code;

ALTER TABLE eligibility.member
    ADD  provided_gender_code eligibility.iwstext,
    DROP gender_code;

--endregion Actually update parsed record

-- region Recreate functions for parsed_record - need to apply them in reverse order they were dropped in


CREATE OR REPLACE FUNCTION eligibility.get_parsed_record_from_file(
    "record" eligibility.file_parse_results
    ) RETURNS eligibility.parsed_record LANGUAGE sql IMMUTABLE AS
$$
    SELECT (
         record.organization_id::bigint,
         record.first_name::eligibility.iwstext,
         record.last_name::eligibility.iwstext,
         record.email::eligibility.iwstext,
         record.unique_corp_id::eligibility.ilztext,
         record.dependent_id::eligibility.citext,
         record.date_of_birth::date,
         record.work_state::eligibility.iwstext,
         record.record::jsonb,
         record.file_id::bigint,
         record.effective_range::daterange
    )::eligibility.parsed_record
$$;




CREATE CAST (eligibility.file_parse_results AS eligibility.parsed_record) WITH FUNCTION eligibility.get_parsed_record_from_file(eligibility.file_parse_results) AS IMPLICIT;

CREATE FUNCTION eligibility.get_parsed_record_from_member(record eligibility.member) RETURNS eligibility.parsed_record
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT (
         record.organization_id::bigint,
         record.first_name::eligibility.iwstext,
         record.last_name::eligibility.iwstext,
         record.email::eligibility.iwstext,
         record.unique_corp_id::eligibility.ilztext,
         record.dependent_id::eligibility.citext,
         record.date_of_birth::date,
         record.work_state::eligibility.iwstext,
         record.record::jsonb,
         record.file_id::bigint,
         record.effective_range::daterange

    )::eligibility.parsed_record
$$;

CREATE CAST (eligibility.member AS eligibility.parsed_record) WITH FUNCTION eligibility.get_parsed_record_from_member(eligibility.member) AS IMPLICIT;

CREATE FUNCTION eligibility.merge_file_parse_results_members(files bigint[], show_missing boolean) RETURNS TABLE(id bigint, organization_id bigint, first_name eligibility.iwstext,
    last_name eligibility.iwstext, email eligibility.iwstext, unique_corp_id eligibility.ilztext, dependent_id eligibility.citext, date_of_birth date, work_state eligibility.iwstext,
    record jsonb, file_id bigint, effective_range daterange, errors eligibility.citext[], warnings eligibility.citext[], created_at timestamp with time zone, updated_at timestamp with time zone, is_missing boolean)
    LANGUAGE sql IMMUTABLE
    AS $$
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

CREATE VIEW eligibility.file_parse_results_members_merged AS
 SELECT merge_file_parse_results_members.*
   FROM eligibility.merge_file_parse_results_members(( SELECT array_agg(( SELECT DISTINCT file_parse_results.file_id
                   FROM eligibility.file_parse_results)) AS array_agg), true) merge_file_parse_results_members(id, organization_id, first_name, last_name, email, unique_corp_id, dependent_id, date_of_birth, work_state, record, file_id, effective_range, errors, warnings, created_at, updated_at, is_missing);


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


ALTER TABLE eligibility.member_address DROP CONSTRAINT member_address_member_id_key;

-- endregion

-- endregion migratedown