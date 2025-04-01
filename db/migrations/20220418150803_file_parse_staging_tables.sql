-- migrate:up

CREATE TABLE IF NOT EXISTS eligibility.file_parse_errors (
    id BIGSERIAL PRIMARY KEY NOT NULL,
    file_id BIGINT REFERENCES eligibility.file (id) ON DELETE NO ACTION,
    organization_id BIGINT REFERENCES eligibility.configuration (organization_id) ON DELETE CASCADE NOT NULL,
    record JSONB NOT NULL,
    errors text[] NOT NULL DEFAULT '{}'::text[],
    warnings text[] NOT NULL DEFAULT '{}'::text[],
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_file_parse_errors_file_id ON eligibility.file_parse_errors(file_id);
CREATE INDEX IF NOT EXISTS idx_file_parse_errors_org_id ON eligibility.file_parse_errors(organization_id);
CREATE INDEX IF NOT EXISTS idx_file_parse_errors_warnings ON eligibility.file_parse_errors USING gin (warnings);
CREATE INDEX IF NOT EXISTS idx_file_parse_errors_errors ON eligibility.file_parse_errors USING gin (errors);
CREATE INDEX IF NOT EXISTS idx_file_parse_errors_record ON eligibility.file_parse_errors USING gin (record);

CREATE TABLE IF NOT EXISTS eligibility.file_parse_results (
    id BIGSERIAL PRIMARY KEY NOT NULL,
    organization_id BIGINT REFERENCES eligibility.configuration(organization_id)
        ON DELETE CASCADE NOT NULL,
    file_id BIGINT REFERENCES eligibility.file(id) ON DELETE CASCADE ,
    first_name eligibility.iwstext NOT NULL DEFAULT '',
    last_name eligibility.iwstext NOT NULL DEFAULT '',
    email eligibility.iwstext NOT NULL DEFAULT '',
    unique_corp_id eligibility.ilztext NOT NULL DEFAULT '',
    dependent_id eligibility.citext NOT NULL DEFAULT '',
    date_of_birth DATE NOT NULL,
    work_state eligibility.iwstext,
    record JSONB,
    errors text[] NOT NULL DEFAULT '{}'::text[],
    warnings text[] NOT NULL DEFAULT '{}'::text[],
    effective_range daterange DEFAULT eligibility.default_range(),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_file_parse_results_file_id ON eligibility.file_parse_results(file_id);
CREATE INDEX IF NOT EXISTS idx_file_parse_results_org_id ON eligibility.file_parse_results(organization_id);
CREATE INDEX IF NOT EXISTS idx_file_parse_results_warnings ON eligibility.file_parse_results USING gin (warnings);
CREATE INDEX IF NOT EXISTS idx_file_parse_results_errors ON eligibility.file_parse_results USING gin (errors);
CREATE INDEX IF NOT EXISTS idx_file_parse_results_record ON eligibility.file_parse_results USING gin (record);
CREATE INDEX IF NOT EXISTS idx_parse_results_member_identity ON eligibility.file_parse_results
(
    organization_id, ltrim(lower(unique_corp_id), '0'), lower(dependent_id) text_pattern_ops
);

ALTER TYPE eligibility.parsed_record
    ALTER ATTRIBUTE unique_corp_id TYPE eligibility.ilztext,
    ALTER ATTRIBUTE dependent_id TYPE eligibility.citext,
    ALTER ATTRIBUTE record TYPE jsonb,
    ALTER ATTRIBUTE first_name TYPE eligibility.iwstext,
    ALTER ATTRIBUTE last_name TYPE eligibility.iwstext,
    ALTER ATTRIBUTE work_state TYPE eligibility.iwstext,
    ALTER ATTRIBUTE email TYPE eligibility.iwstext
    ;


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

CREATE OR REPLACE FUNCTION eligibility.get_parsed_record_from_member(
    "record" eligibility.member
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

DROP CAST IF EXISTS (eligibility.file_parse_results AS eligibility.parsed_record);
DROP CAST IF EXISTS (eligibility.member AS eligibility.parsed_record);
CREATE CAST (eligibility.file_parse_results AS eligibility.parsed_record)
    WITH FUNCTION eligibility.get_parsed_record_from_file(eligibility.file_parse_results) AS IMPLICIT;
CREATE CAST ( eligibility.member AS eligibility.parsed_record)
    WITH FUNCTION eligibility.get_parsed_record_from_member(eligibility.member) AS IMPLICIT;

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

DROP VIEW IF EXISTS eligibility.file_parse_results_members_merged CASCADE;
CREATE VIEW eligibility.file_parse_results_members_merged AS (
    SELECT * FROM eligibility.merge_file_parse_results_members(
        (SELECT array_agg((SELECT DISTINCT file_id from eligibility.file_parse_results)))::bigint[],
        TRUE
        )
);


-- For each organization, grab the number of pending members (i.e. members who would be impacted if we resolve our pending records), the config for
-- the organization, and details around files the org has 'pending' i.e. not fully processed
DROP VIEW IF EXISTS eligibility.incomplete_files_by_org CASCADE;
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

CREATE OR REPLACE FUNCTION eligibility.migrate_file_parse_results("files" bigint[])
RETURNS SETOF eligibility.member LANGUAGE sql VOLATILE AS
$$
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
$$;

-- migrate:down

DROP FUNCTION IF EXISTS eligibility.migrate_file_parse_results(files bigint[]);
DROP VIEW IF EXISTS eligibility.incomplete_files_by_org;
DROP VIEW IF EXISTS eligibility.file_parse_results_members_merged;
DROP FUNCTION IF EXISTS eligibility.merge_file_parse_results_members(files bigint[], bool) CASCADE;
DROP CAST IF EXISTS (eligibility.member AS eligibility.parsed_record);
DROP CAST IF EXISTS (eligibility.file_parse_results AS eligibility.parsed_record);
DROP FUNCTION IF EXISTS eligibility.get_parsed_record_from_member(eligibility.member);
DROP FUNCTION IF EXISTS eligibility.get_parsed_record_from_file(eligibility.file_parse_results);
DROP TABLE IF EXISTS eligibility.file_parse_results CASCADE;
DROP TABLE IF EXISTS eligibility.file_parse_errors CASCADE;

