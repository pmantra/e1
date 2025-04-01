-- Queries pertaining to the `eligibility.file` table.

-- name: all
-- Get all census file references.
SELECT * FROM eligibility.file;

-- name: get^
-- Get a census file reference.
SELECT * FROM eligibility.file WHERE id = :id;

-- name: for_ids
-- Get all census files for the provided IDs.
SELECT * FROM eligibility.file WHERE id = ANY(:ids);

-- name: get_latest_for_org^
-- Get the latest file for a given organization ID.
SELECT * FROM eligibility.file
WHERE organization_id = :organization_id
ORDER BY eligibility.file.created_at DESC
LIMIT 1;

-- name: get_one_before_latest_for_org^
-- Get the most recent file (the one before latest) for a given organization ID.
SELECT * FROM eligibility.file
WHERE organization_id = :organization_id
AND id < (
    SELECT MAX(id) FROM eligibility.file WHERE organization_id = :organization_id
)
ORDER BY id DESC
LIMIT 1;


-- name: get_names
-- Get the file name(s) for the given ID(s).
SELECT name FROM eligibility.file
WHERE id = ANY(:ids);

-- name: get_by_name
-- Get all files with the given name.
SELECT * from eligibility.file
WHERE file.name = :name
ORDER BY eligibility.file.created_at DESC;

-- name: get_by_name_for_org
-- Get all files with the given name.
SELECT * from eligibility.file
WHERE
    name = :name
    AND organization_id = :organization_id
ORDER BY eligibility.file.created_at DESC;

-- name: get_all_for_org
-- Get the latest file for a given organization ID.
SELECT * FROM eligibility.file
WHERE organization_id = :organization_id;

-- name: get_incomplete_for_org
-- Get all incomplete files for an organization.
SELECT * FROM eligibility.file
WHERE
    organization_id = :organization_id
    AND completed_at IS NULL
ORDER BY eligibility.file.created_at DESC;

-- name: get_pending_for_org
-- Get all pending files for an organization.
SELECT * FROM eligibility.file
WHERE
    organization_id = :organization_id
    AND started_at IS NULL
ORDER BY eligibility.file.created_at DESC;

-- name: get_completed_for_org
-- Get all completed files for an organization.
SELECT * FROM eligibility.file
WHERE
    organization_id = :organization_id
    AND completed_at IS NOT NULL
ORDER BY eligibility.file.created_at DESC;

-- name: get_incomplete
-- Get all incomplete files.
SELECT * FROM eligibility.file
WHERE
    completed_at IS NULL
ORDER BY eligibility.file.created_at DESC;

-- name: get_pending
-- Get all pending files..
SELECT * FROM eligibility.file
WHERE
    started_at IS NULL
ORDER BY eligibility.file.created_at DESC;

-- name: get_completed
-- Get all completed files.
SELECT * FROM eligibility.file
WHERE
    completed_at IS NOT NULL
ORDER BY eligibility.file.created_at DESC;


-- name: get_incomplete_org_ids_file_ids
-- Get the org & file ids for all incomplete files
SELECT id, name, organization_id FROM eligibility.file
WHERE completed_at IS NULL
ORDER BY eligibility.file.created_at DESC;

-- name: get_incomplete_by_org
-- Get all incomplete files, grouped by organization.
SELECT * from eligibility.incomplete_files_by_org;

-- name: get_success_count^
-- Get success count for given file_id.
SELECT success_count FROM eligibility.file
WHERE file.id = :id;

-- name: get_failure_count^
-- Get failure count for given file_id.
SELECT failure_count FROM eligibility.file
WHERE file.id = :id;

-- name: get_raw_count^
-- Get raw count for given file_id.
SELECT raw_count FROM eligibility.file
WHERE file.id = :id;
