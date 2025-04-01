-- Queries pertaining to the `eligibility.file_parse_results` table.

-- name: get_all_file_parse_results
-- Get all file_parse_results records.
SELECT * FROM eligibility.file_parse_results;

-- name: get_file_parse_results_for_file
-- Get all the file_parse_results records for a given file ID.
SELECT * FROM eligibility.file_parse_results WHERE file_id = :file_id;


-- name: get_file_parse_results_for_org
-- Get all the file_parse_results records for a given org ID.
SELECT * FROM eligibility.file_parse_results WHERE organization_id = :org_id;


-- name: get_all_file_parse_errors
-- Get all file_parse_errors records.
SELECT * FROM eligibility.file_parse_errors;


--name: get_file_parse_errors_for_file
-- Get all the file_parse_errors records for a given file ID.
SELECT * FROM eligibility.file_parse_errors WHERE file_id = :file_id;


-- name: get_file_parse_errors_for_org
-- Get all the file_parse_results records for a given org ID.
SELECT * FROM eligibility.file_parse_errors WHERE organization_id = :org_id;


-- name: get_incomplete_files_by_org
-- Get all incomplete_files_by_org
SELECT * FROM eligibility.incomplete_files_by_org;

-- name: get_count_hashed_and_new_records
-- Query to return how many records in a file were hashed/represent duplicates from previous files
-- and how many records represent new inserts
SELECT *
FROM
	(
        SELECT count(*) as hashed_count
        FROM eligibility.member_versioned
        WHERE
            file_id = :file_id
        AND created_at < :created_at
	) AS hash_results,
	(
        SELECT count(*) as new_count
        FROM eligibility.member_versioned
        WHERE
            file_id = :file_id
        AND created_at >= :created_at
	) AS new_results;
