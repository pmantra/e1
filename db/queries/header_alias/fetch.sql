-- Queries pertaining to the `eligiblity.configuration` table.

-- name: all
-- Get all header aliases.
SELECT *
FROM eligibility.header_alias
;

-- name: get^
-- Get a single header alias.
SELECT *
FROM eligibility.header_alias
WHERE id = :id;

-- name: get_org_header_alias^
-- Get the alias an organization has mapped for a specific header, if any.
SELECT *
FROM eligibility.header_alias
WHERE organization_id = :organization_id
    AND header = :header;

-- name: get_for_org
-- Get all header aliases for an organization ID.
SELECT *
FROM eligibility.header_alias
WHERE header_alias.organization_id = :organization_id;

-- name: get_for_file
-- Get all header alias for a file ID
SELECT header_alias.*
FROM eligibility.header_alias
INNER JOIN file f on header_alias.organization_id = f.organization_id
WHERE f.id = :file_id;


-- name: get_for_files
-- Get all header alias for the given file IDs
SELECT DISTINCT header_alias.*
FROM eligibility.header_alias
INNER JOIN file f on header_alias.organization_id = f.organization_id
WHERE f.id = ANY(:file_ids)
ORDER BY header_alias.organization_id;

-- name: get_header_mapping$
-- Get a header mapping if `internal-header -> org-alias` for a given organization.
SELECT eligibility.get_header_mapping(:organization_id);

-- name: get_affiliations_header_for_org
SELECT *
FROM eligibility.header_alias
WHERE alias in ('client_id', 'customer_id') and organization_id = :organization_id
