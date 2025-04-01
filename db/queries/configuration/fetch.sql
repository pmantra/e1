-- Queries pertaining to the `eligiblity.configuration` table.

-- name: all
-- Get all census file parsing configurations.
SELECT
    configuration.*
FROM eligibility.configuration
;

-- name: get^
-- Get a parsing configuration for a given organization.
SELECT
    configuration.*
FROM eligibility.configuration
WHERE configuration.organization_id = :organization_id;

-- name: get_for_orgs
-- Get a parsing configuration for a given organization.
SELECT
    configuration.*
FROM eligibility.configuration
WHERE configuration.organization_id = ANY(:organization_ids);

-- name: get_for_file^
-- Get a parsing configuration for a given file ID.
SELECT
    configuration.*
FROM eligibility.configuration
INNER JOIN file f on configuration.organization_id = f.organization_id
WHERE f.id = :file_id;


-- name: get_for_files
-- Get a parsing configuration for a given file ID.
SELECT DISTINCT
    configuration.*
FROM eligibility.configuration
INNER JOIN file f on configuration.organization_id = f.organization_id
WHERE f.id = ANY(:file_ids)
ORDER BY configuration.organization_id;


-- name: get_by_directory_name^
-- Get a parsing configuration for a given organization.
SELECT
    configuration.*
FROM eligibility.configuration
WHERE configuration.directory_name = :directory_name;

-- name: get_sub_orgs_by_data_provider
SELECT
    c.*
FROM eligibility.configuration c
INNER JOIN eligibility.organization_external_id oei
ON c.organization_id = oei.organization_id
WHERE oei.data_provider_organization_id = :data_provider_org_id;

-- name: get_by_external_id^
-- Get a configuration for an organization via an externally-provided ID.
SELECT
    configuration.*
FROM eligibility.configuration
    INNER JOIN eligibility.organization_external_id
        ON configuration.organization_id = organization_external_id.organization_id
        AND organization_external_id.source = :source
        AND organization_external_id.external_id = :external_id;

-- name: get_external_ids
-- Get the external identities for a given organization.
SELECT source, external_id, organization_id, data_provider_organization_id
FROM eligibility.organization_external_id
WHERE organization_id = :organization_id;


-- name: get_all_external_ids
-- Get all configured external ids
SELECT * FROM eligibility.organization_external_id;


-- name: get_external_ids_by_data_provider_id
SELECT *
FROM eligibility.organization_external_id
WHERE data_provider_organization_id = :organization_id;


-- name: get_external_ids_by_value_and_data_provider
SELECT *
FROM eligibility.organization_external_id
WHERE external_id = :external_id
AND data_provider_organization_id = :data_provider_organization_id;


-- name: get_external_ids_by_value_and_source
SELECT *
FROM eligibility.organization_external_id
WHERE external_id = :external_id
AND source = :source;


-- name: get_external_org_infos_by_value_and_source
SELECT oei.organization_id, c.activated_at, c.directory_name
FROM eligibility.organization_external_id oei
INNER JOIN eligibility.configuration c ON oei.organization_id = c.organization_id
WHERE oei.external_id = :external_id
AND oei.source = :source;


-- name: get_external_org_infos_by_value_and_data_provider
SELECT oei.organization_id, c.activated_at, c.directory_name
FROM eligibility.organization_external_id oei
INNER JOIN eligibility.configuration c ON oei.organization_id = c.organization_id
WHERE oei.external_id = :external_id
AND oei.data_provider_organization_id = :data_provider_organization_id;


-- name: get_external_ids_by_fuzzy_value_and_source
SELECT *
FROM eligibility.organization_external_id
WHERE source = :source
AND external_id like '%' || :external_id || '%';

-- name: get_configs_for_optum
SELECT *
FROM eligibility.configuration
WHERE organization_id NOT IN
(SELECT DISTINCT organization_id FROM eligibility.file);