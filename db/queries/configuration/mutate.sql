-- Mutations pertaining to the `eligibility.configuration` table

-- name: persist<!
-- Create or Update an org configuration with the provided information.
INSERT INTO eligibility.configuration(organization_id, directory_name, email_domains, implementation, data_provider, activated_at, terminated_at, employee_only, medical_plan_only, eligibility_type)
VALUES (:organization_id, :directory_name, coalesce(:email_domains, '{}'::text[]), :implementation, :data_provider, :activated_at, :terminated_at, :employee_only, :medical_plan_only, :eligibility_type)
ON CONFLICT (organization_id)
    DO UPDATE SET
        directory_name = excluded.directory_name,
        email_domains = excluded.email_domains,
        -- FIXME: This effectively prevents us from clearing this field for now.
        --        we should probably remove this once we've stopped syncing configs from mono.
        implementation = coalesce(excluded.implementation, configuration.implementation),
        data_provider = excluded.data_provider,
        activated_at = excluded.activated_at,
        terminated_at = excluded.terminated_at,
        employee_only = excluded.employee_only,
        medical_plan_only = excluded.medical_plan_only,
        eligibility_type = excluded.eligibility_type
RETURNING *;

-- name: bulk_persist*!
-- Bulk Create or Update a series of org configurations.
INSERT INTO eligibility.configuration(organization_id, directory_name, email_domains, implementation, data_provider, activated_at, terminated_at, employee_only, medical_plan_only, eligibility_type)
VALUES (:organization_id, :directory_name, coalesce(:email_domains, '{}'::text[]), :implementation, :data_provider, :activated_at, :terminated_at, :employee_only, :medical_plan_only, :eligibility_type)
ON CONFLICT (organization_id)
    DO UPDATE SET
        directory_name = excluded.directory_name,
        email_domains = excluded.email_domains,
        -- FIXME: This effectively prevents us from clearing this field for now.
        --        we should probably remove this once we've stopped syncing configs from mono.
        implementation = coalesce(excluded.implementation, configuration.implementation),
        data_provider = excluded.data_provider,
        activated_at = excluded.activated_at,
        terminated_at = excluded.terminated_at,
        employee_only = excluded.employee_only,
        medical_plan_only = excluded.medical_plan_only,
        eligibility_type = excluded.eligibility_type;

-- name: delete<!
-- Delete an org configuration by organization ID.
DELETE FROM eligibility.configuration
WHERE organization_id = :organization_id
RETURNING *;

-- name: bulk_delete!
-- Delete an org configuration by organization ID.
DELETE FROM eligibility.configuration
WHERE organization_id = any(:organization_ids);

-- name: add_external_id<!
-- Add an external_id for a configuration
INSERT INTO eligibility.organization_external_id(source, external_id, organization_id, data_provider_organization_id)
VALUES (:source, :external_id, :organization_id, :data_provider_organization_id)
ON CONFLICT (source, external_id)
    DO UPDATE SET
        organization_id = excluded.organization_id,
        source = excluded.source,
        data_provider_organization_id = excluded.data_provider_organization_id,
        external_id = excluded.external_id
RETURNING *;

-- name: bulk_add_external_id*!
-- Add an external_id for a configuration
INSERT INTO eligibility.organization_external_id(source, data_provider_organization_id, external_id, organization_id)
VALUES (:source, :data_provider_organization_id, :external_id, :organization_id)
ON CONFLICT (source, external_id)
    DO UPDATE SET
        organization_id = excluded.organization_id,
        source = excluded.source,
        data_provider_organization_id = excluded.data_provider_organization_id,
        external_id = excluded.external_id;

-- name: delete_external_ids_for_org<!
-- Delete an organization_external_ids by organization ID.
DELETE FROM eligibility.organization_external_id
WHERE organization_id = :organization_id
RETURNING *;

-- name: delete_external_ids_for_data_provider_org
-- Delete an organization_external_ids by organization ID.
DELETE FROM eligibility.organization_external_id
WHERE data_provider_organization_id = :data_provider_organization_id
RETURNING *;

-- name: delete_all_external_ids<!
-- Delete all our existing external ID records
DELETE FROM eligibility.organization_external_id;
