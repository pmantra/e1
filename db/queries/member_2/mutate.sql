-- Mutations pertaining to the `eligibility.member_2` table.

-- name: persist<!
-- Create or Update a Member record for an organization.
INSERT INTO eligibility.member_2(
    id,
    version,
    organization_id,
    first_name,
    last_name,
    email,
    unique_corp_id,
    dependent_id,
    date_of_birth,
    work_state,
    work_country,
    record,
    custom_attributes,
    effective_range,
    do_not_contact,
    gender_code,
    employer_assigned_id
)
VALUES (
    :id,
    :version,
    :organization_id,
    :first_name,
    :last_name,
    :email,
    :unique_corp_id,
    :dependent_id,
    :date_of_birth,
    :work_state,
    :work_country,
    :record,
    :custom_attributes,
    coalesce(:effective_range, eligibility.default_range()),
    :do_not_contact,
    :gender_code,
    :employer_assigned_id
)
RETURNING *;

--name: set_updated_at<!
-- Set the updated_at column for a single record
-- This should only be used for testing, so we can manually set the time a record was created
UPDATE eligibility.member_2
SET updated_at = :updated_at
where id = :id;

-- name: bulk_persist
-- Create or Update a series of Member records.
WITH member_2_records AS (
    SELECT (unnest(:records::eligibility.member_2[])::eligibility.member_2).*
)
INSERT INTO eligibility.member_2(
    id,
    version,
    organization_id,
    first_name,
    last_name,
    email,
    unique_corp_id,
    dependent_id,
    date_of_birth,
    work_state,
    work_country,
    record,
    custom_attributes,
    effective_range,
    do_not_contact,
    gender_code,
    employer_assigned_id
)
SELECT
    m2r.id,
    m2r.version,
    m2r.organization_id,
    m2r.first_name,
    m2r.last_name,
    m2r.email,
    m2r.unique_corp_id,
    coalesce(m2r.dependent_id, ''),
    m2r.date_of_birth,
    m2r.work_state,
    m2r.work_country,
    coalesce(m2r.record, '{}')::jsonb,
    coalesce(m2r.custom_attributes, '{}')::jsonb,
    coalesce(m2r.effective_range, eligibility.default_range()),
    m2r.do_not_contact,
    m2r.gender_code,
    m2r.employer_assigned_id
FROM member_2_records m2r
ON CONFLICT (id) DO NOTHING
RETURNING *;
