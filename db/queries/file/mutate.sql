-- Mutations pertaining to the `eligibility.file` table

-- name: tmp_persist<!
-- Create or Update an org configuration with the provided information.
INSERT INTO eligibility.tmp_file(organization_id, name, encoding, error, success_count, failure_count, raw_count)
VALUES (:organization_id, :name, :encoding, :error, :success_count, :failure_count, :raw_count)
RETURNING *;

-- name: tmp_set_started_at<!
-- Set the datetime which processing began for this file.
UPDATE eligibility.tmp_file
SET started_at = now()
WHERE id = :id
RETURNING started_at;

-- name: tmp_set_encoding!
-- Set the content-encoding for this file.
UPDATE eligibility.tmp_file
SET encoding = :encoding
WHERE id = :id;

-- name: tmp_set_error!
-- Set the error for this file.
UPDATE eligibility.tmp_file
SET error = :error
WHERE id = :id;

-- name: tmp_set_completed_at<!
-- Set the datetime which processing completed for this file.
UPDATE eligibility.tmp_file
SET completed_at = now()
WHERE id = :id
RETURNING completed_at;

-- name: persist<!
-- Create or Update an org configuration with the provided information.
INSERT INTO eligibility.file(organization_id, name, encoding, error, success_count, failure_count, raw_count)
VALUES (:organization_id, :name, :encoding, :error, :success_count, :failure_count, :raw_count)
RETURNING *;

-- name: bulk_persist*!
-- Create or Update an org configuration with the provided information.
INSERT INTO eligibility.file(organization_id, name, encoding, error, success_count, failure_count, raw_count)
VALUES (:organization_id, :name, :encoding, :error, :success_count, :failure_count, :raw_count);

-- name: delete<!
-- Delete a census file reference.
DELETE FROM eligibility.file
WHERE id = :id
RETURNING *;

-- name: bulk_delete!
-- Delete a census file reference.
DELETE FROM eligibility.file
WHERE id = any(:ids);

-- name: set_started_at<!
-- Set the datetime which processing began for this file.
UPDATE eligibility.file
SET started_at = now()
WHERE id = :id
RETURNING started_at;

-- name: set_completed_at<!
-- Set the datetime which processing completed for this file.
UPDATE eligibility.file
SET completed_at = now()
WHERE id = :id
RETURNING completed_at;

-- name: set_encoding!
-- Set the content-encoding for this file.
UPDATE eligibility.file
SET encoding = :encoding
WHERE id = :id;

-- name: set_error!
-- Set the error for this file.
UPDATE eligibility.file
SET error = :error
WHERE id = :id;

-- name: set_file_count<!
-- Set all counts for file.
UPDATE eligibility.file
SET raw_count = :raw_count,
    success_count = :success_count,
    failure_count = :failure_count
WHERE id = :id;

