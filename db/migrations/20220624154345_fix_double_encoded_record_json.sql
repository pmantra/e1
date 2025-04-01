-- migrate:up

UPDATE eligibility.member
SET record = (record #>> '{}')::jsonb
WHERE coalesce(record ->> 'file_id', record->>'external_id') IS NULL;

-- migrate:down

