-- migrate:up

-- This migration is a one-off- we previously had orgID 170 (Dentsu) providing us data via file
-- They recently switched their data source to being from Kafka/Optum
-- Unfortunately, before they switched over, the data that had been provided via file was not invalidated
-- so users were able to still sign up under file-based eligibility data. This migration will set the expiration
-- date of the file based Dentsu records to the date they became invalid and should have been replaced by a optum record

-- This is not ideal, but seems a safer bet than running a 'delete' operation against our DB.
-- In the future, if clients cut over like this, we will have file-based records deleted *before* we begin ingesting kafka data.


UPDATE eligibility.member
SET effective_range =  daterange(lower(effective_range),'2022-08-29')
WHERE organization_id = 170 and file_id is not null;

-- migrate:down

-- NO OP
