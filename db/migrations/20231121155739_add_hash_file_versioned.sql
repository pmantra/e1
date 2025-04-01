-- migrate:up
ALTER TABLE eligibility.file_parse_results ADD IF NOT EXISTS hash_value eligibility."iwstext" NULL ;
ALTER TABLE eligibility.file_parse_results ADD IF NOT EXISTS hash_version int NULL;



-- migrate:down
ALTER TABLE eligibility.file_parse_results DROP COLUMN IF EXISTS hash_value;
ALTER TABLE eligibility.file_parse_results DROP COLUMN IF EXISTS hash_version;