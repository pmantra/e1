-- migrate:up
DROP TABLE IF EXISTS eligibility.tmp_optum_hash_ids;
DROP TABLE IF EXISTS eligibility.member_address_versioned_historical;
DROP TABLE IF EXISTS eligibility.member_versioned_historical;
DROP TABLE IF EXISTS eligibility.tmp_member_address;
DROP FUNCTION IF EXISTS eligibility.tmp_migrate_file_parse_results;
DROP TABLE IF EXISTS eligibility.tmp_member;
DROP TABLE IF EXISTS eligibility.tmp_file_parse_errors;
DROP TABLE IF EXISTS eligibility.tmp_file;


-- migrate:down

