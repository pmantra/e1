-- migrate:up transaction:false
CREATE TABLE eligibility.tmp_optum_hash_ids (
	member_versioned_id int NOT NULL,
	organization_id int NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


CREATE TRIGGER set_tmp_optum_hash_ids_timestamp BEFORE UPDATE ON eligibility.tmp_optum_hash_ids FOR EACH ROW EXECUTE FUNCTION eligibility.trigger_set_timestamp();



-- migrate:down
DROP TABLE IF EXISTS eligibility.tmp_optum_hash_ids;

