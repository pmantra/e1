-- migrate:up
CREATE TABLE IF NOT EXISTS eligibility.member_custom_attributes (
    id BIGSERIAL NOT NULL,
    member_id INT8 NOT NULL,
    attribute_name eligibility.citext NOT NULL DEFAULT '',
    attribute_value eligibility.citext NULL DEFAULT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT member_custom_attributes_pkey PRIMARY KEY (id),
    CONSTRAINT member_attribute_uidx UNIQUE (member_id, attribute_name),
    CONSTRAINT custom_attribute_member_id_fkey FOREIGN KEY (member_id)
        REFERENCES eligibility.member_versioned(id) ON DELETE CASCADE
);
DO
$$BEGIN
   CREATE TRIGGER set_member_custom_attributes_update_timestamp BEFORE UPDATE ON eligibility.member_custom_attributes FOR EACH ROW EXECUTE FUNCTION eligibility.trigger_set_timestamp();
EXCEPTION
   WHEN duplicate_object THEN
      NULL;
END;$$;


-- migrate:down
DROP TRIGGER IF EXISTS set_member_custom_attributes_update_timestamp ON eligibility.member_custom_attributes;
DROP TABLE IF EXISTS eligibility.member_custom_attributes CASCADE;
