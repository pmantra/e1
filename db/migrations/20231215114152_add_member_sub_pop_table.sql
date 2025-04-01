-- migrate:up
CREATE TABLE IF NOT EXISTS eligibility.member_sub_population (
    member_id INT8 NOT NULL UNIQUE
        REFERENCES eligibility.member_versioned (id) ON DELETE CASCADE,
    sub_population_id INT8 NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
DO
$$BEGIN
   CREATE TRIGGER set_member_sub_population_update_timestamp
       BEFORE UPDATE ON eligibility.member_sub_population
       FOR EACH ROW EXECUTE FUNCTION eligibility.trigger_set_timestamp();
EXCEPTION
   WHEN duplicate_object THEN
      NULL;
END;$$;

-- migrate:down
DROP TRIGGER IF EXISTS set_member_sub_population_update_timestamp ON eligibility.member_sub_population;
DROP TABLE IF EXISTS eligibility.sub_population CASCADE;
