-- migrate:up
ALTER TABLE eligibility.population ADD IF NOT EXISTS updated_at timestamptz DEFAULT CURRENT_TIMESTAMP;
DO
$$BEGIN
   CREATE TRIGGER set_population_update_timestamp BEFORE UPDATE ON eligibility.population FOR EACH ROW EXECUTE FUNCTION eligibility.trigger_set_timestamp();
EXCEPTION
   WHEN duplicate_object THEN
      NULL;
END;$$;


-- migrate:down
DROP TRIGGER IF EXISTS set_sub_population_update_timestamp ON eligibility.sub_population;
ALTER TABLE eligibility.sub_population DROP IF EXISTS updated_at;
