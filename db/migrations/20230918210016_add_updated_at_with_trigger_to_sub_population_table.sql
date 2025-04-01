-- migrate:up
ALTER TABLE eligibility.sub_population ADD IF NOT EXISTS updated_at timestamptz DEFAULT CURRENT_TIMESTAMP;
DO
$$BEGIN
   CREATE TRIGGER set_sub_population_update_timestamp BEFORE UPDATE ON eligibility.sub_population FOR EACH ROW EXECUTE FUNCTION eligibility.trigger_set_timestamp();
EXCEPTION
   WHEN duplicate_object THEN
      NULL;
END;$$;


-- migrate:down
DROP TRIGGER IF EXISTS set_population_update_timestamp ON eligibility.population;
ALTER TABLE eligibility.population DROP IF EXISTS updated_at;
