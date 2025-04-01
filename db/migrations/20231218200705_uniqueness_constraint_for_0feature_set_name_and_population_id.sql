-- migrate:up transaction:false
ALTER TABLE eligibility.sub_population
ADD CONSTRAINT unique_feature_set_name_within_population_key
UNIQUE (population_id, feature_set_name);

-- migrate:down
ALTER TABLE eligibility.sub_population DROP CONSTRAINT IF EXISTS unique_feature_set_name_within_population_key;
