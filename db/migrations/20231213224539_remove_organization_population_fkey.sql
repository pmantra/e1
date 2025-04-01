-- migrate:up transaction:false
ALTER TABLE eligibility.population DROP CONSTRAINT IF EXISTS organization_population_fkey;

-- migrate:down transaction:false
ALTER TABLE eligibility.population ADD CONSTRAINT organization_population_fkey FOREIGN KEY (organization_id) REFERENCES eligibility.configuration(organization_id) ON DELETE CASCADE;
