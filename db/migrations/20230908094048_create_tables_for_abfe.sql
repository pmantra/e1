-- migrate:up

CREATE TABLE IF NOT EXISTS eligibility.population (
    id BIGSERIAL NOT NULL,
    organization_id INT8 NOT NULL,
    activated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deactivated_at TIMESTAMPTZ NULL DEFAULT NULL,
    sub_pop_lookup_keys_csv TEXT NOT NULL,
    sub_pop_lookup_map_json JSONB NOT NULL,
    advanced BOOL NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT population_pkey PRIMARY KEY (id),
    CONSTRAINT organization_population_fkey FOREIGN KEY (organization_id)
        REFERENCES eligibility.configuration ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_population_organization ON eligibility.population(organization_id);
CREATE INDEX IF NOT EXISTS idx_gin_sub_pop_lookup_map ON eligibility.population USING gin(sub_pop_lookup_map_json);

CREATE TABLE IF NOT EXISTS eligibility.sub_population (
    id BIGSERIAL NOT NULL,
    population_id INT8 NOT NULL,
    feature_set_name TEXT NOT NULL,
    feature_set_details_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT sub_population_pkey PRIMARY KEY (id),
    CONSTRAINT sub_population_population_fkey FOREIGN KEY (population_id)
        REFERENCES eligibility.population(id) ON DELETE CASCADE,
    CONSTRAINT population_feature_set UNIQUE (population_id, feature_set_name)
);
CREATE INDEX IF NOT EXISTS idx_sub_population_population ON eligibility.sub_population(population_id);
CREATE INDEX IF NOT EXISTS idx_gin_feature_set_details_json ON eligibility.sub_population USING gin(feature_set_details_json);

ALTER TABLE eligibility.member_versioned ADD IF NOT EXISTS work_country eligibility.citext NULL DEFAULT NULL;
ALTER TABLE eligibility.member_versioned ADD IF NOT EXISTS custom_attributes JSONB NULL DEFAULT NULL;

-- migrate:down

ALTER TABLE eligibility.member_versioned DROP COLUMN IF EXISTS custom_attributes;
ALTER TABLE eligibility.member_versioned DROP COLUMN IF EXISTS work_country;

DROP TABLE IF EXISTS eligibility.sub_population CASCADE;

DROP TABLE IF EXISTS eligibility.population CASCADE;
