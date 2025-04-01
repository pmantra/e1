-- migrate:up
CREATE TYPE eligibility.external_record AS (
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    unique_corp_id eligibility.ilztext,
    dependent_id TEXT collate eligibility.ci,
    date_of_birth DATE,
    work_state TEXT,
    record TEXT,
    effective_range DATERANGE,
    source TEXT,
    external_id TEXT,
    external_name TEXT,
    received_ts BIGINT
);

-- migrate:down

DROP TYPE eligibility.external_record;
