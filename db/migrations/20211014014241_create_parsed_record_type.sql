-- migrate:up

CREATE TYPE eligibility.parsed_record AS (
    organization_id BIGINT,
    first_name TEXT collate eligibility.ci,
    last_name TEXT collate eligibility.ci,
    email TEXT collate eligibility.ci,
    unique_corp_id eligibility.ilztext collate eligibility.ci,
    dependent_id TEXT collate eligibility.ci,
    date_of_birth date,
    work_state TEXT collate eligibility.ci,
    record TEXT,
    file_id BIGINT,
    effective_range DATERANGE
);

-- migrate:down

DROP TYPE eligibility.parsed_record;
