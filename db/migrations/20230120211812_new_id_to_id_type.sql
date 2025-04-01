-- migrate:up

CREATE TYPE eligibility.id_to_id AS (
	source_id integer,
	target_id integer
);

-- migrate:down

DROP TYPE IF EXISTS eligibility.id_to_id;