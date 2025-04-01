-- migrate:up
DROP TYPE IF EXISTS eligibility.id_to_text;
CREATE TYPE eligibility.id_to_text AS (
	id integer,
	text text
);

-- migrate:down
DROP TYPE eligibility.id_to_text;
