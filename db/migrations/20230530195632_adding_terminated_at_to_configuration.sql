-- migrate:up
ALTER TABLE eligibility."configuration" ADD terminated_at date NULL;


-- migrate:down
ALTER TABLE eligibility."configuration" DROP COLUMN terminated_at;
