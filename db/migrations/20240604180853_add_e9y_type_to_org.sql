-- migrate:up
ALTER TABLE eligibility."configuration" ADD eligibility_type varchar NULL DEFAULT null;

-- migrate:down
ALTER TABLE eligibility."configuration" DROP eligibility_type;

