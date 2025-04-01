-- migrate:up
ALTER TABLE eligibility."configuration" ADD medical_plan_only boolean NULL DEFAULT False;
ALTER TABLE eligibility."configuration" ADD employee_only boolean NULL DEFAULT False;

-- migrate:down
ALTER TABLE eligibility."configuration" DROP COLUMN medical_plan_only;
ALTER TABLE eligibility."configuration" DROP COLUMN employee_only;

