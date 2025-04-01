-- migrate:up
ALTER TABLE eligibility.member_sub_population
ADD CONSTRAINT member_sub_population_pk PRIMARY KEY (member_id, sub_population_id);


-- migrate:down
ALTER TABLE eligibility.member_sub_population
DROP CONSTRAINT member_sub_population_pk;
