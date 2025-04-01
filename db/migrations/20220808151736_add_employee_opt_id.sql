-- migrate:up
ALTER TABLE eligibility.member ADD employerAssignedId eligibility.iwstext NULL;


-- migrate:down

ALTER TABLE eligibility.member DROP COLUMN employerAssignedId;
