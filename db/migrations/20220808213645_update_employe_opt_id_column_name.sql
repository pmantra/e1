-- migrate:up
ALTER TABLE eligibility.member RENAME employerAssignedId TO employer_assigned_id;


-- migrate:down

ALTER TABLE eligibility.member RENAME employer_assigned_id TO employerAssignedId;
