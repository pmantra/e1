-- migrate:up

SET SEARCH_PATH = eligibility;

CREATE TYPE client_specific_implementation AS ENUM ('MICROSOFT');
ALTER TABLE configuration ADD COLUMN implementation client_specific_implementation DEFAULT NULL;

SET SEARCH_PATH = public;

-- migrate:down

SET SEARCH_PATH = eligibility;

ALTER TABLE configuration DROP COLUMN implementation;
DROP TYPE client_specific_implementation;

SET SEARCH_PATH = public;

