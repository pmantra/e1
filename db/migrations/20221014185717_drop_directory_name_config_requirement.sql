-- migrate:up
ALTER TABLE eligibility.configuration ALTER directory_name drop not null;
ALTER TABLE eligibility.configuration DROP CONSTRAINT configuration_directory_name_key;

-- migrate:down
ALTER TABLE eligibility.configuration ALTER directory_name set not null;
ALTER TABLE eligibility."configuration" ADD CONSTRAINT configuration_directory_name_key UNIQUE (directory_name);

