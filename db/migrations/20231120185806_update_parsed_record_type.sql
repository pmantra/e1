-- migrate:up
ALTER TYPE eligibility.parsed_record add ATTRIBUTE hash_value eligibility.iwstext;
ALTER TYPE eligibility.parsed_record add ATTRIBUTE hash_version int;
ALTER TYPE eligibility.external_record add ATTRIBUTE hash_value eligibility.iwstext;
ALTER TYPE eligibility.external_record add ATTRIBUTE hash_version int;
-- migrate:down

