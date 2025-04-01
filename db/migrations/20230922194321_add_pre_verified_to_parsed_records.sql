-- migrate:up
DO
$$BEGIN
	ALTER TYPE eligibility.parsed_record ADD ATTRIBUTE pre_verified bool;
EXCEPTION
   WHEN duplicate_column THEN
      NULL;
END;$$;


-- migrate:down
ALTER TYPE eligibility.parsed_record DROP attribute if EXISTS pre_verified;
