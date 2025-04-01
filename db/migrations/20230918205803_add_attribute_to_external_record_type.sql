-- migrate:up
DO
$$BEGIN
	ALTER TYPE eligibility.external_record ADD ATTRIBUTE work_country eligibility.iwstext, add attribute custom_attributes JSONB;
EXCEPTION
   WHEN duplicate_column THEN
      NULL;
END;$$;


-- migrate:down
ALTER TYPE eligibility.external_record DROP attribute if EXISTS work_country, DROP attribute if exists custom_attributes;
