-- migrate:up transaction:false
DO
$$
BEGIN
    ALTER TABLE eligibility.member_verification
        ADD CONSTRAINT member_verification_id_pkey PRIMARY KEY (id);
EXCEPTION
    WHEN duplicate_object THEN
        NULL;
    WHEN invalid_table_definition THEN
        NULL;
END;
$$;

-- migrate:down transaction:false
ALTER TABLE eligibility.member_verification
    DROP CONSTRAINT IF EXISTS member_verification_id_pkey;
