-- migrate:up
DROP SEQUENCE IF EXISTS eligibility.verification_user_id_seq CASCADE;

CREATE INDEX idx_verification_user_id ON eligibility.verification USING btree (user_id);

-- migrate:down
CREATE SEQUENCE eligibility.verification_user_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE eligibility.verification_user_id_seq OWNED BY eligibility.verification.user_id;

ALTER TABLE ONLY eligibility.verification ALTER COLUMN user_id SET DEFAULT nextval('eligibility.verification_user_id_seq'::regclass);

DROP INDEX IF EXISTS eligibility.idx_verification_user_id;