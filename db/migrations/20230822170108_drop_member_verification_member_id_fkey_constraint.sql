-- migrate:up
ALTER TABLE eligibility.member_verification DROP CONSTRAINT IF EXISTS member_verification_member_id_fkey;

-- migrate:down
ALTER TABLE ONLY eligibility.member_verification
    ADD CONSTRAINT member_verification_member_id_fkey FOREIGN KEY (member_id) REFERENCES eligibility.member_versioned(id) ON DELETE CASCADE;

