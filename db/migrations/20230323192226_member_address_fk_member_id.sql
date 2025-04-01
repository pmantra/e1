-- migrate:up

ALTER TABLE ONLY eligibility.member_address
    ADD CONSTRAINT fk_member_address_member FOREIGN KEY (member_id) REFERENCES eligibility.member(id)
ON DELETE CASCADE;

-- migrate:down

ALTER TABLE ONLY eligibility.member_address
    DROP CONSTRAINT fk_member_address_member;