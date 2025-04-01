-- migrate:up


-- Create a table to hold the member address information we recieve
CREATE TABLE eligibility.member_address
(
    id                 BIGSERIAL PRIMARY KEY NOT NULL,
    member_id           BIGINT NOT NULL,
    address_1          TEXT,
    address_2          TEXT,
    city               TEXT,
    state              TEXT,
    postal_code        TEXT,
    postal_code_suffix TEXT,
    country_code       TEXT,
    address_type       TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER set_address_timestamp
    BEFORE UPDATE ON eligibility.member_address
    FOR EACH ROW
    EXECUTE PROCEDURE eligibility.trigger_set_timestamp();

CREATE UNIQUE INDEX uidx_member_address_state_zip
    ON eligibility.member_address (
        member_id,
        address_1,
        city,
        state,
        postal_code,
        country_code

    );
CREATE INDEX idx_address_member_id
    ON eligibility.member_address(member_id);
CREATE INDEX idx_address_created_at
    ON eligibility.member_address(created_at);
CREATE INDEX idx_address_updated_at
    ON eligibility.member_address(updated_at);
CREATE INDEX idx_address_state
    ON eligibility.member_address(state);
CREATE INDEX idx_address_country
    ON eligibility.member_address(country_code);


-- ADD new fields to our member table related to their address

ALTER TABLE eligibility.member
    ADD do_not_contact eligibility.iwstext,
    ADD provided_gender_code eligibility.iwstext;


CREATE INDEX idx_member_id_do_not_contact
    ON eligibility.member(id, do_not_contact);
CREATE INDEX idx_member_do_not_contact
    ON eligibility.member(do_not_contact);


-- migrate:down

DROP TABLE eligibility.member_address;
ALTER TABLE eligibility.member DROP COLUMN do_not_contact;
ALTER TABLE eligibility.member DROP COLUMN provided_gender_code;
