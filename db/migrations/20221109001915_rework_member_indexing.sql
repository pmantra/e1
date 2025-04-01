-- migrate:up
DROP INDEX IF EXISTS eligibility.idx_address_country;
DROP INDEX IF EXISTS eligibility.idx_address_state;
DROP INDEX IF EXISTS eligibility.idx_address_created_at;
DROP INDEX IF EXISTS eligibility.idx_address_update_at;

DROP INDEX IF EXISTS eligibility.idx_member_created_at;
DROP INDEX IF EXISTS eligibility.idx_member_updated_at;
DROP INDEX IF EXISTS eligibility.idx_member_do_not_contact;
DROP INDEX IF EXISTS eligibility.idx_member_name;
DROP INDEX IF EXISTS eligibility.idx_member_record;

CREATE INDEX IF NOT EXISTS idx_member_effective_range ON eligibility.member USING gist (effective_range);

-- migrate:down

CREATE INDEX IF NOT EXISTS idx_address_country ON eligibility.member_address (country_code);
CREATE INDEX IF NOT EXISTS idx_address_state ON eligibility.member_address (state);
CREATE INDEX IF NOT EXISTS idx_address_created_at ON eligibility.member_address (created_at);
CREATE INDEX IF NOT EXISTS idx_address_update_at ON eligibility.member_address (updated_at);

CREATE INDEX IF NOT EXISTS idx_member_created_at ON eligibility.member (created_at);
CREATE INDEX IF NOT EXISTS idx_member_update_at ON eligibility.member (updated_at);
CREATE INDEX IF NOT EXISTS idx_member_do_not_contact ON eligibility.member (do_not_contact);
CREATE INDEX IF NOT EXISTS idx_member_name ON eligibility.member (first_name, last_name);
CREATE INDEX IF NOT EXISTS idx_member_record ON eligibility.member USING gin (record);

DROP INDEX IF EXISTS idx_member_effective_range;
