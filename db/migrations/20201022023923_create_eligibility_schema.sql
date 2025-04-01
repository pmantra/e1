-- migrate:up
-- Set up global types and procedures.
CREATE SCHEMA eligibility;
GRANT ALL ON SCHEMA eligibility TO postgres;

SET search_path = eligibility;

-- A case-insensitive collation for text.
-- Taken from https://www.postgresql.org/docs/12/collation.html#COLLATION-NONDETERMINISTIC
CREATE COLLATION ci (provider = icu, locale = 'und-u-ks-level2', deterministic = false);

-- A procedure to set `ROW.updated_at` to the current timestamp
CREATE OR REPLACE FUNCTION trigger_set_timestamp()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = CURRENT_TIMESTAMP;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create custom "domains", which can be considered type extensions.
-- Taken from https://www.depesz.com/2008/10/15/text-comparisons-that-does-automatic-trim/

-- The `ilztext` trims any leading zeroes (0) from the front of the value
-- when running comparisons.
-- In an ideal world we'd just cast to an integer, but we can't guarantee the provided
-- values will always be digits.
CREATE DOMAIN ilztext AS TEXT COLLATE ci;

CREATE FUNCTION ilztext_req(TEXT, ilztext) RETURNS bool AS $$
    SELECT ltrim($1, '0') = ltrim($2, '0');
$$ LANGUAGE SQL immutable;

CREATE OPERATOR = (
    leftarg = text,
    rightarg = ilztext,
    negator = <>,
    PROCEDURE = ilztext_req
);

CREATE FUNCTION ilztext_rne(TEXT, ilztext) RETURNS bool AS $$
    SELECT ltrim($1, '0') <> ltrim($2, '0');
$$ LANGUAGE SQL immutable;

CREATE OPERATOR <> (
    leftarg = text,
    rightarg = ilztext,
    negator = =,
    PROCEDURE = ilztext_rne
);

CREATE FUNCTION ilztext_leq(ilztext, TEXT) RETURNS bool AS $$
    SELECT ltrim($1, '0') = ltrim($2, '0');
$$ LANGUAGE SQL immutable;

CREATE OPERATOR = (
    leftarg = ilztext,
    rightarg = text,
    negator = <>,
    PROCEDURE = ilztext_leq
);

CREATE FUNCTION ilztext_lne(ilztext, TEXT) RETURNS bool AS $$
    SELECT ltrim($1, '0') <> ltrim($2, '0');
$$ LANGUAGE SQL immutable;

CREATE OPERATOR <> (
    leftarg = ilztext,
    rightarg = text,
    negator = =,
    PROCEDURE = ilztext_lne
);

CREATE FUNCTION ilztext_beq(ilztext, ilztext) RETURNS bool AS $$
    SELECT ltrim($1, '0') = ltrim($2, '0');
$$ LANGUAGE SQL immutable;

CREATE OPERATOR = (
    leftarg = ilztext,
    rightarg = ilztext,
    negator = <>,
    PROCEDURE = ilztext_beq
);

CREATE FUNCTION ilztext_bne(ilztext, ilztext) RETURNS bool AS $$
    SELECT ltrim($1, '0') <> ltrim($2, '0');
$$ LANGUAGE SQL immutable;

CREATE OPERATOR <> (
    leftarg = ilztext,
    rightarg = ilztext,
    negator = =,
    PROCEDURE = ilztext_bne
);

-- Define our tables.

CREATE TABLE configuration (
    organization_id BIGINT PRIMARY KEY NOT NULL,
    primary_key TEXT NOT NULL,
    directory_name TEXT UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER set_configuration_timestamp
    BEFORE UPDATE ON configuration
    FOR EACH ROW
    EXECUTE PROCEDURE trigger_set_timestamp();


CREATE TABLE header_alias (
    organization_id BIGINT REFERENCES configuration(organization_id)
        ON DELETE CASCADE NOT NULL,
    header TEXT COLLATE ci NOT NULL,
    alias TEXT COLLATE ci NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT org_header_alias UNIQUE (organization_id, header, alias)
);

CREATE TRIGGER set_header_alias_timestamp
    BEFORE UPDATE ON header_alias
    FOR EACH ROW
    EXECUTE PROCEDURE trigger_set_timestamp();


CREATE TABLE file (
    id BIGSERIAL PRIMARY KEY NOT NULL,
    organization_id BIGINT REFERENCES configuration(organization_id)
        ON DELETE CASCADE NOT NULL,
    name TEXT NOT NULL,
    encoding TEXT NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_file_name on file(name);
CREATE INDEX idx_file_org_started_at on file(organization_id, started_at);
CREATE INDEX idx_file_org_completed_at on file(organization_id, completed_at);
CREATE INDEX idx_file_created_at on file(created_at);
CREATE INDEX idx_file_updated_at on file(updated_at);

CREATE TRIGGER set_file_timestamp
    BEFORE UPDATE ON file
    FOR EACH ROW
    EXECUTE PROCEDURE trigger_set_timestamp();

CREATE TABLE member (
    id BIGSERIAL PRIMARY KEY NOT NULL,
    organization_id BIGINT REFERENCES configuration(organization_id)
        ON DELETE CASCADE NOT NULL,
    file_id BIGINT REFERENCES file(id) ON DELETE NO ACTION,
    first_name TEXT COLLATE ci NOT NULL DEFAULT '',
    last_name TEXT COLLATE ci NOT NULL DEFAULT '',
    email TEXT COLLATE ci NOT NULL DEFAULT '',
    unique_corp_id ilztext NOT NULL DEFAULT '',
    dependent_id TEXT COLLATE ci NOT NULL DEFAULT '',
    date_of_birth DATE NOT NULL,
    work_state TEXT COLLATE ci,
    record JSONB,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TRIGGER set_member_timestamp
    BEFORE UPDATE ON member
    FOR EACH ROW
    EXECUTE PROCEDURE trigger_set_timestamp();

CREATE INDEX idx_member_name
    ON member(first_name, last_name);
CREATE INDEX idx_member_email
    ON member(email);
CREATE INDEX idx_primary_verification
    ON member(email, date_of_birth);
CREATE INDEX idx_secondary_verification
    ON member(first_name, last_name, work_state, date_of_birth);
CREATE UNIQUE INDEX uidx_member_org_identity
    ON member (
        organization_id,
        email,
        ltrim(unique_corp_id, '0'),
        dependent_id
    );
CREATE INDEX idx_member_record
    ON member USING GIN(record);
CREATE INDEX idx_member_created_at
    ON member(created_at);
CREATE INDEX idx_member_updated_at
    ON member(updated_at);

-- Create a custom type and comparators for our unique key.

CREATE TYPE org_identity as (
    organization_id BIGINT,
    email TEXT COLLATE ci,
    unique_corp_id ilztext,
    dependent_id TEXT COLLATE ci
);

CREATE FUNCTION get_identity(member) RETURNS org_identity AS $$
    SELECT (
        $1.organization_id, $1.email, $1.unique_corp_id, $1.dependent_id
    )::eligibility.org_identity;
$$ LANGUAGE sql immutable;

CREATE FUNCTION get_header_mapping(header_alias) RETURNS jsonb as $$
    SELECT (
        CASE WHEN $1.header IS NOT NULL
            THEN jsonb_build_object($1.header, $1.alias)
            ELSE '{}'::jsonb
        END
    )
$$ LANGUAGE sql immutable;

CREATE FUNCTION org_identity_req(member, org_identity) RETURNS bool AS $$
    SELECT (
        $1.organization_id = $2.organization_id
        AND $1.email = $2.email
        AND $1.unique_corp_id = $2.unique_corp_id
        AND $1.dependent_id = $2.dependent_id
    );
$$ LANGUAGE SQL immutable;

CREATE OPERATOR = (
    leftarg = member,
    rightarg = org_identity,
    negator = <>,
    PROCEDURE = org_identity_req
);

CREATE FUNCTION org_identity_rne(member, org_identity) RETURNS bool AS $$
    SELECT (
        $1.organization_id <> $2.organization_id
        OR $1.email <> $2.email
        OR $1.unique_corp_id <> $2.unique_corp_id
        OR $1.dependent_id <> $2.dependent_id
    );
$$ LANGUAGE SQL immutable;

CREATE OPERATOR <> (
    leftarg = member,
    rightarg = org_identity,
    negator = =,
    PROCEDURE = org_identity_rne
);

CREATE FUNCTION org_identity_leq(org_identity, member) RETURNS bool AS $$
    SELECT (
        $1.organization_id = $2.organization_id
        AND $1.email = $2.email
        AND $1.unique_corp_id = $2.unique_corp_id
        AND $1.dependent_id = $2.dependent_id
    );
$$ LANGUAGE SQL immutable;

CREATE OPERATOR = (
    leftarg = org_identity,
    rightarg = member,
    negator = <>,
    PROCEDURE = org_identity_leq
);

CREATE FUNCTION org_identity_lne(org_identity, member) RETURNS bool AS $$
    SELECT (
        $1.organization_id <> $2.organization_id
        OR $1.email <> $2.email
        OR $1.unique_corp_id <> $2.unique_corp_id
        OR $1.dependent_id <> $2.dependent_id
    );
$$ LANGUAGE SQL immutable;

CREATE OPERATOR <> (
    leftarg = org_identity,
    rightarg = member,
    negator = =,
    PROCEDURE = org_identity_lne
);

CREATE FUNCTION org_identity_beq(org_identity, org_identity) RETURNS bool AS $$
    SELECT (
        $1.organization_id = $2.organization_id
        AND $1.email = $2.email
        AND $1.unique_corp_id = $2.unique_corp_id
        AND $1.dependent_id = $2.dependent_id
    );
$$ LANGUAGE SQL immutable;

CREATE OPERATOR = (
    leftarg = org_identity,
    rightarg = org_identity,
    negator = <>,
    PROCEDURE = org_identity_beq
);

CREATE FUNCTION org_identity_bne(org_identity, org_identity) RETURNS bool AS $$
    SELECT (
        $1.organization_id <> $2.organization_id
        OR $1.email <> $2.email
        OR $1.unique_corp_id <> $2.unique_corp_id
        OR $1.dependent_id <> $2.dependent_id
    );
$$ LANGUAGE SQL immutable;

CREATE OPERATOR <> (
    leftarg = org_identity,
    rightarg = org_identity,
    negator = =,
    PROCEDURE = org_identity_bne
);

SET SEARCH_PATH = public;

-- migrate:down

DROP SCHEMA IF EXISTS eligibility CASCADE;
