-- migrate:up
SET search_path = "eligibility";
DROP OPERATOR IF EXISTS eligibility.<>(text, eligibility.ilztext);
DROP OPERATOR IF EXISTS eligibility.=(text, eligibility.ilztext);
DROP OPERATOR IF EXISTS eligibility.<>(eligibility.ilztext, text);
DROP OPERATOR IF EXISTS eligibility.=(eligibility.ilztext, text);
DROP OPERATOR IF EXISTS eligibility.<>(eligibility.ilztext, eligibility.ilztext);
DROP OPERATOR IF EXISTS eligibility.=(eligibility.ilztext, eligibility.ilztext);
DROP OPERATOR IF EXISTS eligibility.<>(eligibility.member, eligibility.org_identity);
DROP OPERATOR IF EXISTS eligibility.=(eligibility.member, eligibility.org_identity);
DROP OPERATOR IF EXISTS eligibility.<>(eligibility.org_identity, eligibility.member);
DROP OPERATOR IF EXISTS eligibility.=(eligibility.org_identity, eligibility.member);
DROP OPERATOR IF EXISTS eligibility.<>(eligibility.org_identity, eligibility.org_identity);
DROP OPERATOR IF EXISTS eligibility.=(eligibility.org_identity, eligibility.org_identity);
DROP FUNCTION IF EXISTS eligibility.org_identity_leq(org_identity, member);
DROP FUNCTION IF EXISTS eligibility.org_identity_beq(org_identity, org_identity);
DROP FUNCTION IF EXISTS eligibility.org_identity_req(member, org_identity);
DROP FUNCTION IF EXISTS eligibility.org_identity_bne(org_identity, org_identity);
DROP FUNCTION IF EXISTS eligibility.org_identity_lne(org_identity, member);
DROP FUNCTION IF EXISTS eligibility.org_identity_rne(member, org_identity);

CREATE OPERATOR = (
    leftarg = text,
    rightarg = ilztext,
    negator = <>,
    commutator = =,
    RESTRICT = eqsel,
    JOIN = eqjoinsel,
    MERGES,
    PROCEDURE = ilztext_req
);
CREATE OPERATOR <> (
    leftarg = text,
    rightarg = ilztext,
    negator = =,
    commutator = <>,
    RESTRICT = neqsel,
    JOIN = neqjoinsel,
    MERGES,
    PROCEDURE = ilztext_rne
);
CREATE OPERATOR = (
    leftarg = ilztext,
    rightarg = text,
    negator = <>,
    commutator = =,
    RESTRICT = eqsel,
    JOIN = eqjoinsel,
    MERGES,
    PROCEDURE = ilztext_leq
);
CREATE OPERATOR <> (
    leftarg = ilztext,
    rightarg = text,
    negator = =,
    commutator = <>,
    RESTRICT = neqsel,
    JOIN = neqjoinsel,
    MERGES,
    PROCEDURE = ilztext_lne
);
CREATE OPERATOR = (
    leftarg = ilztext,
    rightarg = ilztext,
    negator = <>,
    commutator = =,
    RESTRICT = eqsel,
    JOIN = eqjoinsel,
    MERGES,
    PROCEDURE = ilztext_beq
);
CREATE OPERATOR <> (
    leftarg = ilztext,
    rightarg = ilztext,
    negator = =,
    commutator = <>,
    RESTRICT = neqsel,
    JOIN = neqjoinsel,
    MERGES,
    PROCEDURE = ilztext_bne
);

-- Flesh out any missing equality comparisons
CREATE OR REPLACE FUNCTION ilztext_blt(ilztext, ilztext) RETURNS bool AS $$
    SELECT ltrim($1, '0') < ltrim($2, '0');
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION ilztext_ble(ilztext, ilztext) RETURNS bool AS $$
    SELECT ltrim($1, '0') <= ltrim($2, '0');
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION ilztext_bgt(ilztext, ilztext) RETURNS bool AS $$
    SELECT ltrim($1, '0') > ltrim($2, '0');
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION ilztext_bge(ilztext, ilztext) RETURNS bool AS $$
    SELECT ltrim($1, '0') >= ltrim($2, '0');
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION ilztext_rlt(text, ilztext) RETURNS bool AS $$
    SELECT ltrim($1, '0') < ltrim($2, '0');
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION ilztext_rle(text, ilztext) RETURNS bool AS $$
    SELECT ltrim($1, '0') <= ltrim($2, '0');
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION ilztext_rgt(text, ilztext) RETURNS bool AS $$
    SELECT ltrim($1, '0') > ltrim($2, '0');
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION ilztext_rge(text, ilztext) RETURNS bool AS $$
    SELECT ltrim($1, '0') >= ltrim($2, '0');
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION ilztext_llt(ilztext, text) RETURNS bool AS $$
    SELECT ltrim($1, '0') < ltrim($2, '0');
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION ilztext_lle(ilztext, text) RETURNS bool AS $$
    SELECT ltrim($1, '0') <= ltrim($2, '0');
$$ LANGUAGE sql IMMUTABLE;


CREATE OR REPLACE FUNCTION ilztext_lgt(ilztext, text) RETURNS bool AS $$
    SELECT ltrim($1, '0') > ltrim($2, '0');
$$ LANGUAGE sql IMMUTABLE;


CREATE OR REPLACE FUNCTION ilztext_lge(ilztext, text) RETURNS bool AS $$
    SELECT ltrim($1, '0') >= ltrim($2, '0');
$$ LANGUAGE sql IMMUTABLE;

CREATE OPERATOR < (
    leftarg = ilztext,
    rightarg = ilztext,
    negator = >,
    commutator = >,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel,
    MERGES,
    PROCEDURE = ilztext_blt
);

CREATE OPERATOR <= (
    leftarg = ilztext,
    rightarg = ilztext,
    negator = >=,
    commutator = >=,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel,
    MERGES,
    PROCEDURE = ilztext_ble
);

CREATE OPERATOR >= (
    leftarg = ilztext,
    rightarg = ilztext,
    negator = <=,
    commutator = <=,
    RESTRICT = scalargesel,
    JOIN = scalargejoinsel,
    MERGES,
    PROCEDURE = ilztext_bge
);

CREATE OPERATOR > (
    leftarg = ilztext,
    rightarg = ilztext,
    negator = <,
    commutator = <,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel,
    MERGES,
    PROCEDURE = ilztext_bgt
);

CREATE OPERATOR < (
    leftarg = text,
    rightarg = ilztext,
    negator = >,
    commutator = >,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel,
    MERGES,
    PROCEDURE = ilztext_rlt
);

CREATE OPERATOR <= (
    leftarg = text,
    rightarg = ilztext,
    negator = >=,
    commutator = >=,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel,
    MERGES,
    PROCEDURE = ilztext_rle
);

CREATE OPERATOR > (
    leftarg = text,
    rightarg = ilztext,
    negator = <,
    commutator = <,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel,
    MERGES,
    PROCEDURE = ilztext_rgt
);

CREATE OPERATOR >= (
    leftarg = text,
    rightarg = ilztext,
    negator = <=,
    commutator = <=,
    RESTRICT = scalargesel,
    JOIN = scalargejoinsel,
    MERGES,
    PROCEDURE = ilztext_rge
);

CREATE OPERATOR < (
    leftarg = ilztext,
    rightarg = text,
    negator = >,
    commutator = >,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel,
    MERGES,
    PROCEDURE = ilztext_llt
);

CREATE OPERATOR <= (
    leftarg = ilztext,
    rightarg = text,
    negator = >=,
    commutator = >=,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel,
    MERGES,
    PROCEDURE = ilztext_lle
);

CREATE OPERATOR > (
    leftarg = ilztext,
    rightarg = text,
    negator = >,
    commutator = >,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel,
    MERGES,
    PROCEDURE = ilztext_lgt
);

CREATE OPERATOR >= (
    leftarg = ilztext,
    rightarg = text,
    negator = <=,
    commutator = <=,
    RESTRICT = scalargesel,
    JOIN = scalargejoinsel,
    MERGES,
    PROCEDURE = ilztext_lge
);

-- This is a cmp function for use in a custom btree operator class.
CREATE OR REPLACE FUNCTION ilztext_same(text, ilztext) RETURNS int AS
$$
    SELECT CASE
        WHEN $1 < $2 THEN -1
        WHEN $1 > $2 THEN +1
        ELSE 0
    END;
$$ LANGUAGE sql IMMUTABLE ;

CREATE OR REPLACE FUNCTION ilztext_same(ilztext, text) RETURNS int AS
$$
    SELECT CASE
        WHEN $1 < $2 THEN -1
        WHEN $1 > $2 THEN +1
        ELSE 0
    END;
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION ilztext_same(ilztext, ilztext) RETURNS int AS
$$
    SELECT CASE
        WHEN $1 < $2 THEN -1
        WHEN $1 > $2 THEN +1
        ELSE 0
    END;
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION get_identity(member) RETURNS org_identity AS $$
    SELECT (
        $1.organization_id, $1.email, ltrim($1.unique_corp_id, '0'), $1.dependent_id
    )::eligibility.org_identity;
$$ LANGUAGE sql immutable;

DELETE FROM eligibility.member WHERE id = ANY (
    SELECT unnest(array_remove(array_agg(id), max(id))) as id
    FROM eligibility.member
    GROUP BY (organization_id, ltrim(unique_corp_id, '0'), btrim(dependent_id), email)
);
UPDATE eligibility.member SET dependent_id = ''
WHERE dependent_id = ' ';

-- Use a generated column for the member identity.
-- This saves operational overhead, since we now don't need to cast the member object.
CREATE INDEX IF NOT EXISTS idx_unique_corp_id
    ON eligibility.member (ltrim(unique_corp_id, '0'));
CREATE INDEX IF NOT EXISTS idx_member_organization_id
    ON eligibility.member (organization_id);
CREATE INDEX IF NOT EXISTS idx_member_file_id
    ON eligibility.member (file_id);

-- The query planner is bad at optimizing functions,
-- so we're going to expose the underlying query directly.
DROP FUNCTION IF EXISTS
    eligibility.get_member_difference_by_org_identities(bigint, eligibility.org_identity[]);

SET search_path = "public";

-- migrate:down
SET search_path = "eligibility";

DROP INDEX IF EXISTS eligibility.idx_unique_corp_id;
DROP INDEX IF EXISTS eligibility.idx_member_organization_id;
DROP INDEX IF EXISTS eligibility.idx_member_file_id;

DROP FUNCTION IF EXISTS eligibility.ilztext_same(eligibility.ilztext, eligibility.ilztext);
DROP FUNCTION IF EXISTS eligibility.ilztext_same(text, eligibility.ilztext);
DROP FUNCTION IF EXISTS eligibility.ilztext_same(eligibility.ilztext, text);

DROP OPERATOR IF EXISTS =(ilztext, text);
DROP OPERATOR IF EXISTS <>(text, ilztext);
DROP OPERATOR IF EXISTS =(text, ilztext);
DROP OPERATOR IF EXISTS <>(ilztext, text);
DROP OPERATOR IF EXISTS <>(ilztext, ilztext);
DROP OPERATOR IF EXISTS =(ilztext, ilztext);
DROP OPERATOR IF EXISTS >(ilztext, ilztext);
DROP OPERATOR IF EXISTS <(ilztext, ilztext);
DROP OPERATOR IF EXISTS >=(ilztext, ilztext);
DROP OPERATOR IF EXISTS <=(ilztext, ilztext);
DROP OPERATOR IF EXISTS >(ilztext, text);
DROP OPERATOR IF EXISTS >(text, ilztext);
DROP OPERATOR IF EXISTS <(text, ilztext);
DROP OPERATOR IF EXISTS >=(ilztext, text);
DROP OPERATOR IF EXISTS >=(text, ilztext);
DROP OPERATOR IF EXISTS <=(text, ilztext);
DROP OPERATOR IF EXISTS <(ilztext, text);
DROP OPERATOR IF EXISTS <=(ilztext, text);

DROP FUNCTION IF EXISTS ilztext_blt(ilztext, ilztext);
DROP FUNCTION IF EXISTS ilztext_ble(ilztext, ilztext);
DROP FUNCTION IF EXISTS ilztext_bgt(ilztext, ilztext);
DROP FUNCTION IF EXISTS ilztext_bge(ilztext, ilztext);
DROP FUNCTION IF EXISTS ilztext_rlt(text, ilztext);
DROP FUNCTION IF EXISTS ilztext_rle(text, ilztext);
DROP FUNCTION IF EXISTS ilztext_rgt(text, ilztext);
DROP FUNCTION IF EXISTS ilztext_rge(text, ilztext);
DROP FUNCTION IF EXISTS ilztext_llt(ilztext, text);
DROP FUNCTION IF EXISTS ilztext_lle(ilztext, text);
DROP FUNCTION IF EXISTS ilztext_lgt(ilztext, text);
DROP FUNCTION IF EXISTS ilztext_lge(ilztext, text);

CREATE OPERATOR = (
    leftarg = text,
    rightarg = ilztext,
    negator = <>,
    PROCEDURE = ilztext_req
);
CREATE OPERATOR <> (
    leftarg = text,
    rightarg = ilztext,
    negator = =,
    PROCEDURE = ilztext_rne
);
CREATE OPERATOR = (
    leftarg = ilztext,
    rightarg = text,
    negator = <>,
    PROCEDURE = ilztext_leq
);
CREATE OPERATOR <> (
    leftarg = ilztext,
    rightarg = text,
    negator = =,
    PROCEDURE = ilztext_lne
);
CREATE OPERATOR = (
    leftarg = ilztext,
    rightarg = ilztext,
    negator = <>,
    PROCEDURE = ilztext_beq
);
CREATE OPERATOR <> (
    leftarg = ilztext,
    rightarg = ilztext,
    negator = =,
    PROCEDURE = ilztext_bne
);
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

SET search_path = "public";
