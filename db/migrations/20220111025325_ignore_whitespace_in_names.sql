-- migrate:up

/* The `eligibility.iwstext` ignores whitespace padding on text values during comparison.

   First, we create the 'domain', which is essentially a sub-type.
   Then, we tell Postgres how to compare it to itself and other text types.

   For every common logical operator (=, <>, <, >, <=, >=), there are three variations:
        1. "right-handed": right side is iwstext, left side is text.
        2. "left-handed": left side is iwstext, right side is text.
        3. "both-handed": both sides are iwstext.
 */

SET search_path = "eligibility";

-- Declare the type as a child of text with a case-insensitive collation.
CREATE DOMAIN iwstext text;

-- Create our direct equality functions, making them compatible with plain text as well.
CREATE OR REPLACE FUNCTION iwstext_req("left" text, "right" iwstext) RETURNS bool AS $$
    SELECT trim(lower("left")) = trim(lower("right"));
$$ LANGUAGE SQL immutable;

CREATE OR REPLACE FUNCTION iwstext_leq("left" iwstext, "right" text) RETURNS bool AS $$
    SELECT trim(lower("left")) = trim(lower("right"));
$$ LANGUAGE SQL immutable;

CREATE OR REPLACE FUNCTION iwstext_beq("left" iwstext, "right" iwstext) RETURNS bool AS $$
    SELECT trim(lower("left")) = trim(lower("right"));
$$ LANGUAGE SQL immutable;

CREATE OR REPLACE FUNCTION iwstext_rne("left" text, "right" iwstext) RETURNS bool AS $$
    SELECT trim(lower("left")) <> trim(lower("right"));
$$ LANGUAGE SQL immutable;

CREATE OR REPLACE FUNCTION iwstext_lne("left" iwstext, "right" text) RETURNS bool AS $$
    SELECT trim(lower("left")) <> trim(lower("right"));
$$ LANGUAGE SQL immutable;

CREATE OR REPLACE FUNCTION iwstext_bne("left" iwstext, "right" iwstext) RETURNS bool AS $$
    SELECT trim(lower("left")) <> trim(lower("right"));
$$ LANGUAGE SQL immutable;

-- These functions allow iwstext to operate correctly on a B-tree index.
CREATE OR REPLACE FUNCTION iwstext_rlt("left" text, "right" iwstext) RETURNS bool AS $$
    SELECT trim(lower("left")) < trim(lower("right"));
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION iwstext_llt("left" iwstext, "right" text) RETURNS bool AS $$
    SELECT trim(lower("left")) < trim(lower("right"));
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION iwstext_blt("left" iwstext, "right" iwstext) RETURNS bool AS $$
    SELECT trim(lower("left")) < trim(lower("right"));
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION iwstext_rgt("left" text, "right" iwstext) RETURNS bool AS $$
    SELECT trim(lower("left")) > trim(lower("right"));
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION iwstext_lgt("left" iwstext, "right" text) RETURNS bool AS $$
    SELECT trim(lower("left")) > trim(lower("right"));
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION iwstext_bgt("left" iwstext, "right" iwstext) RETURNS bool AS $$
    SELECT trim(lower("left")) > trim(lower("right"));
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION iwstext_rle("left" text, "right" iwstext) RETURNS bool AS $$
    SELECT trim(lower("left")) <= trim(lower("right"));
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION iwstext_lle("left" iwstext, "right" text) RETURNS bool AS $$
    SELECT trim(lower("left")) <= trim(lower("right"));
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION iwstext_ble("left" iwstext, "right" iwstext) RETURNS bool AS $$
    SELECT trim(lower("left")) <= trim(lower("right"));
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION iwstext_rge("left" text, "right" iwstext) RETURNS bool AS $$
    SELECT trim(lower("left")) >= trim(lower("right"));
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION iwstext_lge("left" iwstext, "right" text) RETURNS bool AS $$
    SELECT trim(lower("left")) >= trim(lower("right"));
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION iwstext_bge("left" iwstext, "right" iwstext) RETURNS bool AS $$
    SELECT trim(lower("left")) >= trim(lower("right"));
$$ LANGUAGE sql IMMUTABLE;


/* Custom operators which implement direct comparisons using the functions defined above.

   This allows us to use logical comparisons in sql with native syntax, e.g.:

        SELECT "FOO "::iwstext = "foo"::iwstext;

   Only caveat is that `eligibility` must be a part of the search path
   (which we manage on connect).

   In its full complement, it also ensures we can compare correctly using joins and indexes.
*/
CREATE OPERATOR = (
    leftarg = text,
    rightarg = iwstext,
    negator = <>,
    commutator = =,
    RESTRICT = eqsel,
    JOIN = eqjoinsel,
    MERGES,
    PROCEDURE = iwstext_req
);
CREATE OPERATOR <> (
    leftarg = text,
    rightarg = iwstext,
    negator = =,
    commutator = <>,
    RESTRICT = neqsel,
    JOIN = neqjoinsel,
    MERGES,
    PROCEDURE = iwstext_rne
);
CREATE OPERATOR = (
    leftarg = iwstext,
    rightarg = text,
    negator = <>,
    commutator = =,
    RESTRICT = eqsel,
    JOIN = eqjoinsel,
    MERGES,
    PROCEDURE = iwstext_leq
);
CREATE OPERATOR <> (
    leftarg = iwstext,
    rightarg = text,
    negator = =,
    commutator = <>,
    RESTRICT = neqsel,
    JOIN = neqjoinsel,
    MERGES,
    PROCEDURE = iwstext_lne
);
CREATE OPERATOR = (
    leftarg = iwstext,
    rightarg = iwstext,
    negator = <>,
    commutator = =,
    RESTRICT = eqsel,
    JOIN = eqjoinsel,
    MERGES,
    PROCEDURE = iwstext_beq
);
CREATE OPERATOR <> (
    leftarg = iwstext,
    rightarg = iwstext,
    negator = =,
    commutator = <>,
    RESTRICT = neqsel,
    JOIN = neqjoinsel,
    MERGES,
    PROCEDURE = iwstext_bne
);

CREATE OPERATOR < (
    leftarg = iwstext,
    rightarg = iwstext,
    negator = >,
    commutator = <,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel,
    MERGES,
    PROCEDURE = iwstext_blt
);

CREATE OPERATOR <= (
    leftarg = iwstext,
    rightarg = iwstext,
    negator = >=,
    commutator = <=,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel,
    MERGES,
    PROCEDURE = iwstext_ble
);

CREATE OPERATOR >= (
    leftarg = iwstext,
    rightarg = iwstext,
    negator = <=,
    commutator = >=,
    RESTRICT = scalargesel,
    JOIN = scalargejoinsel,
    MERGES,
    PROCEDURE = iwstext_bge
);

CREATE OPERATOR > (
    leftarg = iwstext,
    rightarg = iwstext,
    negator = <,
    commutator = >,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel,
    MERGES,
    PROCEDURE = iwstext_bgt
);

CREATE OPERATOR < (
    leftarg = text,
    rightarg = iwstext,
    negator = >,
    commutator = <,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel,
    MERGES,
    PROCEDURE = iwstext_rlt
);

CREATE OPERATOR <= (
    leftarg = text,
    rightarg = iwstext,
    negator = >=,
    commutator = <=,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel,
    MERGES,
    PROCEDURE = iwstext_rle
);

CREATE OPERATOR > (
    leftarg = text,
    rightarg = iwstext,
    negator = <,
    commutator = >,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel,
    MERGES,
    PROCEDURE = iwstext_rgt
);

CREATE OPERATOR >= (
    leftarg = text,
    rightarg = iwstext,
    negator = <=,
    commutator = <=,
    RESTRICT = scalargesel,
    JOIN = scalargejoinsel,
    MERGES,
    PROCEDURE = iwstext_rge
);

CREATE OPERATOR < (
    leftarg = iwstext,
    rightarg = text,
    negator = >,
    commutator = <,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel,
    MERGES,
    PROCEDURE = iwstext_llt
);

CREATE OPERATOR <= (
    leftarg = iwstext,
    rightarg = text,
    negator = >=,
    commutator = <=,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel,
    MERGES,
    PROCEDURE = iwstext_lle
);

CREATE OPERATOR > (
    leftarg = iwstext,
    rightarg = text,
    negator = >,
    commutator = <,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel,
    MERGES,
    PROCEDURE = iwstext_lgt
);

CREATE OPERATOR >= (
    leftarg = iwstext,
    rightarg = text,
    negator = <=,
    commutator = >=,
    RESTRICT = scalargesel,
    JOIN = scalargejoinsel,
    MERGES,
    PROCEDURE = iwstext_lge
);

-- This is a cmp function for use in a custom btree operator class.
CREATE OR REPLACE FUNCTION iwstext_same("left" text, "right" iwstext) RETURNS int AS
$$
    SELECT CASE
        WHEN "left" < "right" THEN -1
        WHEN "left" > "right" THEN +1
        ELSE 0
    END;
$$ LANGUAGE sql IMMUTABLE ;

CREATE OR REPLACE FUNCTION iwstext_same("left" iwstext, "right" text) RETURNS int AS
$$
    SELECT CASE
        WHEN "left" < "right" THEN -1
        WHEN "left" > "right" THEN +1
        ELSE 0
    END;
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION iwstext_same("left" iwstext, "right" iwstext) RETURNS int AS
$$
    SELECT CASE
        WHEN "left" < "right" THEN -1
        WHEN "left" > "right" THEN +1
        ELSE 0
    END;
$$ LANGUAGE sql IMMUTABLE;

-- Make first_name and last_name ignore all whitespace on the edges.
ALTER TABLE member
    ALTER COLUMN first_name TYPE iwstext,
    ALTER COLUMN last_name TYPE iwstext,
    ALTER COLUMN work_state TYPE iwstext,
    ALTER COLUMN email TYPE iwstext;

DROP INDEX IF EXISTS idx_secondary_verification;
CREATE INDEX IF NOT EXISTS idx_secondary_verification
    ON member (
        date_of_birth,
        trim(lower(first_name)),
        trim(lower(last_name)),
        trim(lower(work_state))

        text_pattern_ops
    );

DROP INDEX IF EXISTS idx_member_email;
CREATE INDEX IF NOT EXISTS idx_member_email
    ON member(trim(lower(email)) text_pattern_ops);

DROP INDEX IF EXISTS idx_primary_verification;
CREATE INDEX IF NOT EXISTS idx_primary_verification
    ON member(
        date_of_birth,
        trim(lower(email))

        text_pattern_ops
    );

SET search_path = "public";

-- migrate:down

DROP INDEX IF EXISTS eligibility.idx_secondary_verification;
DROP INDEX IF EXISTS eligibility.idx_primary_verification;
DROP INDEX IF EXISTS eligibility.idx_member_email;

ALTER TABLE eligibility.member
    ALTER COLUMN first_name TYPE text COLLATE eligibility.ci,
    ALTER COLUMN last_name TYPE text COLLATE eligibility.ci,
    ALTER COLUMN work_state TYPE text COLLATE eligibility.ci,
    ALTER COLUMN email TYPE text COLLATE eligibility.ci;

DROP DOMAIN eligibility.iwstext CASCADE;

CREATE INDEX IF NOT EXISTS idx_secondary_verification
    ON eligibility.member (first_name, last_name, date_of_birth, work_state);
CREATE INDEX IF NOT EXISTS idx_primary_verification
    ON eligibility.member (date_of_birth, email);
CREATE INDEX IF NOT EXISTS idx_member_email
    ON eligibility.member (email);
