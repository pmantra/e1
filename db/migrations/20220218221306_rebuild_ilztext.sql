-- migrate:up

/* The `eligibility.ilztext` ignores left-padded zeroes text values during comparison.

   First, we create the 'domain', which is essentially a sub-type.
   Then, we tell Postgres how to compare it to itself and other text types.

   For every common logical operator (=, <>, <, >, <=, >=), there are three variations:
        1. "right-handed": right side is ilztext, left side is text.
        2. "left-handed": left side is ilztext, right side is text.
        3. "both-handed": both sides are ilztext.
 */

-- setting the search path because this is just too verbose
SET search_path = "eligibility";

-- Rename the current domain so that we can start to replace it.
ALTER DOMAIN ilztext RENAME TO ilztextci;

CREATE DOMAIN ilztext TEXT;

-- Create our direct equality functions, making them compatible with plain text as well.
CREATE OR REPLACE FUNCTION ilztext_req("left" text, "right" ilztext) RETURNS bool AS $$
    SELECT ltrim(lower("left"), '0') = ltrim(lower("right"), '0');
$$ LANGUAGE SQL immutable;

CREATE OR REPLACE FUNCTION ilztext_leq("left" ilztext, "right" text) RETURNS bool AS $$
    SELECT ltrim(lower("left"), '0') = ltrim(lower("right"), '0');
$$ LANGUAGE SQL immutable;

CREATE OR REPLACE FUNCTION ilztext_beq("left" ilztext, "right" ilztext) RETURNS bool AS $$
    SELECT ltrim(lower("left"), '0') = ltrim(lower("right"), '0');
$$ LANGUAGE SQL immutable;

CREATE OR REPLACE FUNCTION ilztext_rne("left" text, "right" ilztext) RETURNS bool AS $$
    SELECT ltrim(lower("left"), '0') <> ltrim(lower("right"), '0');
$$ LANGUAGE SQL immutable;

CREATE OR REPLACE FUNCTION ilztext_lne("left" ilztext, "right" text) RETURNS bool AS $$
    SELECT ltrim(lower("left"), '0') <> ltrim(lower("right"), '0');
$$ LANGUAGE SQL immutable;

CREATE OR REPLACE FUNCTION ilztext_bne("left" ilztext, "right" ilztext) RETURNS bool AS $$
    SELECT ltrim(lower("left"), '0') <> ltrim(lower("right"), '0');
$$ LANGUAGE SQL immutable;

-- These functions allow ilztext to operate correctly on a B-tree index.
CREATE OR REPLACE FUNCTION ilztext_rlt("left" text, "right" ilztext) RETURNS bool AS $$
    SELECT ltrim(lower("left"), '0') < ltrim(lower("right"), '0');
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION ilztext_llt("left" ilztext, "right" text) RETURNS bool AS $$
    SELECT ltrim(lower("left"), '0') < ltrim(lower("right"), '0');
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION ilztext_blt("left" ilztext, "right" ilztext) RETURNS bool AS $$
    SELECT ltrim(lower("left"), '0') < ltrim(lower("right"), '0');
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION ilztext_rgt("left" text, "right" ilztext) RETURNS bool AS $$
    SELECT ltrim(lower("left"), '0') > ltrim(lower("right"), '0');
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION ilztext_lgt("left" ilztext, "right" text) RETURNS bool AS $$
    SELECT ltrim(lower("left"), '0') > ltrim(lower("right"), '0');
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION ilztext_bgt("left" ilztext, "right" ilztext) RETURNS bool AS $$
    SELECT ltrim(lower("left"), '0') > ltrim(lower("right"), '0');
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION ilztext_rle("left" text, "right" ilztext) RETURNS bool AS $$
    SELECT ltrim(lower("left"), '0') <= ltrim(lower("right"), '0');
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION ilztext_lle("left" ilztext, "right" text) RETURNS bool AS $$
    SELECT ltrim(lower("left"), '0') <= ltrim(lower("right"), '0');
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION ilztext_ble("left" ilztext, "right" ilztext) RETURNS bool AS $$
    SELECT ltrim(lower("left"), '0') <= ltrim(lower("right"), '0');
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION ilztext_rge("left" text, "right" ilztext) RETURNS bool AS $$
    SELECT ltrim(lower("left"), '0') >= ltrim(lower("right"), '0');
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION ilztext_lge("left" ilztext, "right" text) RETURNS bool AS $$
    SELECT ltrim(lower("left"), '0') >= ltrim(lower("right"), '0');
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION ilztext_bge("left" ilztext, "right" ilztext) RETURNS bool AS $$
    SELECT ltrim(lower("left"), '0') >= ltrim(lower("right"), '0');
$$ LANGUAGE sql IMMUTABLE;


/* Custom operators which implement direct comparisons using the functions defined above.

   This allows us to use logical comparisons in sql with native syntax, e.g.:

        SELECT "FOO "::ilztext = "foo"::ilztext;

   Only caveat is that `eligibility` must be a part of the search path
   (which we manage on connect).

   In its full complement, it also ensures we can compare correctly using joins and indexes.
*/
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

CREATE OPERATOR < (
    leftarg = ilztext,
    rightarg = ilztext,
    negator = >,
    commutator = <,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel,
    MERGES,
    PROCEDURE = ilztext_blt
);

CREATE OPERATOR <= (
    leftarg = ilztext,
    rightarg = ilztext,
    negator = >=,
    commutator = <=,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel,
    MERGES,
    PROCEDURE = ilztext_ble
);

CREATE OPERATOR >= (
    leftarg = ilztext,
    rightarg = ilztext,
    negator = <=,
    commutator = >=,
    RESTRICT = scalargesel,
    JOIN = scalargejoinsel,
    MERGES,
    PROCEDURE = ilztext_bge
);

CREATE OPERATOR > (
    leftarg = ilztext,
    rightarg = ilztext,
    negator = <,
    commutator = >,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel,
    MERGES,
    PROCEDURE = ilztext_bgt
);

CREATE OPERATOR < (
    leftarg = text,
    rightarg = ilztext,
    negator = >,
    commutator = <,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel,
    MERGES,
    PROCEDURE = ilztext_rlt
);

CREATE OPERATOR <= (
    leftarg = text,
    rightarg = ilztext,
    negator = >=,
    commutator = <=,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel,
    MERGES,
    PROCEDURE = ilztext_rle
);

CREATE OPERATOR > (
    leftarg = text,
    rightarg = ilztext,
    negator = <,
    commutator = >,
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
    commutator = <,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel,
    MERGES,
    PROCEDURE = ilztext_llt
);

CREATE OPERATOR <= (
    leftarg = ilztext,
    rightarg = text,
    negator = >=,
    commutator = <=,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel,
    MERGES,
    PROCEDURE = ilztext_lle
);

CREATE OPERATOR > (
    leftarg = ilztext,
    rightarg = text,
    negator = >,
    commutator = <,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel,
    MERGES,
    PROCEDURE = ilztext_lgt
);

CREATE OPERATOR >= (
    leftarg = ilztext,
    rightarg = text,
    negator = <=,
    commutator = >=,
    RESTRICT = scalargesel,
    JOIN = scalargejoinsel,
    MERGES,
    PROCEDURE = ilztext_lge
);

-- This is a cmp function for use in a custom btree operator class.
CREATE OR REPLACE FUNCTION ilztext_same("left" text, "right" ilztext) RETURNS int AS
$$
    SELECT CASE
        WHEN "left" < "right" THEN -1
        WHEN "left" > "right" THEN +1
        ELSE 0
    END;
$$ LANGUAGE sql IMMUTABLE ;

CREATE OR REPLACE FUNCTION ilztext_same("left" ilztext, "right" text) RETURNS int AS
$$
    SELECT CASE
        WHEN "left" < "right" THEN -1
        WHEN "left" > "right" THEN +1
        ELSE 0
    END;
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION ilztext_same("left" ilztext, "right" ilztext) RETURNS int AS
$$
    SELECT CASE
        WHEN "left" < "right" THEN -1
        WHEN "left" > "right" THEN +1
        ELSE 0
    END;
$$ LANGUAGE sql IMMUTABLE;


SET SEARCH_PATH = "public";

-- migrate:down

DROP DOMAIN eligibility.ilztext CASCADE;
ALTER DOMAIN eligibility.ilztextci RENAME TO ilztext;
