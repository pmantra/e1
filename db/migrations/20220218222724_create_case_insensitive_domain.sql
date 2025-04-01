-- migrate:up

/* The `eligibility.citext` is a best-effort case-insensitive text type.

   We avoid an exhaustive case-insensitive collation, because this in non-deterministic,
   so only has limited success with indexes.

   First, we create the 'domain', which is essentially a sub-type.
   Then, we tell Postgres how to compare it to itself and other text types.

   For every common logical operator (=, <>, <, >, <=, >=), there are three variations:
        1. "right-handed": right side is citext, left side is text.
        2. "left-handed": left side is citext, right side is text.
        3. "both-handed": both sides are citext.
 */

-- setting the search path because this is just too verbose
SET search_path = "eligibility";

-- Rename the current domain so that we can start to replace it.

CREATE DOMAIN citext TEXT;

-- Create our direct equality functions, making them compatible with plain text as well.
CREATE OR REPLACE FUNCTION citext_req("left" text, "right" citext) RETURNS bool AS $$
    SELECT lower("left") = lower("right");
$$ LANGUAGE SQL immutable;

CREATE OR REPLACE FUNCTION citext_leq("left" citext, "right" text) RETURNS bool AS $$
    SELECT lower("left") = lower("right");
$$ LANGUAGE SQL immutable;

CREATE OR REPLACE FUNCTION citext_beq("left" citext, "right" citext) RETURNS bool AS $$
    SELECT lower("left") = lower("right");
$$ LANGUAGE SQL immutable;

CREATE OR REPLACE FUNCTION citext_rne("left" text, "right" citext) RETURNS bool AS $$
    SELECT lower("left") <> lower("right");
$$ LANGUAGE SQL immutable;

CREATE OR REPLACE FUNCTION citext_lne("left" citext, "right" text) RETURNS bool AS $$
    SELECT lower("left") <> lower("right");
$$ LANGUAGE SQL immutable;

CREATE OR REPLACE FUNCTION citext_bne("left" citext, "right" citext) RETURNS bool AS $$
    SELECT lower("left") <> lower("right");
$$ LANGUAGE SQL immutable;

-- These functions allow citext to operate correctly on a B-tree index.
CREATE OR REPLACE FUNCTION citext_rlt("left" text, "right" citext) RETURNS bool AS $$
    SELECT lower("left") < lower("right");
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION citext_llt("left" citext, "right" text) RETURNS bool AS $$
    SELECT lower("left") < lower("right");
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION citext_blt("left" citext, "right" citext) RETURNS bool AS $$
    SELECT lower("left") < lower("right");
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION citext_rgt("left" text, "right" citext) RETURNS bool AS $$
    SELECT lower("left") > lower("right");
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION citext_lgt("left" citext, "right" text) RETURNS bool AS $$
    SELECT lower("left") > lower("right");
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION citext_bgt("left" citext, "right" citext) RETURNS bool AS $$
    SELECT lower("left") > lower("right");
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION citext_rle("left" text, "right" citext) RETURNS bool AS $$
    SELECT lower("left") <= lower("right");
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION citext_lle("left" citext, "right" text) RETURNS bool AS $$
    SELECT lower("left") <= lower("right");
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION citext_ble("left" citext, "right" citext) RETURNS bool AS $$
    SELECT lower("left") <= lower("right");
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION citext_rge("left" text, "right" citext) RETURNS bool AS $$
    SELECT lower("left") >= lower("right");
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION citext_lge("left" citext, "right" text) RETURNS bool AS $$
    SELECT lower("left") >= lower("right");
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION citext_bge("left" citext, "right" citext) RETURNS bool AS $$
    SELECT lower("left") >= lower("right");
$$ LANGUAGE sql IMMUTABLE;


/* Custom operators which implement direct comparisons using the functions defined above.

   This allows us to use logical comparisons in sql with native syntax, e.g.:

        SELECT "FOO "::citext = "foo"::citext;

   Only caveat is that `eligibility` must be a part of the search path
   (which we manage on connect).

   In its full complement, it also ensures we can compare correctly using joins and indexes.
*/
CREATE OPERATOR = (
    leftarg = text,
    rightarg = citext,
    negator = <>,
    commutator = =,
    RESTRICT = eqsel,
    JOIN = eqjoinsel,
    MERGES,
    PROCEDURE = citext_req
);
CREATE OPERATOR <> (
    leftarg = text,
    rightarg = citext,
    negator = =,
    commutator = <>,
    RESTRICT = neqsel,
    JOIN = neqjoinsel,
    MERGES,
    PROCEDURE = citext_rne
);
CREATE OPERATOR = (
    leftarg = citext,
    rightarg = text,
    negator = <>,
    commutator = =,
    RESTRICT = eqsel,
    JOIN = eqjoinsel,
    MERGES,
    PROCEDURE = citext_leq
);
CREATE OPERATOR <> (
    leftarg = citext,
    rightarg = text,
    negator = =,
    commutator = <>,
    RESTRICT = neqsel,
    JOIN = neqjoinsel,
    MERGES,
    PROCEDURE = citext_lne
);
CREATE OPERATOR = (
    leftarg = citext,
    rightarg = citext,
    negator = <>,
    commutator = =,
    RESTRICT = eqsel,
    JOIN = eqjoinsel,
    MERGES,
    PROCEDURE = citext_beq
);
CREATE OPERATOR <> (
    leftarg = citext,
    rightarg = citext,
    negator = =,
    commutator = <>,
    RESTRICT = neqsel,
    JOIN = neqjoinsel,
    MERGES,
    PROCEDURE = citext_bne
);

CREATE OPERATOR < (
    leftarg = citext,
    rightarg = citext,
    negator = >,
    commutator = <,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel,
    MERGES,
    PROCEDURE = citext_blt
);

CREATE OPERATOR <= (
    leftarg = citext,
    rightarg = citext,
    negator = >=,
    commutator = <=,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel,
    MERGES,
    PROCEDURE = citext_ble
);

CREATE OPERATOR >= (
    leftarg = citext,
    rightarg = citext,
    negator = <=,
    commutator = >=,
    RESTRICT = scalargesel,
    JOIN = scalargejoinsel,
    MERGES,
    PROCEDURE = citext_bge
);

CREATE OPERATOR > (
    leftarg = citext,
    rightarg = citext,
    negator = <,
    commutator = >,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel,
    MERGES,
    PROCEDURE = citext_bgt
);

CREATE OPERATOR < (
    leftarg = text,
    rightarg = citext,
    negator = >,
    commutator = <,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel,
    MERGES,
    PROCEDURE = citext_rlt
);

CREATE OPERATOR <= (
    leftarg = text,
    rightarg = citext,
    negator = >=,
    commutator = <=,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel,
    MERGES,
    PROCEDURE = citext_rle
);

CREATE OPERATOR > (
    leftarg = text,
    rightarg = citext,
    negator = <,
    commutator = >,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel,
    MERGES,
    PROCEDURE = citext_rgt
);

CREATE OPERATOR >= (
    leftarg = text,
    rightarg = citext,
    negator = <=,
    commutator = <=,
    RESTRICT = scalargesel,
    JOIN = scalargejoinsel,
    MERGES,
    PROCEDURE = citext_rge
);

CREATE OPERATOR < (
    leftarg = citext,
    rightarg = text,
    negator = >,
    commutator = <,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel,
    MERGES,
    PROCEDURE = citext_llt
);

CREATE OPERATOR <= (
    leftarg = citext,
    rightarg = text,
    negator = >=,
    commutator = <=,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel,
    MERGES,
    PROCEDURE = citext_lle
);

CREATE OPERATOR > (
    leftarg = citext,
    rightarg = text,
    negator = >,
    commutator = <,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel,
    MERGES,
    PROCEDURE = citext_lgt
);

CREATE OPERATOR >= (
    leftarg = citext,
    rightarg = text,
    negator = <=,
    commutator = >=,
    RESTRICT = scalargesel,
    JOIN = scalargejoinsel,
    MERGES,
    PROCEDURE = citext_lge
);

-- This is a cmp function for use in a custom btree operator class.
CREATE OR REPLACE FUNCTION citext_same("left" text, "right" citext) RETURNS int AS
$$
    SELECT CASE
        WHEN "left" < "right" THEN -1
        WHEN "left" > "right" THEN +1
        ELSE 0
    END;
$$ LANGUAGE sql IMMUTABLE ;

CREATE OR REPLACE FUNCTION citext_same("left" citext, "right" text) RETURNS int AS
$$
    SELECT CASE
        WHEN "left" < "right" THEN -1
        WHEN "left" > "right" THEN +1
        ELSE 0
    END;
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION citext_same("left" citext, "right" citext) RETURNS int AS
$$
    SELECT CASE
        WHEN "left" < "right" THEN -1
        WHEN "left" > "right" THEN +1
        ELSE 0
    END;
$$ LANGUAGE sql IMMUTABLE;


SET SEARCH_PATH = "public";

-- migrate:down

DROP DOMAIN eligibility.citext CASCADE;
