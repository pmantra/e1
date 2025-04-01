SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: eligibility; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA eligibility;


--
-- Name: ci; Type: COLLATION; Schema: eligibility; Owner: -
--

CREATE COLLATION eligibility.ci (provider = icu, deterministic = false, locale = 'und-u-ks-level2');


--
-- Name: btree_gin; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS btree_gin WITH SCHEMA public;


--
-- Name: EXTENSION btree_gin; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION btree_gin IS 'support for indexing common datatypes in GIN';


--
-- Name: pg_trgm; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;


--
-- Name: EXTENSION pg_trgm; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams';


--
-- Name: citext; Type: DOMAIN; Schema: eligibility; Owner: -
--

CREATE DOMAIN eligibility.citext AS text;


--
-- Name: client_specific_implementation; Type: TYPE; Schema: eligibility; Owner: -
--

CREATE TYPE eligibility.client_specific_implementation AS ENUM (
    'MICROSOFT'
);


--
-- Name: ilztext; Type: DOMAIN; Schema: eligibility; Owner: -
--

CREATE DOMAIN eligibility.ilztext AS text;


--
-- Name: iwstext; Type: DOMAIN; Schema: eligibility; Owner: -
--

CREATE DOMAIN eligibility.iwstext AS text;


--
-- Name: external_record; Type: TYPE; Schema: eligibility; Owner: -
--

CREATE TYPE eligibility.external_record AS (
	first_name text,
	last_name text,
	email text,
	unique_corp_id eligibility.ilztext,
	dependent_id eligibility.citext,
	date_of_birth date,
	work_state text,
	record jsonb,
	effective_range daterange,
	source text,
	external_id text,
	external_name text,
	received_ts bigint,
	do_not_contact eligibility.iwstext,
	gender_code eligibility.iwstext,
	employer_assigned_id eligibility.iwstext,
	organization_id bigint,
	work_country eligibility.iwstext,
	custom_attributes jsonb,
	hash_value eligibility.iwstext,
	hash_version integer
);


--
-- Name: file_error; Type: TYPE; Schema: eligibility; Owner: -
--

CREATE TYPE eligibility.file_error AS ENUM (
    'missing',
    'delimiter',
    'unknown'
);


--
-- Name: id_to_id; Type: TYPE; Schema: eligibility; Owner: -
--

CREATE TYPE eligibility.id_to_id AS (
	source_id integer,
	target_id integer
);


--
-- Name: id_to_range; Type: TYPE; Schema: eligibility; Owner: -
--

CREATE TYPE eligibility.id_to_range AS (
	id integer,
	range daterange
);


--
-- Name: id_to_text; Type: TYPE; Schema: eligibility; Owner: -
--

CREATE TYPE eligibility.id_to_text AS (
	id integer,
	text text
);


--
-- Name: ilztextci; Type: DOMAIN; Schema: eligibility; Owner: -
--

CREATE DOMAIN eligibility.ilztextci AS text COLLATE eligibility.ci;


--
-- Name: org_identity; Type: TYPE; Schema: eligibility; Owner: -
--

CREATE TYPE eligibility.org_identity AS (
	organization_id bigint,
	unique_corp_id eligibility.ilztext,
	dependent_id eligibility.citext
);


--
-- Name: parsed_record; Type: TYPE; Schema: eligibility; Owner: -
--

CREATE TYPE eligibility.parsed_record AS (
	organization_id bigint,
	first_name eligibility.iwstext,
	last_name eligibility.iwstext,
	email eligibility.iwstext,
	unique_corp_id eligibility.ilztext,
	dependent_id eligibility.citext,
	date_of_birth date,
	work_state eligibility.iwstext,
	do_not_contact eligibility.iwstext,
	gender_code eligibility.iwstext,
	employer_assigned_id eligibility.iwstext,
	record jsonb,
	file_id bigint,
	effective_range daterange,
	work_country eligibility.iwstext,
	custom_attributes jsonb,
	pre_verified boolean,
	hash_value eligibility.iwstext,
	hash_version integer
);


--
-- Name: default_range(); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.default_range() RETURNS daterange
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT daterange((current_date - INTERVAL '1 day')::date, null, '[)');
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: file_parse_results; Type: TABLE; Schema: eligibility; Owner: -
--

CREATE TABLE eligibility.file_parse_results (
    id bigint NOT NULL,
    organization_id bigint NOT NULL,
    file_id bigint,
    first_name eligibility.iwstext DEFAULT ''::text NOT NULL,
    last_name eligibility.iwstext DEFAULT ''::text NOT NULL,
    email eligibility.iwstext DEFAULT ''::text NOT NULL,
    unique_corp_id eligibility.ilztext DEFAULT ''::text NOT NULL,
    dependent_id eligibility.citext DEFAULT ''::text NOT NULL,
    date_of_birth date NOT NULL,
    work_state eligibility.iwstext,
    record jsonb,
    errors text[] DEFAULT '{}'::text[] NOT NULL,
    warnings text[] DEFAULT '{}'::text[] NOT NULL,
    effective_range daterange DEFAULT eligibility.default_range(),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    do_not_contact eligibility.iwstext,
    gender_code eligibility.iwstext,
    employer_assigned_id eligibility.iwstext,
    work_country eligibility.citext DEFAULT NULL::text,
    custom_attributes jsonb,
    hash_value eligibility.iwstext,
    hash_version integer
);


--
-- Name: get_parsed_record_from_file(eligibility.file_parse_results); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.get_parsed_record_from_file(record eligibility.file_parse_results) RETURNS eligibility.parsed_record
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT (
         record.organization_id::bigint,
         record.first_name::eligibility.iwstext,
         record.last_name::eligibility.iwstext,
         record.email::eligibility.iwstext,
         record.unique_corp_id::eligibility.ilztext,
         record.dependent_id::eligibility.citext,
         record.date_of_birth::date,
         record.work_state::eligibility.iwstext,
         record.do_not_contact::eligibility.iwstext,
         record.gender_code::eligibility.iwstext,
         record.employer_assigned_id::eligibility.iwstext,
         record.record::jsonb,
         record.file_id::bigint,
         record.effective_range::daterange
    )::eligibility.parsed_record
$$;


--
-- Name: CAST (eligibility.file_parse_results AS eligibility.parsed_record); Type: CAST; Schema: -; Owner: -
--

CREATE CAST (eligibility.file_parse_results AS eligibility.parsed_record) WITH FUNCTION eligibility.get_parsed_record_from_file(eligibility.file_parse_results) AS IMPLICIT;


--
-- Name: member; Type: TABLE; Schema: eligibility; Owner: -
--

CREATE TABLE eligibility.member (
    id bigint NOT NULL,
    organization_id bigint NOT NULL,
    file_id bigint,
    first_name eligibility.iwstext DEFAULT ''::text NOT NULL,
    last_name eligibility.iwstext DEFAULT ''::text NOT NULL,
    email eligibility.iwstext DEFAULT ''::text NOT NULL,
    unique_corp_id eligibility.ilztext DEFAULT ''::text NOT NULL,
    dependent_id eligibility.citext DEFAULT ''::text NOT NULL,
    date_of_birth date NOT NULL,
    work_state eligibility.iwstext,
    record jsonb,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    effective_range daterange DEFAULT eligibility.default_range() NOT NULL,
    do_not_contact eligibility.iwstext,
    gender_code eligibility.iwstext,
    employer_assigned_id eligibility.iwstext,
    work_country eligibility.citext DEFAULT NULL::text,
    custom_attributes jsonb
);


--
-- Name: get_identity(eligibility.member); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.get_identity(eligibility.member) RETURNS eligibility.org_identity
    LANGUAGE sql IMMUTABLE
    AS $_$
    SELECT (
        $1.organization_id, ltrim($1.unique_corp_id, '0'), $1.dependent_id
    )::eligibility.org_identity;
$_$;


--
-- Name: CAST (eligibility.member AS eligibility.org_identity); Type: CAST; Schema: -; Owner: -
--

CREATE CAST (eligibility.member AS eligibility.org_identity) WITH FUNCTION eligibility.get_identity(eligibility.member) AS IMPLICIT;


--
-- Name: get_parsed_record_from_member(eligibility.member); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.get_parsed_record_from_member(record eligibility.member) RETURNS eligibility.parsed_record
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT (
         record.organization_id::bigint,
         record.first_name::eligibility.iwstext,
         record.last_name::eligibility.iwstext,
         record.email::eligibility.iwstext,
         record.unique_corp_id::eligibility.ilztext,
         record.dependent_id::eligibility.citext,
         record.date_of_birth::date,
         record.work_state::eligibility.iwstext,
         record.do_not_contact::eligibility.iwstext,
         record.gender_code::eligibility.iwstext,
         record.employer_assigned_id::eligibility.iwstext,
         record.record::jsonb,
         record.file_id::bigint,
         record.effective_range::daterange

    )::eligibility.parsed_record
$$;


--
-- Name: CAST (eligibility.member AS eligibility.parsed_record); Type: CAST; Schema: -; Owner: -
--

CREATE CAST (eligibility.member AS eligibility.parsed_record) WITH FUNCTION eligibility.get_parsed_record_from_member(eligibility.member) AS IMPLICIT;


--
-- Name: batch_migrate_member(integer, integer); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.batch_migrate_member(start_id integer, end_id integer) RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO eligibility.member_versioned(id, organization_id, file_id, first_name, last_name, email, unique_corp_id, dependent_id, date_of_birth, work_state, record, created_at, updated_at, effective_range, do_not_contact, gender_code, employer_assigned_id)
    SELECT id, organization_id, file_id, first_name, last_name, email, unique_corp_id, dependent_id, date_of_birth, work_state, record, created_at, updated_at, effective_range, do_not_contact, gender_code, employer_assigned_id
    FROM eligibility.member
    WHERE id >= start_id AND id < end_id;
END;
$$;


--
-- Name: batch_migrate_member_address(integer, integer); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.batch_migrate_member_address(start_id integer, end_id integer) RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO eligibility.member_address_versioned
    SELECT *
    FROM eligibility.member_address
    WHERE id >= start_id AND id < end_id;
END;
$$;


--
-- Name: citext_beq(eligibility.citext, eligibility.citext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.citext_beq("left" eligibility.citext, "right" eligibility.citext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT lower("left") = lower("right");
$$;


--
-- Name: citext_bge(eligibility.citext, eligibility.citext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.citext_bge("left" eligibility.citext, "right" eligibility.citext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT lower("left") >= lower("right");
$$;


--
-- Name: citext_bgt(eligibility.citext, eligibility.citext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.citext_bgt("left" eligibility.citext, "right" eligibility.citext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT lower("left") > lower("right");
$$;


--
-- Name: citext_ble(eligibility.citext, eligibility.citext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.citext_ble("left" eligibility.citext, "right" eligibility.citext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT lower("left") <= lower("right");
$$;


--
-- Name: citext_blt(eligibility.citext, eligibility.citext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.citext_blt("left" eligibility.citext, "right" eligibility.citext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT lower("left") < lower("right");
$$;


--
-- Name: citext_bne(eligibility.citext, eligibility.citext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.citext_bne("left" eligibility.citext, "right" eligibility.citext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT lower("left") <> lower("right");
$$;


--
-- Name: citext_leq(eligibility.citext, text); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.citext_leq("left" eligibility.citext, "right" text) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT lower("left") = lower("right");
$$;


--
-- Name: citext_lge(eligibility.citext, text); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.citext_lge("left" eligibility.citext, "right" text) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT lower("left") >= lower("right");
$$;


--
-- Name: citext_lgt(eligibility.citext, text); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.citext_lgt("left" eligibility.citext, "right" text) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT lower("left") > lower("right");
$$;


--
-- Name: citext_lle(eligibility.citext, text); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.citext_lle("left" eligibility.citext, "right" text) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT lower("left") <= lower("right");
$$;


--
-- Name: citext_llt(eligibility.citext, text); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.citext_llt("left" eligibility.citext, "right" text) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT lower("left") < lower("right");
$$;


--
-- Name: citext_lne(eligibility.citext, text); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.citext_lne("left" eligibility.citext, "right" text) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT lower("left") <> lower("right");
$$;


--
-- Name: citext_req(text, eligibility.citext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.citext_req("left" text, "right" eligibility.citext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT lower("left") = lower("right");
$$;


--
-- Name: citext_rge(text, eligibility.citext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.citext_rge("left" text, "right" eligibility.citext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT lower("left") >= lower("right");
$$;


--
-- Name: citext_rgt(text, eligibility.citext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.citext_rgt("left" text, "right" eligibility.citext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT lower("left") > lower("right");
$$;


--
-- Name: citext_rle(text, eligibility.citext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.citext_rle("left" text, "right" eligibility.citext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT lower("left") <= lower("right");
$$;


--
-- Name: citext_rlt(text, eligibility.citext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.citext_rlt("left" text, "right" eligibility.citext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT lower("left") < lower("right");
$$;


--
-- Name: citext_rne(text, eligibility.citext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.citext_rne("left" text, "right" eligibility.citext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT lower("left") <> lower("right");
$$;


--
-- Name: citext_same(eligibility.citext, eligibility.citext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.citext_same("left" eligibility.citext, "right" eligibility.citext) RETURNS integer
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT CASE
        WHEN "left" < "right" THEN -1
        WHEN "left" > "right" THEN +1
        ELSE 0
    END;
$$;


--
-- Name: citext_same(eligibility.citext, text); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.citext_same("left" eligibility.citext, "right" text) RETURNS integer
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT CASE
        WHEN "left" < "right" THEN -1
        WHEN "left" > "right" THEN +1
        ELSE 0
    END;
$$;


--
-- Name: citext_same(text, eligibility.citext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.citext_same("left" text, "right" eligibility.citext) RETURNS integer
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT CASE
        WHEN "left" < "right" THEN -1
        WHEN "left" > "right" THEN +1
        ELSE 0
    END;
$$;


--
-- Name: get_header_mapping(bigint); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.get_header_mapping(bigint) RETURNS jsonb
    LANGUAGE sql IMMUTABLE
    AS $_$
    WITH headers AS (
        SELECT jsonb_object_agg(header, alias) AS headers
        FROM header_alias
        WHERE organization_id = $1
    )
    SELECT (
        CASE WHEN headers.headers IS NULL
            THEN '{}'::jsonb
            ELSE headers.headers END
        )
    FROM headers
$_$;


--
-- Name: ilztext_beq(eligibility.ilztext, eligibility.ilztext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_beq("left" eligibility.ilztext, "right" eligibility.ilztext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT ltrim(lower("left"), '0') = ltrim(lower("right"), '0');
$$;


--
-- Name: ilztext_beq(eligibility.ilztextci, eligibility.ilztextci); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_beq(eligibility.ilztextci, eligibility.ilztextci) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $_$
    SELECT ltrim($1, '0') = ltrim($2, '0');
$_$;


--
-- Name: ilztext_bge(eligibility.ilztext, eligibility.ilztext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_bge("left" eligibility.ilztext, "right" eligibility.ilztext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT ltrim(lower("left"), '0') >= ltrim(lower("right"), '0');
$$;


--
-- Name: ilztext_bge(eligibility.ilztextci, eligibility.ilztextci); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_bge(eligibility.ilztextci, eligibility.ilztextci) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $_$
    SELECT ltrim($1, '0') >= ltrim($2, '0');
$_$;


--
-- Name: ilztext_bgt(eligibility.ilztext, eligibility.ilztext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_bgt("left" eligibility.ilztext, "right" eligibility.ilztext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT ltrim(lower("left"), '0') > ltrim(lower("right"), '0');
$$;


--
-- Name: ilztext_bgt(eligibility.ilztextci, eligibility.ilztextci); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_bgt(eligibility.ilztextci, eligibility.ilztextci) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $_$
    SELECT ltrim($1, '0') > ltrim($2, '0');
$_$;


--
-- Name: ilztext_ble(eligibility.ilztext, eligibility.ilztext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_ble("left" eligibility.ilztext, "right" eligibility.ilztext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT ltrim(lower("left"), '0') <= ltrim(lower("right"), '0');
$$;


--
-- Name: ilztext_ble(eligibility.ilztextci, eligibility.ilztextci); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_ble(eligibility.ilztextci, eligibility.ilztextci) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $_$
    SELECT ltrim($1, '0') <= ltrim($2, '0');
$_$;


--
-- Name: ilztext_blt(eligibility.ilztext, eligibility.ilztext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_blt("left" eligibility.ilztext, "right" eligibility.ilztext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT ltrim(lower("left"), '0') < ltrim(lower("right"), '0');
$$;


--
-- Name: ilztext_blt(eligibility.ilztextci, eligibility.ilztextci); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_blt(eligibility.ilztextci, eligibility.ilztextci) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $_$
    SELECT ltrim($1, '0') < ltrim($2, '0');
$_$;


--
-- Name: ilztext_bne(eligibility.ilztext, eligibility.ilztext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_bne("left" eligibility.ilztext, "right" eligibility.ilztext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT ltrim(lower("left"), '0') <> ltrim(lower("right"), '0');
$$;


--
-- Name: ilztext_bne(eligibility.ilztextci, eligibility.ilztextci); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_bne(eligibility.ilztextci, eligibility.ilztextci) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $_$
    SELECT ltrim($1, '0') <> ltrim($2, '0');
$_$;


--
-- Name: ilztext_leq(eligibility.ilztext, text); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_leq("left" eligibility.ilztext, "right" text) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT ltrim(lower("left"), '0') = ltrim(lower("right"), '0');
$$;


--
-- Name: ilztext_leq(eligibility.ilztextci, text); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_leq(eligibility.ilztextci, text) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $_$
    SELECT ltrim($1, '0') = ltrim($2, '0');
$_$;


--
-- Name: ilztext_lge(eligibility.ilztext, text); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_lge("left" eligibility.ilztext, "right" text) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT ltrim(lower("left"), '0') >= ltrim(lower("right"), '0');
$$;


--
-- Name: ilztext_lge(eligibility.ilztextci, text); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_lge(eligibility.ilztextci, text) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $_$
    SELECT ltrim($1, '0') >= ltrim($2, '0');
$_$;


--
-- Name: ilztext_lgt(eligibility.ilztext, text); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_lgt("left" eligibility.ilztext, "right" text) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT ltrim(lower("left"), '0') > ltrim(lower("right"), '0');
$$;


--
-- Name: ilztext_lgt(eligibility.ilztextci, text); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_lgt(eligibility.ilztextci, text) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $_$
    SELECT ltrim($1, '0') > ltrim($2, '0');
$_$;


--
-- Name: ilztext_lle(eligibility.ilztext, text); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_lle("left" eligibility.ilztext, "right" text) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT ltrim(lower("left"), '0') <= ltrim(lower("right"), '0');
$$;


--
-- Name: ilztext_lle(eligibility.ilztextci, text); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_lle(eligibility.ilztextci, text) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $_$
    SELECT ltrim($1, '0') <= ltrim($2, '0');
$_$;


--
-- Name: ilztext_llt(eligibility.ilztext, text); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_llt("left" eligibility.ilztext, "right" text) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT ltrim(lower("left"), '0') < ltrim(lower("right"), '0');
$$;


--
-- Name: ilztext_llt(eligibility.ilztextci, text); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_llt(eligibility.ilztextci, text) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $_$
    SELECT ltrim($1, '0') < ltrim($2, '0');
$_$;


--
-- Name: ilztext_lne(eligibility.ilztext, text); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_lne("left" eligibility.ilztext, "right" text) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT ltrim(lower("left"), '0') <> ltrim(lower("right"), '0');
$$;


--
-- Name: ilztext_lne(eligibility.ilztextci, text); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_lne(eligibility.ilztextci, text) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $_$
    SELECT ltrim($1, '0') <> ltrim($2, '0');
$_$;


--
-- Name: ilztext_req(text, eligibility.ilztext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_req("left" text, "right" eligibility.ilztext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT ltrim(lower("left"), '0') = ltrim(lower("right"), '0');
$$;


--
-- Name: ilztext_req(text, eligibility.ilztextci); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_req(text, eligibility.ilztextci) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $_$
    SELECT ltrim($1, '0') = ltrim($2, '0');
$_$;


--
-- Name: ilztext_rge(text, eligibility.ilztext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_rge("left" text, "right" eligibility.ilztext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT ltrim(lower("left"), '0') >= ltrim(lower("right"), '0');
$$;


--
-- Name: ilztext_rge(text, eligibility.ilztextci); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_rge(text, eligibility.ilztextci) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $_$
    SELECT ltrim($1, '0') >= ltrim($2, '0');
$_$;


--
-- Name: ilztext_rgt(text, eligibility.ilztext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_rgt("left" text, "right" eligibility.ilztext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT ltrim(lower("left"), '0') > ltrim(lower("right"), '0');
$$;


--
-- Name: ilztext_rgt(text, eligibility.ilztextci); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_rgt(text, eligibility.ilztextci) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $_$
    SELECT ltrim($1, '0') > ltrim($2, '0');
$_$;


--
-- Name: ilztext_rle(text, eligibility.ilztext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_rle("left" text, "right" eligibility.ilztext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT ltrim(lower("left"), '0') <= ltrim(lower("right"), '0');
$$;


--
-- Name: ilztext_rle(text, eligibility.ilztextci); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_rle(text, eligibility.ilztextci) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $_$
    SELECT ltrim($1, '0') <= ltrim($2, '0');
$_$;


--
-- Name: ilztext_rlt(text, eligibility.ilztext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_rlt("left" text, "right" eligibility.ilztext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT ltrim(lower("left"), '0') < ltrim(lower("right"), '0');
$$;


--
-- Name: ilztext_rlt(text, eligibility.ilztextci); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_rlt(text, eligibility.ilztextci) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $_$
    SELECT ltrim($1, '0') < ltrim($2, '0');
$_$;


--
-- Name: ilztext_rne(text, eligibility.ilztext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_rne("left" text, "right" eligibility.ilztext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT ltrim(lower("left"), '0') <> ltrim(lower("right"), '0');
$$;


--
-- Name: ilztext_rne(text, eligibility.ilztextci); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_rne(text, eligibility.ilztextci) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $_$
    SELECT ltrim($1, '0') <> ltrim($2, '0');
$_$;


--
-- Name: ilztext_same(eligibility.ilztext, eligibility.ilztext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_same("left" eligibility.ilztext, "right" eligibility.ilztext) RETURNS integer
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT CASE
        WHEN "left" < "right" THEN -1
        WHEN "left" > "right" THEN +1
        ELSE 0
    END;
$$;


--
-- Name: ilztext_same(eligibility.ilztext, text); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_same("left" eligibility.ilztext, "right" text) RETURNS integer
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT CASE
        WHEN "left" < "right" THEN -1
        WHEN "left" > "right" THEN +1
        ELSE 0
    END;
$$;


--
-- Name: ilztext_same(eligibility.ilztextci, eligibility.ilztextci); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_same(eligibility.ilztextci, eligibility.ilztextci) RETURNS integer
    LANGUAGE sql IMMUTABLE
    AS $_$
    SELECT CASE
        WHEN $1 < $2 THEN -1
        WHEN $1 > $2 THEN +1
        ELSE 0
    END;
$_$;


--
-- Name: ilztext_same(eligibility.ilztextci, text); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_same(eligibility.ilztextci, text) RETURNS integer
    LANGUAGE sql IMMUTABLE
    AS $_$
    SELECT CASE
        WHEN $1 < $2 THEN -1
        WHEN $1 > $2 THEN +1
        ELSE 0
    END;
$_$;


--
-- Name: ilztext_same(text, eligibility.ilztext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_same("left" text, "right" eligibility.ilztext) RETURNS integer
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT CASE
        WHEN "left" < "right" THEN -1
        WHEN "left" > "right" THEN +1
        ELSE 0
    END;
$$;


--
-- Name: ilztext_same(text, eligibility.ilztextci); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.ilztext_same(text, eligibility.ilztextci) RETURNS integer
    LANGUAGE sql IMMUTABLE
    AS $_$
    SELECT CASE
        WHEN $1 < $2 THEN -1
        WHEN $1 > $2 THEN +1
        ELSE 0
    END;
$_$;


--
-- Name: iwstext_beq(eligibility.iwstext, eligibility.iwstext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.iwstext_beq("left" eligibility.iwstext, "right" eligibility.iwstext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT trim(lower("left")) = trim(lower("right"));
$$;


--
-- Name: iwstext_bge(eligibility.iwstext, eligibility.iwstext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.iwstext_bge("left" eligibility.iwstext, "right" eligibility.iwstext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT trim(lower("left")) >= trim(lower("right"));
$$;


--
-- Name: iwstext_bgt(eligibility.iwstext, eligibility.iwstext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.iwstext_bgt("left" eligibility.iwstext, "right" eligibility.iwstext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT trim(lower("left")) > trim(lower("right"));
$$;


--
-- Name: iwstext_ble(eligibility.iwstext, eligibility.iwstext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.iwstext_ble("left" eligibility.iwstext, "right" eligibility.iwstext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT trim(lower("left")) <= trim(lower("right"));
$$;


--
-- Name: iwstext_blt(eligibility.iwstext, eligibility.iwstext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.iwstext_blt("left" eligibility.iwstext, "right" eligibility.iwstext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT trim(lower("left")) < trim(lower("right"));
$$;


--
-- Name: iwstext_bne(eligibility.iwstext, eligibility.iwstext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.iwstext_bne("left" eligibility.iwstext, "right" eligibility.iwstext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT trim(lower("left")) <> trim(lower("right"));
$$;


--
-- Name: iwstext_leq(eligibility.iwstext, text); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.iwstext_leq("left" eligibility.iwstext, "right" text) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT trim(lower("left")) = trim(lower("right"));
$$;


--
-- Name: iwstext_lge(eligibility.iwstext, text); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.iwstext_lge("left" eligibility.iwstext, "right" text) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT trim(lower("left")) >= trim(lower("right"));
$$;


--
-- Name: iwstext_lgt(eligibility.iwstext, text); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.iwstext_lgt("left" eligibility.iwstext, "right" text) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT trim(lower("left")) > trim(lower("right"));
$$;


--
-- Name: iwstext_lle(eligibility.iwstext, text); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.iwstext_lle("left" eligibility.iwstext, "right" text) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT trim(lower("left")) <= trim(lower("right"));
$$;


--
-- Name: iwstext_llt(eligibility.iwstext, text); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.iwstext_llt("left" eligibility.iwstext, "right" text) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT trim(lower("left")) < trim(lower("right"));
$$;


--
-- Name: iwstext_lne(eligibility.iwstext, text); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.iwstext_lne("left" eligibility.iwstext, "right" text) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT trim(lower("left")) <> trim(lower("right"));
$$;


--
-- Name: iwstext_req(text, eligibility.iwstext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.iwstext_req("left" text, "right" eligibility.iwstext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT trim(lower("left")) = trim(lower("right"));
$$;


--
-- Name: iwstext_rge(text, eligibility.iwstext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.iwstext_rge("left" text, "right" eligibility.iwstext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT trim(lower("left")) >= trim(lower("right"));
$$;


--
-- Name: iwstext_rgt(text, eligibility.iwstext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.iwstext_rgt("left" text, "right" eligibility.iwstext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT trim(lower("left")) > trim(lower("right"));
$$;


--
-- Name: iwstext_rle(text, eligibility.iwstext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.iwstext_rle("left" text, "right" eligibility.iwstext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT trim(lower("left")) <= trim(lower("right"));
$$;


--
-- Name: iwstext_rlt(text, eligibility.iwstext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.iwstext_rlt("left" text, "right" eligibility.iwstext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT trim(lower("left")) < trim(lower("right"));
$$;


--
-- Name: iwstext_rne(text, eligibility.iwstext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.iwstext_rne("left" text, "right" eligibility.iwstext) RETURNS boolean
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT trim(lower("left")) <> trim(lower("right"));
$$;


--
-- Name: iwstext_same(eligibility.iwstext, eligibility.iwstext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.iwstext_same("left" eligibility.iwstext, "right" eligibility.iwstext) RETURNS integer
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT CASE
        WHEN "left" < "right" THEN -1
        WHEN "left" > "right" THEN +1
        ELSE 0
    END;
$$;


--
-- Name: iwstext_same(eligibility.iwstext, text); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.iwstext_same("left" eligibility.iwstext, "right" text) RETURNS integer
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT CASE
        WHEN "left" < "right" THEN -1
        WHEN "left" > "right" THEN +1
        ELSE 0
    END;
$$;


--
-- Name: iwstext_same(text, eligibility.iwstext); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.iwstext_same("left" text, "right" eligibility.iwstext) RETURNS integer
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT CASE
        WHEN "left" < "right" THEN -1
        WHEN "left" > "right" THEN +1
        ELSE 0
    END;
$$;


--
-- Name: merge_file_parse_results_members(bigint[], boolean); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.merge_file_parse_results_members(files bigint[], show_missing boolean) RETURNS TABLE(id bigint, organization_id bigint, first_name eligibility.iwstext, last_name eligibility.iwstext, email eligibility.iwstext, unique_corp_id eligibility.ilztext, dependent_id eligibility.citext, date_of_birth date, work_state eligibility.iwstext, do_not_contact eligibility.iwstext, gender_code eligibility.iwstext, employer_assigned_id eligibility.iwstext, record jsonb, file_id bigint, effective_range daterange, errors eligibility.citext[], warnings eligibility.citext[], created_at timestamp with time zone, updated_at timestamp with time zone, is_missing boolean)
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT
        existing.id,
        (
            -- apply our 'pending' changes to the record, otherwise display the existing member info
            CASE WHEN parsed.id IS NULL
                THEN existing.parsed_record
                ELSE parsed.parsed_record
                END
        ).*,
        COALESCE(parsed.errors, '{}')::eligibility.citext[]   AS errors,
        COALESCE(parsed.warnings, '{}')::eligibility.citext[] AS warnings,
        existing.created_at,
        existing.updated_at,
        CASE WHEN parsed.id IS NULL THEN TRUE ELSE FALSE END AS is_missing
    FROM (
        -- grab all the members that correspond to the organizations we care about. we don't filter just on file_id, in the
        -- case that we get multiple orgs in a single file.
        SELECT id,
            organization_id ,
            unique_corp_id ,
            dependent_id ,
            m::eligibility.parsed_record AS parsed_record,
            created_at,
            updated_at
        FROM   eligibility."member" m
        WHERE  m.organization_id = ANY (
            SELECT organization_id
            FROM   eligibility.FILE
            WHERE  id = ANY (files)
        )
    ) AS existing
    FULL JOIN (
        -- grab all parsed records for the  fileIDs we care about
        SELECT
            id,
            organization_id,
            unique_corp_id,
            dependent_id,
            errors,
            warnings,
            fpr::eligibility.parsed_record AS parsed_record
        FROM
            eligibility.file_parse_results fpr
        WHERE
            file_id = ANY (files)
    ) AS parsed
    ON        existing.organization_id = parsed.organization_id
    AND       existing.unique_corp_id = parsed.unique_corp_id
    AND       existing.dependent_id = parsed.dependent_id
    WHERE
        coalesce(show_missing, TRUE)  OR parsed.id IS NOT NULL
$$;


--
-- Name: migrate_file_parse_results(bigint[]); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.migrate_file_parse_results(files bigint[]) RETURNS SETOF eligibility.member
    LANGUAGE sql
    AS $$
WITH records AS (
    DELETE FROM eligibility.file_parse_results
    WHERE file_id = ANY (files)
    RETURNING
        organization_id,
        first_name,
        last_name,
        email,
        unique_corp_id,
        dependent_id,
        date_of_birth,
        work_state,
        record,
        file_id,
        effective_range
)
INSERT INTO eligibility.member(
    organization_id,
    first_name,
    last_name,
    email,
    unique_corp_id,
    dependent_id,
    date_of_birth,
    work_state,
    record,
    file_id,
    effective_range
)
SELECT DISTINCT ON (
        pr.organization_id, lower(ltrim(pr.unique_corp_id, '0')), lower(pr.dependent_id)
    )
    pr.organization_id,
    pr.first_name,
    pr.last_name,
    pr.email,
    pr.unique_corp_id,
    pr.dependent_id,
    pr.date_of_birth,
    pr.work_state,
    coalesce(pr.record, '{}')::jsonb,
    pr.file_id,
    coalesce(pr.effective_range, eligibility.default_range())
FROM records pr
ON CONFLICT (
    organization_id, ltrim(lower(unique_corp_id), '0'), lower(dependent_id)
    )
    DO UPDATE SET
        organization_id = excluded.organization_id,
        first_name = excluded.first_name,
        last_name = excluded.last_name,
        email = excluded.email,
        unique_corp_id = excluded.unique_corp_id,
        dependent_id = excluded.dependent_id,
        date_of_birth = excluded.date_of_birth,
        work_state = excluded.work_state,
        record = excluded.record,
        file_id = excluded.file_id,
        effective_range = excluded.effective_range
RETURNING *
$$;


--
-- Name: migrate_file_parse_results_dual_write(bigint[]); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.migrate_file_parse_results_dual_write(files bigint[]) RETURNS SETOF eligibility.member
    LANGUAGE sql
    AS $$
WITH records AS (
    DELETE FROM eligibility.file_parse_results
    WHERE file_id = ANY (files)
    RETURNING
        organization_id,
        first_name,
        last_name,
        email,
        unique_corp_id,
        dependent_id,
        date_of_birth,
        work_state,
        work_country,
        record,
        custom_attributes,
        file_id,
        effective_range
), member_insert AS (
    INSERT INTO eligibility.member(
        organization_id,
        first_name,
        last_name,
        email,
        unique_corp_id,
        dependent_id,
        date_of_birth,
        work_state,
        work_country,
        record,
        custom_attributes,
        file_id,
        effective_range
    )
    SELECT DISTINCT ON (
            pr.organization_id, lower(ltrim(pr.unique_corp_id, '0')), lower(pr.dependent_id)
        )
        pr.organization_id,
        pr.first_name,
        pr.last_name,
        pr.email,
        pr.unique_corp_id,
        pr.dependent_id,
        pr.date_of_birth,
        pr.work_state,
        pr.work_country,
        coalesce(pr.record, '{}')::jsonb,
        coalesce(pr.custom_attributes, '{}')::jsonb,
        pr.file_id,
        coalesce(pr.effective_range, eligibility.default_range())
    FROM records pr
    ON CONFLICT (
        organization_id, ltrim(lower(unique_corp_id), '0'), lower(dependent_id)
        )
        DO UPDATE SET
            organization_id = excluded.organization_id,
            first_name = excluded.first_name,
            last_name = excluded.last_name,
            email = excluded.email,
            unique_corp_id = excluded.unique_corp_id,
            dependent_id = excluded.dependent_id,
            date_of_birth = excluded.date_of_birth,
            work_state = excluded.work_state,
            work_country = excluded.work_country,
            record = excluded.record,
            custom_attributes = excluded.custom_attributes,
            file_id = excluded.file_id,
            effective_range = excluded.effective_range
    RETURNING *
), member_versioned_insert AS (
    INSERT INTO eligibility.member_versioned(
        organization_id,
        first_name,
        last_name,
        email,
        unique_corp_id,
        dependent_id,
        date_of_birth,
        work_state,
        work_country,
        record,
        custom_attributes,
        file_id,
        effective_range
    )
    SELECT DISTINCT ON (
            pr.organization_id, lower(ltrim(pr.unique_corp_id, '0')), lower(pr.dependent_id)
        )
        pr.organization_id,
        pr.first_name,
        pr.last_name,
        pr.email,
        pr.unique_corp_id,
        pr.dependent_id,
        pr.date_of_birth,
        pr.work_state,
        pr.work_country,
        coalesce(pr.record, '{}')::jsonb,
        coalesce(pr.custom_attributes, '{}')::jsonb,
        pr.file_id,
        coalesce(pr.effective_range, eligibility.default_range())
    FROM records pr
)
SELECT * FROM member_insert;
$$;


--
-- Name: member_versioned; Type: TABLE; Schema: eligibility; Owner: -
--

CREATE TABLE eligibility.member_versioned (
    id bigint NOT NULL,
    organization_id bigint NOT NULL,
    file_id bigint,
    first_name eligibility.iwstext DEFAULT ''::text NOT NULL,
    last_name eligibility.iwstext DEFAULT ''::text NOT NULL,
    email eligibility.iwstext DEFAULT ''::text NOT NULL,
    unique_corp_id eligibility.ilztext DEFAULT ''::text NOT NULL,
    dependent_id eligibility.citext DEFAULT ''::text NOT NULL,
    date_of_birth date NOT NULL,
    work_state eligibility.iwstext,
    record jsonb,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    effective_range daterange DEFAULT eligibility.default_range() NOT NULL,
    policies jsonb,
    do_not_contact eligibility.iwstext,
    gender_code eligibility.iwstext,
    employer_assigned_id eligibility.iwstext,
    work_country eligibility.citext DEFAULT NULL::text,
    custom_attributes jsonb,
    pre_verified boolean DEFAULT false NOT NULL,
    hash_value eligibility.iwstext,
    hash_version integer
);


--
-- Name: migrate_file_parse_results_versioned(bigint[]); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.migrate_file_parse_results_versioned(files bigint[]) RETURNS SETOF eligibility.member_versioned
    LANGUAGE sql
    AS $$
WITH records AS (
    DELETE FROM eligibility.file_parse_results
    WHERE file_id = ANY (files)
    RETURNING
        organization_id,
        first_name,
        last_name,
        email,
        unique_corp_id,
        dependent_id,
        date_of_birth,
        work_state,
        work_country,
        record,
        custom_attributes,
        file_id,
        effective_range
)
INSERT INTO eligibility.member_versioned(
    organization_id,
    first_name,
    last_name,
    email,
    unique_corp_id,
    dependent_id,
    date_of_birth,
    work_state,
    work_country,
    record,
    custom_attributes,
    file_id,
    effective_range
)
SELECT DISTINCT ON (
        pr.organization_id, lower(ltrim(pr.unique_corp_id, '0')), lower(pr.dependent_id)
    )
    pr.organization_id,
    pr.first_name,
    pr.last_name,
    pr.email,
    pr.unique_corp_id,
    pr.dependent_id,
    pr.date_of_birth,
    pr.work_state,
    pr.work_country,
    coalesce(pr.record, '{}')::jsonb,
    coalesce(pr.custom_attributes, '{}')::jsonb,
    pr.file_id,
    coalesce(pr.effective_range, eligibility.default_range())
FROM records pr
RETURNING *
$$;


--
-- Name: trigger_set_timestamp(); Type: FUNCTION; Schema: eligibility; Owner: -
--

CREATE FUNCTION eligibility.trigger_set_timestamp() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  NEW.updated_at = CURRENT_TIMESTAMP;
  RETURN NEW;
END;
$$;


--
-- Name: <; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.< (
    FUNCTION = eligibility.ilztext_blt,
    LEFTARG = eligibility.ilztextci,
    RIGHTARG = eligibility.ilztextci,
    COMMUTATOR = OPERATOR(eligibility.>),
    NEGATOR = OPERATOR(eligibility.>),
    MERGES,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel
);


--
-- Name: <; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.< (
    FUNCTION = eligibility.ilztext_rlt,
    LEFTARG = text,
    RIGHTARG = eligibility.ilztextci,
    COMMUTATOR = OPERATOR(eligibility.>),
    NEGATOR = OPERATOR(eligibility.>),
    MERGES,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel
);


--
-- Name: <; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.< (
    FUNCTION = eligibility.ilztext_llt,
    LEFTARG = eligibility.ilztextci,
    RIGHTARG = text,
    COMMUTATOR = OPERATOR(eligibility.>),
    NEGATOR = OPERATOR(eligibility.>),
    MERGES,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel
);


--
-- Name: <; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.< (
    FUNCTION = eligibility.iwstext_blt,
    LEFTARG = eligibility.iwstext,
    RIGHTARG = eligibility.iwstext,
    COMMUTATOR = OPERATOR(eligibility.<),
    NEGATOR = OPERATOR(eligibility.>),
    MERGES,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel
);


--
-- Name: <; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.< (
    FUNCTION = eligibility.iwstext_llt,
    LEFTARG = eligibility.iwstext,
    RIGHTARG = text,
    COMMUTATOR = OPERATOR(eligibility.<),
    NEGATOR = OPERATOR(eligibility.>),
    MERGES,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel
);


--
-- Name: <; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.< (
    FUNCTION = eligibility.iwstext_rlt,
    LEFTARG = text,
    RIGHTARG = eligibility.iwstext,
    COMMUTATOR = OPERATOR(eligibility.<),
    NEGATOR = OPERATOR(eligibility.>),
    MERGES,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel
);


--
-- Name: <; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.< (
    FUNCTION = eligibility.ilztext_blt,
    LEFTARG = eligibility.ilztext,
    RIGHTARG = eligibility.ilztext,
    COMMUTATOR = OPERATOR(eligibility.<),
    NEGATOR = OPERATOR(eligibility.>),
    MERGES,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel
);


--
-- Name: <; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.< (
    FUNCTION = eligibility.ilztext_llt,
    LEFTARG = eligibility.ilztext,
    RIGHTARG = text,
    COMMUTATOR = OPERATOR(eligibility.<),
    NEGATOR = OPERATOR(eligibility.>),
    MERGES,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel
);


--
-- Name: <; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.< (
    FUNCTION = eligibility.ilztext_rlt,
    LEFTARG = text,
    RIGHTARG = eligibility.ilztext,
    COMMUTATOR = OPERATOR(eligibility.<),
    NEGATOR = OPERATOR(eligibility.>),
    MERGES,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel
);


--
-- Name: <; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.< (
    FUNCTION = eligibility.citext_blt,
    LEFTARG = eligibility.citext,
    RIGHTARG = eligibility.citext,
    COMMUTATOR = OPERATOR(eligibility.<),
    NEGATOR = OPERATOR(eligibility.>),
    MERGES,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel
);


--
-- Name: <; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.< (
    FUNCTION = eligibility.citext_llt,
    LEFTARG = eligibility.citext,
    RIGHTARG = text,
    COMMUTATOR = OPERATOR(eligibility.<),
    NEGATOR = OPERATOR(eligibility.>),
    MERGES,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel
);


--
-- Name: <; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.< (
    FUNCTION = eligibility.citext_rlt,
    LEFTARG = text,
    RIGHTARG = eligibility.citext,
    COMMUTATOR = OPERATOR(eligibility.<),
    NEGATOR = OPERATOR(eligibility.>),
    MERGES,
    RESTRICT = scalarltsel,
    JOIN = scalarltjoinsel
);


--
-- Name: <=; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.<= (
    FUNCTION = eligibility.ilztext_ble,
    LEFTARG = eligibility.ilztextci,
    RIGHTARG = eligibility.ilztextci,
    COMMUTATOR = OPERATOR(eligibility.>=),
    NEGATOR = OPERATOR(eligibility.>=),
    MERGES,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel
);


--
-- Name: <=; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.<= (
    FUNCTION = eligibility.ilztext_rle,
    LEFTARG = text,
    RIGHTARG = eligibility.ilztextci,
    COMMUTATOR = OPERATOR(eligibility.>=),
    NEGATOR = OPERATOR(eligibility.>=),
    MERGES,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel
);


--
-- Name: <=; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.<= (
    FUNCTION = eligibility.ilztext_lle,
    LEFTARG = eligibility.ilztextci,
    RIGHTARG = text,
    COMMUTATOR = OPERATOR(eligibility.>=),
    NEGATOR = OPERATOR(eligibility.>=),
    MERGES,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel
);


--
-- Name: <=; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.<= (
    FUNCTION = eligibility.iwstext_ble,
    LEFTARG = eligibility.iwstext,
    RIGHTARG = eligibility.iwstext,
    COMMUTATOR = OPERATOR(eligibility.<=),
    NEGATOR = OPERATOR(eligibility.>=),
    MERGES,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel
);


--
-- Name: <=; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.<= (
    FUNCTION = eligibility.iwstext_lle,
    LEFTARG = eligibility.iwstext,
    RIGHTARG = text,
    COMMUTATOR = OPERATOR(eligibility.<=),
    NEGATOR = OPERATOR(eligibility.>=),
    MERGES,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel
);


--
-- Name: <=; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.<= (
    FUNCTION = eligibility.iwstext_rle,
    LEFTARG = text,
    RIGHTARG = eligibility.iwstext,
    COMMUTATOR = OPERATOR(eligibility.<=),
    NEGATOR = OPERATOR(eligibility.>=),
    MERGES,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel
);


--
-- Name: <=; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.<= (
    FUNCTION = eligibility.ilztext_ble,
    LEFTARG = eligibility.ilztext,
    RIGHTARG = eligibility.ilztext,
    COMMUTATOR = OPERATOR(eligibility.<=),
    NEGATOR = OPERATOR(eligibility.>=),
    MERGES,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel
);


--
-- Name: <=; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.<= (
    FUNCTION = eligibility.ilztext_lle,
    LEFTARG = eligibility.ilztext,
    RIGHTARG = text,
    COMMUTATOR = OPERATOR(eligibility.<=),
    NEGATOR = OPERATOR(eligibility.>=),
    MERGES,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel
);


--
-- Name: <=; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.<= (
    FUNCTION = eligibility.ilztext_rle,
    LEFTARG = text,
    RIGHTARG = eligibility.ilztext,
    COMMUTATOR = OPERATOR(eligibility.<=),
    NEGATOR = OPERATOR(eligibility.>=),
    MERGES,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel
);


--
-- Name: <=; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.<= (
    FUNCTION = eligibility.citext_ble,
    LEFTARG = eligibility.citext,
    RIGHTARG = eligibility.citext,
    COMMUTATOR = OPERATOR(eligibility.<=),
    NEGATOR = OPERATOR(eligibility.>=),
    MERGES,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel
);


--
-- Name: <=; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.<= (
    FUNCTION = eligibility.citext_lle,
    LEFTARG = eligibility.citext,
    RIGHTARG = text,
    COMMUTATOR = OPERATOR(eligibility.<=),
    NEGATOR = OPERATOR(eligibility.>=),
    MERGES,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel
);


--
-- Name: <=; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.<= (
    FUNCTION = eligibility.citext_rle,
    LEFTARG = text,
    RIGHTARG = eligibility.citext,
    COMMUTATOR = OPERATOR(eligibility.<=),
    NEGATOR = OPERATOR(eligibility.>=),
    MERGES,
    RESTRICT = scalarlesel,
    JOIN = scalarlejoinsel
);


--
-- Name: <>; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.<> (
    FUNCTION = eligibility.ilztext_rne,
    LEFTARG = text,
    RIGHTARG = eligibility.ilztextci,
    COMMUTATOR = OPERATOR(eligibility.<>),
    NEGATOR = OPERATOR(eligibility.=),
    MERGES,
    RESTRICT = neqsel,
    JOIN = neqjoinsel
);


--
-- Name: <>; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.<> (
    FUNCTION = eligibility.ilztext_lne,
    LEFTARG = eligibility.ilztextci,
    RIGHTARG = text,
    COMMUTATOR = OPERATOR(eligibility.<>),
    NEGATOR = OPERATOR(eligibility.=),
    MERGES,
    RESTRICT = neqsel,
    JOIN = neqjoinsel
);


--
-- Name: <>; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.<> (
    FUNCTION = eligibility.ilztext_bne,
    LEFTARG = eligibility.ilztextci,
    RIGHTARG = eligibility.ilztextci,
    COMMUTATOR = OPERATOR(eligibility.<>),
    NEGATOR = OPERATOR(eligibility.=),
    MERGES,
    RESTRICT = neqsel,
    JOIN = neqjoinsel
);


--
-- Name: <>; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.<> (
    FUNCTION = eligibility.iwstext_rne,
    LEFTARG = text,
    RIGHTARG = eligibility.iwstext,
    COMMUTATOR = OPERATOR(eligibility.<>),
    NEGATOR = OPERATOR(eligibility.=),
    MERGES,
    RESTRICT = neqsel,
    JOIN = neqjoinsel
);


--
-- Name: <>; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.<> (
    FUNCTION = eligibility.iwstext_lne,
    LEFTARG = eligibility.iwstext,
    RIGHTARG = text,
    COMMUTATOR = OPERATOR(eligibility.<>),
    NEGATOR = OPERATOR(eligibility.=),
    MERGES,
    RESTRICT = neqsel,
    JOIN = neqjoinsel
);


--
-- Name: <>; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.<> (
    FUNCTION = eligibility.iwstext_bne,
    LEFTARG = eligibility.iwstext,
    RIGHTARG = eligibility.iwstext,
    COMMUTATOR = OPERATOR(eligibility.<>),
    NEGATOR = OPERATOR(eligibility.=),
    MERGES,
    RESTRICT = neqsel,
    JOIN = neqjoinsel
);


--
-- Name: <>; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.<> (
    FUNCTION = eligibility.ilztext_rne,
    LEFTARG = text,
    RIGHTARG = eligibility.ilztext,
    COMMUTATOR = OPERATOR(eligibility.<>),
    NEGATOR = OPERATOR(eligibility.=),
    MERGES,
    RESTRICT = neqsel,
    JOIN = neqjoinsel
);


--
-- Name: <>; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.<> (
    FUNCTION = eligibility.ilztext_lne,
    LEFTARG = eligibility.ilztext,
    RIGHTARG = text,
    COMMUTATOR = OPERATOR(eligibility.<>),
    NEGATOR = OPERATOR(eligibility.=),
    MERGES,
    RESTRICT = neqsel,
    JOIN = neqjoinsel
);


--
-- Name: <>; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.<> (
    FUNCTION = eligibility.ilztext_bne,
    LEFTARG = eligibility.ilztext,
    RIGHTARG = eligibility.ilztext,
    COMMUTATOR = OPERATOR(eligibility.<>),
    NEGATOR = OPERATOR(eligibility.=),
    MERGES,
    RESTRICT = neqsel,
    JOIN = neqjoinsel
);


--
-- Name: <>; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.<> (
    FUNCTION = eligibility.citext_rne,
    LEFTARG = text,
    RIGHTARG = eligibility.citext,
    COMMUTATOR = OPERATOR(eligibility.<>),
    NEGATOR = OPERATOR(eligibility.=),
    MERGES,
    RESTRICT = neqsel,
    JOIN = neqjoinsel
);


--
-- Name: <>; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.<> (
    FUNCTION = eligibility.citext_lne,
    LEFTARG = eligibility.citext,
    RIGHTARG = text,
    COMMUTATOR = OPERATOR(eligibility.<>),
    NEGATOR = OPERATOR(eligibility.=),
    MERGES,
    RESTRICT = neqsel,
    JOIN = neqjoinsel
);


--
-- Name: <>; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.<> (
    FUNCTION = eligibility.citext_bne,
    LEFTARG = eligibility.citext,
    RIGHTARG = eligibility.citext,
    COMMUTATOR = OPERATOR(eligibility.<>),
    NEGATOR = OPERATOR(eligibility.=),
    MERGES,
    RESTRICT = neqsel,
    JOIN = neqjoinsel
);


--
-- Name: =; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.= (
    FUNCTION = eligibility.ilztext_leq,
    LEFTARG = eligibility.ilztextci,
    RIGHTARG = text,
    COMMUTATOR = OPERATOR(eligibility.=),
    NEGATOR = OPERATOR(eligibility.<>),
    MERGES,
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);


--
-- Name: =; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.= (
    FUNCTION = eligibility.ilztext_req,
    LEFTARG = text,
    RIGHTARG = eligibility.ilztextci,
    COMMUTATOR = OPERATOR(eligibility.=),
    NEGATOR = OPERATOR(eligibility.<>),
    MERGES,
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);


--
-- Name: =; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.= (
    FUNCTION = eligibility.ilztext_beq,
    LEFTARG = eligibility.ilztextci,
    RIGHTARG = eligibility.ilztextci,
    COMMUTATOR = OPERATOR(eligibility.=),
    NEGATOR = OPERATOR(eligibility.<>),
    MERGES,
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);


--
-- Name: =; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.= (
    FUNCTION = eligibility.iwstext_leq,
    LEFTARG = eligibility.iwstext,
    RIGHTARG = text,
    COMMUTATOR = OPERATOR(eligibility.=),
    NEGATOR = OPERATOR(eligibility.<>),
    MERGES,
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);


--
-- Name: =; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.= (
    FUNCTION = eligibility.iwstext_req,
    LEFTARG = text,
    RIGHTARG = eligibility.iwstext,
    COMMUTATOR = OPERATOR(eligibility.=),
    NEGATOR = OPERATOR(eligibility.<>),
    MERGES,
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);


--
-- Name: =; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.= (
    FUNCTION = eligibility.iwstext_beq,
    LEFTARG = eligibility.iwstext,
    RIGHTARG = eligibility.iwstext,
    COMMUTATOR = OPERATOR(eligibility.=),
    NEGATOR = OPERATOR(eligibility.<>),
    MERGES,
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);


--
-- Name: =; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.= (
    FUNCTION = eligibility.ilztext_leq,
    LEFTARG = eligibility.ilztext,
    RIGHTARG = text,
    COMMUTATOR = OPERATOR(eligibility.=),
    NEGATOR = OPERATOR(eligibility.<>),
    MERGES,
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);


--
-- Name: =; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.= (
    FUNCTION = eligibility.ilztext_req,
    LEFTARG = text,
    RIGHTARG = eligibility.ilztext,
    COMMUTATOR = OPERATOR(eligibility.=),
    NEGATOR = OPERATOR(eligibility.<>),
    MERGES,
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);


--
-- Name: =; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.= (
    FUNCTION = eligibility.ilztext_beq,
    LEFTARG = eligibility.ilztext,
    RIGHTARG = eligibility.ilztext,
    COMMUTATOR = OPERATOR(eligibility.=),
    NEGATOR = OPERATOR(eligibility.<>),
    MERGES,
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);


--
-- Name: =; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.= (
    FUNCTION = eligibility.citext_leq,
    LEFTARG = eligibility.citext,
    RIGHTARG = text,
    COMMUTATOR = OPERATOR(eligibility.=),
    NEGATOR = OPERATOR(eligibility.<>),
    MERGES,
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);


--
-- Name: =; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.= (
    FUNCTION = eligibility.citext_req,
    LEFTARG = text,
    RIGHTARG = eligibility.citext,
    COMMUTATOR = OPERATOR(eligibility.=),
    NEGATOR = OPERATOR(eligibility.<>),
    MERGES,
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);


--
-- Name: =; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.= (
    FUNCTION = eligibility.citext_beq,
    LEFTARG = eligibility.citext,
    RIGHTARG = eligibility.citext,
    COMMUTATOR = OPERATOR(eligibility.=),
    NEGATOR = OPERATOR(eligibility.<>),
    MERGES,
    RESTRICT = eqsel,
    JOIN = eqjoinsel
);


--
-- Name: >; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.> (
    FUNCTION = eligibility.ilztext_bgt,
    LEFTARG = eligibility.ilztextci,
    RIGHTARG = eligibility.ilztextci,
    COMMUTATOR = OPERATOR(eligibility.<),
    NEGATOR = OPERATOR(eligibility.<),
    MERGES,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);


--
-- Name: >; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.> (
    FUNCTION = eligibility.ilztext_lgt,
    LEFTARG = eligibility.ilztextci,
    RIGHTARG = text,
    COMMUTATOR = OPERATOR(eligibility.>),
    NEGATOR = OPERATOR(eligibility.>),
    MERGES,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);


--
-- Name: >; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.> (
    FUNCTION = eligibility.ilztext_rgt,
    LEFTARG = text,
    RIGHTARG = eligibility.ilztextci,
    COMMUTATOR = OPERATOR(eligibility.<),
    NEGATOR = OPERATOR(eligibility.<),
    MERGES,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);


--
-- Name: >; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.> (
    FUNCTION = eligibility.iwstext_bgt,
    LEFTARG = eligibility.iwstext,
    RIGHTARG = eligibility.iwstext,
    COMMUTATOR = OPERATOR(eligibility.>),
    NEGATOR = OPERATOR(eligibility.<),
    MERGES,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);


--
-- Name: >; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.> (
    FUNCTION = eligibility.iwstext_rgt,
    LEFTARG = text,
    RIGHTARG = eligibility.iwstext,
    COMMUTATOR = OPERATOR(eligibility.>),
    NEGATOR = OPERATOR(eligibility.<),
    MERGES,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);


--
-- Name: >; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.> (
    FUNCTION = eligibility.iwstext_lgt,
    LEFTARG = eligibility.iwstext,
    RIGHTARG = text,
    COMMUTATOR = OPERATOR(eligibility.<),
    NEGATOR = OPERATOR(eligibility.>),
    MERGES,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);


--
-- Name: >; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.> (
    FUNCTION = eligibility.ilztext_bgt,
    LEFTARG = eligibility.ilztext,
    RIGHTARG = eligibility.ilztext,
    COMMUTATOR = OPERATOR(eligibility.>),
    NEGATOR = OPERATOR(eligibility.<),
    MERGES,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);


--
-- Name: >; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.> (
    FUNCTION = eligibility.ilztext_rgt,
    LEFTARG = text,
    RIGHTARG = eligibility.ilztext,
    COMMUTATOR = OPERATOR(eligibility.>),
    NEGATOR = OPERATOR(eligibility.<),
    MERGES,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);


--
-- Name: >; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.> (
    FUNCTION = eligibility.ilztext_lgt,
    LEFTARG = eligibility.ilztext,
    RIGHTARG = text,
    COMMUTATOR = OPERATOR(eligibility.<),
    NEGATOR = OPERATOR(eligibility.>),
    MERGES,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);


--
-- Name: >; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.> (
    FUNCTION = eligibility.citext_bgt,
    LEFTARG = eligibility.citext,
    RIGHTARG = eligibility.citext,
    COMMUTATOR = OPERATOR(eligibility.>),
    NEGATOR = OPERATOR(eligibility.<),
    MERGES,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);


--
-- Name: >; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.> (
    FUNCTION = eligibility.citext_rgt,
    LEFTARG = text,
    RIGHTARG = eligibility.citext,
    COMMUTATOR = OPERATOR(eligibility.>),
    NEGATOR = OPERATOR(eligibility.<),
    MERGES,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);


--
-- Name: >; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.> (
    FUNCTION = eligibility.citext_lgt,
    LEFTARG = eligibility.citext,
    RIGHTARG = text,
    COMMUTATOR = OPERATOR(eligibility.<),
    NEGATOR = OPERATOR(eligibility.>),
    MERGES,
    RESTRICT = scalargtsel,
    JOIN = scalargtjoinsel
);


--
-- Name: >=; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.>= (
    FUNCTION = eligibility.ilztext_bge,
    LEFTARG = eligibility.ilztextci,
    RIGHTARG = eligibility.ilztextci,
    COMMUTATOR = OPERATOR(eligibility.<=),
    NEGATOR = OPERATOR(eligibility.<=),
    MERGES,
    RESTRICT = scalargesel,
    JOIN = scalargejoinsel
);


--
-- Name: >=; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.>= (
    FUNCTION = eligibility.ilztext_lge,
    LEFTARG = eligibility.ilztextci,
    RIGHTARG = text,
    COMMUTATOR = OPERATOR(eligibility.<=),
    NEGATOR = OPERATOR(eligibility.<=),
    MERGES,
    RESTRICT = scalargesel,
    JOIN = scalargejoinsel
);


--
-- Name: >=; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.>= (
    FUNCTION = eligibility.ilztext_rge,
    LEFTARG = text,
    RIGHTARG = eligibility.ilztextci,
    COMMUTATOR = OPERATOR(eligibility.<=),
    NEGATOR = OPERATOR(eligibility.<=),
    MERGES,
    RESTRICT = scalargesel,
    JOIN = scalargejoinsel
);


--
-- Name: >=; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.>= (
    FUNCTION = eligibility.iwstext_bge,
    LEFTARG = eligibility.iwstext,
    RIGHTARG = eligibility.iwstext,
    COMMUTATOR = OPERATOR(eligibility.>=),
    NEGATOR = OPERATOR(eligibility.<=),
    MERGES,
    RESTRICT = scalargesel,
    JOIN = scalargejoinsel
);


--
-- Name: >=; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.>= (
    FUNCTION = eligibility.iwstext_rge,
    LEFTARG = text,
    RIGHTARG = eligibility.iwstext,
    COMMUTATOR = OPERATOR(eligibility.<=),
    NEGATOR = OPERATOR(eligibility.<=),
    MERGES,
    RESTRICT = scalargesel,
    JOIN = scalargejoinsel
);


--
-- Name: >=; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.>= (
    FUNCTION = eligibility.iwstext_lge,
    LEFTARG = eligibility.iwstext,
    RIGHTARG = text,
    COMMUTATOR = OPERATOR(eligibility.>=),
    NEGATOR = OPERATOR(eligibility.<=),
    MERGES,
    RESTRICT = scalargesel,
    JOIN = scalargejoinsel
);


--
-- Name: >=; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.>= (
    FUNCTION = eligibility.ilztext_bge,
    LEFTARG = eligibility.ilztext,
    RIGHTARG = eligibility.ilztext,
    COMMUTATOR = OPERATOR(eligibility.>=),
    NEGATOR = OPERATOR(eligibility.<=),
    MERGES,
    RESTRICT = scalargesel,
    JOIN = scalargejoinsel
);


--
-- Name: >=; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.>= (
    FUNCTION = eligibility.ilztext_rge,
    LEFTARG = text,
    RIGHTARG = eligibility.ilztext,
    COMMUTATOR = OPERATOR(eligibility.<=),
    NEGATOR = OPERATOR(eligibility.<=),
    MERGES,
    RESTRICT = scalargesel,
    JOIN = scalargejoinsel
);


--
-- Name: >=; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.>= (
    FUNCTION = eligibility.ilztext_lge,
    LEFTARG = eligibility.ilztext,
    RIGHTARG = text,
    COMMUTATOR = OPERATOR(eligibility.>=),
    NEGATOR = OPERATOR(eligibility.<=),
    MERGES,
    RESTRICT = scalargesel,
    JOIN = scalargejoinsel
);


--
-- Name: >=; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.>= (
    FUNCTION = eligibility.citext_bge,
    LEFTARG = eligibility.citext,
    RIGHTARG = eligibility.citext,
    COMMUTATOR = OPERATOR(eligibility.>=),
    NEGATOR = OPERATOR(eligibility.<=),
    MERGES,
    RESTRICT = scalargesel,
    JOIN = scalargejoinsel
);


--
-- Name: >=; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.>= (
    FUNCTION = eligibility.citext_rge,
    LEFTARG = text,
    RIGHTARG = eligibility.citext,
    COMMUTATOR = OPERATOR(eligibility.<=),
    NEGATOR = OPERATOR(eligibility.<=),
    MERGES,
    RESTRICT = scalargesel,
    JOIN = scalargejoinsel
);


--
-- Name: >=; Type: OPERATOR; Schema: eligibility; Owner: -
--

CREATE OPERATOR eligibility.>= (
    FUNCTION = eligibility.citext_lge,
    LEFTARG = eligibility.citext,
    RIGHTARG = text,
    COMMUTATOR = OPERATOR(eligibility.>=),
    NEGATOR = OPERATOR(eligibility.<=),
    MERGES,
    RESTRICT = scalargesel,
    JOIN = scalargejoinsel
);


--
-- Name: configuration; Type: TABLE; Schema: eligibility; Owner: -
--

CREATE TABLE eligibility.configuration (
    organization_id bigint NOT NULL,
    directory_name text,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    email_domains text[] DEFAULT '{}'::text[] NOT NULL,
    implementation eligibility.client_specific_implementation,
    data_provider boolean DEFAULT false NOT NULL,
    activated_at date,
    terminated_at date,
    medical_plan_only boolean DEFAULT false,
    employee_only boolean DEFAULT false,
    eligibility_type character varying,
    ingestion_config jsonb
);


--
-- Name: file; Type: TABLE; Schema: eligibility; Owner: -
--

CREATE TABLE eligibility.file (
    id bigint NOT NULL,
    organization_id bigint NOT NULL,
    name text NOT NULL,
    encoding text NOT NULL,
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    error eligibility.file_error,
    success_count integer DEFAULT 0,
    failure_count integer DEFAULT 0,
    raw_count integer DEFAULT 0
);


--
-- Name: file_id_seq; Type: SEQUENCE; Schema: eligibility; Owner: -
--

CREATE SEQUENCE eligibility.file_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: file_id_seq; Type: SEQUENCE OWNED BY; Schema: eligibility; Owner: -
--

ALTER SEQUENCE eligibility.file_id_seq OWNED BY eligibility.file.id;


--
-- Name: file_parse_errors; Type: TABLE; Schema: eligibility; Owner: -
--

CREATE TABLE eligibility.file_parse_errors (
    id bigint NOT NULL,
    file_id bigint,
    organization_id bigint NOT NULL,
    record jsonb NOT NULL,
    errors text[] DEFAULT '{}'::text[] NOT NULL,
    warnings text[] DEFAULT '{}'::text[] NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: file_parse_errors_id_seq; Type: SEQUENCE; Schema: eligibility; Owner: -
--

CREATE SEQUENCE eligibility.file_parse_errors_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: file_parse_errors_id_seq; Type: SEQUENCE OWNED BY; Schema: eligibility; Owner: -
--

ALTER SEQUENCE eligibility.file_parse_errors_id_seq OWNED BY eligibility.file_parse_errors.id;


--
-- Name: file_parse_results_id_seq; Type: SEQUENCE; Schema: eligibility; Owner: -
--

CREATE SEQUENCE eligibility.file_parse_results_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: file_parse_results_id_seq; Type: SEQUENCE OWNED BY; Schema: eligibility; Owner: -
--

ALTER SEQUENCE eligibility.file_parse_results_id_seq OWNED BY eligibility.file_parse_results.id;


--
-- Name: header_alias; Type: TABLE; Schema: eligibility; Owner: -
--

CREATE TABLE eligibility.header_alias (
    organization_id bigint NOT NULL,
    header text NOT NULL COLLATE eligibility.ci,
    alias text NOT NULL COLLATE eligibility.ci,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    id bigint NOT NULL,
    is_eligibility_field boolean DEFAULT false NOT NULL
);


--
-- Name: header_alias_id_seq; Type: SEQUENCE; Schema: eligibility; Owner: -
--

CREATE SEQUENCE eligibility.header_alias_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: header_alias_id_seq; Type: SEQUENCE OWNED BY; Schema: eligibility; Owner: -
--

ALTER SEQUENCE eligibility.header_alias_id_seq OWNED BY eligibility.header_alias.id;


--
-- Name: incomplete_files_by_org; Type: VIEW; Schema: eligibility; Owner: -
--

CREATE VIEW eligibility.incomplete_files_by_org AS
SELECT
    NULL::bigint AS id,
    NULL::bigint AS total_members,
    NULL::jsonb AS config,
    NULL::jsonb AS incomplete;


--
-- Name: member_2; Type: TABLE; Schema: eligibility; Owner: -
--

CREATE TABLE eligibility.member_2 (
    id bigint NOT NULL,
    version bigint NOT NULL,
    organization_id bigint NOT NULL,
    first_name eligibility.iwstext DEFAULT ''::text NOT NULL,
    last_name eligibility.iwstext DEFAULT ''::text NOT NULL,
    email eligibility.iwstext DEFAULT ''::text NOT NULL,
    unique_corp_id eligibility.ilztext DEFAULT ''::text NOT NULL,
    dependent_id eligibility.citext DEFAULT ''::text NOT NULL,
    date_of_birth date NOT NULL,
    work_state eligibility.iwstext,
    work_country eligibility.citext DEFAULT NULL::text,
    record jsonb,
    custom_attributes jsonb,
    effective_range daterange DEFAULT eligibility.default_range() NOT NULL,
    do_not_contact eligibility.iwstext,
    gender_code eligibility.iwstext,
    employer_assigned_id eligibility.iwstext,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: member_address; Type: TABLE; Schema: eligibility; Owner: -
--

CREATE TABLE eligibility.member_address (
    id bigint NOT NULL,
    member_id bigint NOT NULL,
    address_1 text,
    address_2 text,
    city text,
    state text,
    postal_code text,
    postal_code_suffix text,
    country_code text,
    address_type text,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: member_address_id_seq; Type: SEQUENCE; Schema: eligibility; Owner: -
--

CREATE SEQUENCE eligibility.member_address_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: member_address_id_seq; Type: SEQUENCE OWNED BY; Schema: eligibility; Owner: -
--

ALTER SEQUENCE eligibility.member_address_id_seq OWNED BY eligibility.member_address.id;


--
-- Name: member_address_versioned; Type: TABLE; Schema: eligibility; Owner: -
--

CREATE TABLE eligibility.member_address_versioned (
    id bigint NOT NULL,
    member_id bigint NOT NULL,
    address_1 text,
    address_2 text,
    city text,
    state text,
    postal_code text,
    postal_code_suffix text,
    country_code text,
    address_type text,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: member_address_versioned_historical; Type: TABLE; Schema: eligibility; Owner: -
--

CREATE TABLE eligibility.member_address_versioned_historical (
    id bigint NOT NULL,
    member_id bigint NOT NULL,
    address_1 text,
    address_2 text,
    city text,
    state text,
    postal_code text,
    postal_code_suffix text,
    country_code text,
    address_type text,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: member_address_versioned_historical_id_seq; Type: SEQUENCE; Schema: eligibility; Owner: -
--

CREATE SEQUENCE eligibility.member_address_versioned_historical_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: member_address_versioned_historical_id_seq; Type: SEQUENCE OWNED BY; Schema: eligibility; Owner: -
--

ALTER SEQUENCE eligibility.member_address_versioned_historical_id_seq OWNED BY eligibility.member_address_versioned_historical.id;


--
-- Name: member_address_versioned_id_seq; Type: SEQUENCE; Schema: eligibility; Owner: -
--

CREATE SEQUENCE eligibility.member_address_versioned_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: member_address_versioned_id_seq; Type: SEQUENCE OWNED BY; Schema: eligibility; Owner: -
--

ALTER SEQUENCE eligibility.member_address_versioned_id_seq OWNED BY eligibility.member_address_versioned.id;


--
-- Name: member_custom_attributes; Type: TABLE; Schema: eligibility; Owner: -
--

CREATE TABLE eligibility.member_custom_attributes (
    id bigint NOT NULL,
    member_id bigint NOT NULL,
    attribute_name eligibility.citext DEFAULT ''::text NOT NULL,
    attribute_value eligibility.citext DEFAULT NULL::text,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: member_custom_attributes_id_seq; Type: SEQUENCE; Schema: eligibility; Owner: -
--

CREATE SEQUENCE eligibility.member_custom_attributes_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: member_custom_attributes_id_seq; Type: SEQUENCE OWNED BY; Schema: eligibility; Owner: -
--

ALTER SEQUENCE eligibility.member_custom_attributes_id_seq OWNED BY eligibility.member_custom_attributes.id;


--
-- Name: member_detail_view; Type: VIEW; Schema: eligibility; Owner: -
--

CREATE VIEW eligibility.member_detail_view AS
 SELECT m.id,
    m.organization_id,
    m.first_name,
    m.last_name,
    m.date_of_birth,
    m.work_state,
    m.email,
    m.unique_corp_id,
    m.employer_assigned_id,
    m.dependent_id,
    m.effective_range,
    m.record,
    m.file_id,
    m.do_not_contact,
    m.gender_code,
    m.created_at,
    m.updated_at,
    ma.city,
    ma.country_code,
    ma.postal_code,
        CASE
            WHEN (((lower(m.effective_range) IS NOT NULL) AND (lower(m.effective_range) <= CURRENT_DATE) AND ((upper(m.effective_range) IS NULL) OR (upper(m.effective_range) > CURRENT_DATE))) = true) THEN 'TRUE'::text
            ELSE 'FALSE'::text
        END AS active
   FROM eligibility.member m,
    eligibility.member_address ma
  WHERE (m.id = ma.member_id);


--
-- Name: member_id_seq; Type: SEQUENCE; Schema: eligibility; Owner: -
--

CREATE SEQUENCE eligibility.member_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: member_id_seq; Type: SEQUENCE OWNED BY; Schema: eligibility; Owner: -
--

ALTER SEQUENCE eligibility.member_id_seq OWNED BY eligibility.member.id;


--
-- Name: member_sub_population; Type: TABLE; Schema: eligibility; Owner: -
--

CREATE TABLE eligibility.member_sub_population (
    member_id bigint NOT NULL,
    sub_population_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: member_verification; Type: TABLE; Schema: eligibility; Owner: -
--

CREATE TABLE eligibility.member_verification (
    id bigint NOT NULL,
    member_id bigint,
    verification_id bigint,
    verification_attempt_id bigint,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: member_verification_id_seq; Type: SEQUENCE; Schema: eligibility; Owner: -
--

CREATE SEQUENCE eligibility.member_verification_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: member_verification_id_seq; Type: SEQUENCE OWNED BY; Schema: eligibility; Owner: -
--

ALTER SEQUENCE eligibility.member_verification_id_seq OWNED BY eligibility.member_verification.id;


--
-- Name: member_versioned_historical; Type: TABLE; Schema: eligibility; Owner: -
--

CREATE TABLE eligibility.member_versioned_historical (
    id bigint NOT NULL,
    organization_id bigint NOT NULL,
    file_id bigint,
    first_name eligibility.iwstext DEFAULT ''::text NOT NULL,
    last_name eligibility.iwstext DEFAULT ''::text NOT NULL,
    email eligibility.iwstext DEFAULT ''::text NOT NULL,
    unique_corp_id eligibility.ilztext DEFAULT ''::text NOT NULL,
    dependent_id eligibility.citext DEFAULT ''::text NOT NULL,
    date_of_birth date NOT NULL,
    work_state eligibility.iwstext,
    record jsonb,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    effective_range daterange DEFAULT eligibility.default_range() NOT NULL,
    policies jsonb,
    do_not_contact eligibility.iwstext,
    gender_code eligibility.iwstext,
    employer_assigned_id eligibility.iwstext,
    work_country eligibility.citext DEFAULT NULL::text,
    custom_attributes jsonb,
    pre_verified boolean DEFAULT false NOT NULL,
    hash_value eligibility.iwstext,
    hash_version integer
);


--
-- Name: member_versioned_historical_id_seq; Type: SEQUENCE; Schema: eligibility; Owner: -
--

CREATE SEQUENCE eligibility.member_versioned_historical_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: member_versioned_historical_id_seq; Type: SEQUENCE OWNED BY; Schema: eligibility; Owner: -
--

ALTER SEQUENCE eligibility.member_versioned_historical_id_seq OWNED BY eligibility.member_versioned_historical.id;


--
-- Name: member_versioned_id_seq; Type: SEQUENCE; Schema: eligibility; Owner: -
--

CREATE SEQUENCE eligibility.member_versioned_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: member_versioned_id_seq; Type: SEQUENCE OWNED BY; Schema: eligibility; Owner: -
--

ALTER SEQUENCE eligibility.member_versioned_id_seq OWNED BY eligibility.member_versioned.id;


--
-- Name: organization_external_id; Type: TABLE; Schema: eligibility; Owner: -
--

CREATE TABLE eligibility.organization_external_id (
    id bigint NOT NULL,
    source eligibility.citext,
    external_id eligibility.citext NOT NULL,
    organization_id bigint NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    data_provider_organization_id bigint
);


--
-- Name: organization_external_id_id_seq; Type: SEQUENCE; Schema: eligibility; Owner: -
--

CREATE SEQUENCE eligibility.organization_external_id_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: organization_external_id_id_seq; Type: SEQUENCE OWNED BY; Schema: eligibility; Owner: -
--

ALTER SEQUENCE eligibility.organization_external_id_id_seq OWNED BY eligibility.organization_external_id.id;


--
-- Name: population; Type: TABLE; Schema: eligibility; Owner: -
--

CREATE TABLE eligibility.population (
    id bigint NOT NULL,
    organization_id bigint NOT NULL,
    activated_at timestamp with time zone,
    deactivated_at timestamp with time zone,
    sub_pop_lookup_keys_csv text NOT NULL,
    sub_pop_lookup_map_json jsonb NOT NULL,
    advanced boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: population_id_seq; Type: SEQUENCE; Schema: eligibility; Owner: -
--

CREATE SEQUENCE eligibility.population_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: population_id_seq; Type: SEQUENCE OWNED BY; Schema: eligibility; Owner: -
--

ALTER SEQUENCE eligibility.population_id_seq OWNED BY eligibility.population.id;


--
-- Name: sub_population; Type: TABLE; Schema: eligibility; Owner: -
--

CREATE TABLE eligibility.sub_population (
    id bigint NOT NULL,
    population_id bigint NOT NULL,
    feature_set_name text NOT NULL,
    feature_set_details_json jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: sub_population_id_seq; Type: SEQUENCE; Schema: eligibility; Owner: -
--

CREATE SEQUENCE eligibility.sub_population_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: sub_population_id_seq; Type: SEQUENCE OWNED BY; Schema: eligibility; Owner: -
--

ALTER SEQUENCE eligibility.sub_population_id_seq OWNED BY eligibility.sub_population.id;


--
-- Name: tmp_file_parse_results; Type: TABLE; Schema: eligibility; Owner: -
--

CREATE TABLE eligibility.tmp_file_parse_results (
    id bigint DEFAULT nextval('eligibility.file_parse_results_id_seq'::regclass) NOT NULL,
    organization_id bigint NOT NULL,
    file_id bigint,
    first_name eligibility.iwstext DEFAULT ''::text NOT NULL,
    last_name eligibility.iwstext DEFAULT ''::text NOT NULL,
    email eligibility.iwstext DEFAULT ''::text NOT NULL,
    unique_corp_id eligibility.ilztext DEFAULT ''::text NOT NULL,
    dependent_id eligibility.citext DEFAULT ''::text NOT NULL,
    date_of_birth date NOT NULL,
    work_state eligibility.iwstext,
    record jsonb,
    errors text[] DEFAULT '{}'::text[] NOT NULL,
    warnings text[] DEFAULT '{}'::text[] NOT NULL,
    effective_range daterange DEFAULT eligibility.default_range(),
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    do_not_contact eligibility.iwstext,
    gender_code eligibility.iwstext,
    employer_assigned_id eligibility.iwstext
);


--
-- Name: verification; Type: TABLE; Schema: eligibility; Owner: -
--

CREATE TABLE eligibility.verification (
    id bigint NOT NULL,
    user_id bigint NOT NULL,
    organization_id bigint NOT NULL,
    unique_corp_id eligibility.ilztext DEFAULT ''::text NOT NULL,
    dependent_id eligibility.citext DEFAULT ''::text NOT NULL,
    first_name eligibility.iwstext DEFAULT ''::text NOT NULL,
    last_name eligibility.iwstext DEFAULT ''::text NOT NULL,
    email eligibility.iwstext DEFAULT ''::text NOT NULL,
    date_of_birth date,
    work_state eligibility.iwstext,
    verification_type eligibility.iwstext DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    deactivated_at timestamp with time zone,
    verified_at timestamp with time zone,
    additional_fields jsonb,
    verification_session uuid,
    verification_2_id bigint
);


--
-- Name: verification_2; Type: TABLE; Schema: eligibility; Owner: -
--

CREATE TABLE eligibility.verification_2 (
    id bigint NOT NULL,
    user_id bigint NOT NULL,
    organization_id bigint NOT NULL,
    unique_corp_id eligibility.ilztext DEFAULT ''::text NOT NULL,
    dependent_id eligibility.citext DEFAULT ''::text NOT NULL,
    first_name eligibility.iwstext DEFAULT ''::text NOT NULL,
    last_name eligibility.iwstext DEFAULT ''::text NOT NULL,
    email eligibility.iwstext DEFAULT ''::text NOT NULL,
    date_of_birth date,
    work_state eligibility.iwstext,
    verification_type eligibility.iwstext DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    deactivated_at timestamp with time zone,
    verified_at timestamp with time zone,
    additional_fields jsonb,
    verification_session uuid,
    member_id bigint,
    member_version bigint
);


--
-- Name: verification_2_id_seq; Type: SEQUENCE; Schema: eligibility; Owner: -
--

CREATE SEQUENCE eligibility.verification_2_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: verification_2_id_seq; Type: SEQUENCE OWNED BY; Schema: eligibility; Owner: -
--

ALTER SEQUENCE eligibility.verification_2_id_seq OWNED BY eligibility.verification_2.id;


--
-- Name: verification_attempt; Type: TABLE; Schema: eligibility; Owner: -
--

CREATE TABLE eligibility.verification_attempt (
    id bigint NOT NULL,
    organization_id bigint,
    unique_corp_id eligibility.ilztext,
    dependent_id eligibility.citext,
    first_name eligibility.iwstext,
    last_name eligibility.iwstext,
    email eligibility.iwstext,
    date_of_birth date,
    work_state eligibility.iwstext,
    verification_type eligibility.iwstext DEFAULT ''::text NOT NULL,
    policy_used jsonb,
    successful_verification boolean,
    verification_id bigint,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    verified_at timestamp with time zone,
    additional_fields jsonb,
    user_id integer
);


--
-- Name: verification_attempt_id_seq; Type: SEQUENCE; Schema: eligibility; Owner: -
--

CREATE SEQUENCE eligibility.verification_attempt_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: verification_attempt_id_seq; Type: SEQUENCE OWNED BY; Schema: eligibility; Owner: -
--

ALTER SEQUENCE eligibility.verification_attempt_id_seq OWNED BY eligibility.verification_attempt.id;


--
-- Name: verification_id_seq; Type: SEQUENCE; Schema: eligibility; Owner: -
--

CREATE SEQUENCE eligibility.verification_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: verification_id_seq; Type: SEQUENCE OWNED BY; Schema: eligibility; Owner: -
--

ALTER SEQUENCE eligibility.verification_id_seq OWNED BY eligibility.verification.id;


--
-- Name: schema_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_migrations (
    version character varying(128) NOT NULL
);


--
-- Name: file id; Type: DEFAULT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.file ALTER COLUMN id SET DEFAULT nextval('eligibility.file_id_seq'::regclass);


--
-- Name: file_parse_errors id; Type: DEFAULT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.file_parse_errors ALTER COLUMN id SET DEFAULT nextval('eligibility.file_parse_errors_id_seq'::regclass);


--
-- Name: file_parse_results id; Type: DEFAULT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.file_parse_results ALTER COLUMN id SET DEFAULT nextval('eligibility.file_parse_results_id_seq'::regclass);


--
-- Name: header_alias id; Type: DEFAULT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.header_alias ALTER COLUMN id SET DEFAULT nextval('eligibility.header_alias_id_seq'::regclass);


--
-- Name: member id; Type: DEFAULT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member ALTER COLUMN id SET DEFAULT nextval('eligibility.member_id_seq'::regclass);


--
-- Name: member_address id; Type: DEFAULT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_address ALTER COLUMN id SET DEFAULT nextval('eligibility.member_address_id_seq'::regclass);


--
-- Name: member_address_versioned id; Type: DEFAULT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_address_versioned ALTER COLUMN id SET DEFAULT nextval('eligibility.member_address_versioned_id_seq'::regclass);


--
-- Name: member_address_versioned_historical id; Type: DEFAULT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_address_versioned_historical ALTER COLUMN id SET DEFAULT nextval('eligibility.member_address_versioned_historical_id_seq'::regclass);


--
-- Name: member_custom_attributes id; Type: DEFAULT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_custom_attributes ALTER COLUMN id SET DEFAULT nextval('eligibility.member_custom_attributes_id_seq'::regclass);


--
-- Name: member_verification id; Type: DEFAULT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_verification ALTER COLUMN id SET DEFAULT nextval('eligibility.member_verification_id_seq'::regclass);


--
-- Name: member_versioned id; Type: DEFAULT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_versioned ALTER COLUMN id SET DEFAULT nextval('eligibility.member_versioned_id_seq'::regclass);


--
-- Name: member_versioned_historical id; Type: DEFAULT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_versioned_historical ALTER COLUMN id SET DEFAULT nextval('eligibility.member_versioned_historical_id_seq'::regclass);


--
-- Name: organization_external_id id; Type: DEFAULT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.organization_external_id ALTER COLUMN id SET DEFAULT nextval('eligibility.organization_external_id_id_seq'::regclass);


--
-- Name: population id; Type: DEFAULT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.population ALTER COLUMN id SET DEFAULT nextval('eligibility.population_id_seq'::regclass);


--
-- Name: sub_population id; Type: DEFAULT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.sub_population ALTER COLUMN id SET DEFAULT nextval('eligibility.sub_population_id_seq'::regclass);


--
-- Name: verification id; Type: DEFAULT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.verification ALTER COLUMN id SET DEFAULT nextval('eligibility.verification_id_seq'::regclass);


--
-- Name: verification_2 id; Type: DEFAULT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.verification_2 ALTER COLUMN id SET DEFAULT nextval('eligibility.verification_2_id_seq'::regclass);


--
-- Name: verification_attempt id; Type: DEFAULT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.verification_attempt ALTER COLUMN id SET DEFAULT nextval('eligibility.verification_attempt_id_seq'::regclass);


--
-- Name: configuration configuration_pkey; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.configuration
    ADD CONSTRAINT configuration_pkey PRIMARY KEY (organization_id);


--
-- Name: verification_attempt failed_verification_pkey; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.verification_attempt
    ADD CONSTRAINT failed_verification_pkey PRIMARY KEY (id);


--
-- Name: file_parse_errors file_parse_errors_pkey; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.file_parse_errors
    ADD CONSTRAINT file_parse_errors_pkey PRIMARY KEY (id);


--
-- Name: file_parse_results file_parse_results_pkey; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.file_parse_results
    ADD CONSTRAINT file_parse_results_pkey PRIMARY KEY (id);


--
-- Name: file file_pkey; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.file
    ADD CONSTRAINT file_pkey PRIMARY KEY (id);


--
-- Name: header_alias header_alias_pkey; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.header_alias
    ADD CONSTRAINT header_alias_pkey PRIMARY KEY (id);


--
-- Name: member_2 member_2_pkey; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_2
    ADD CONSTRAINT member_2_pkey PRIMARY KEY (id);


--
-- Name: member_address member_address_member_id_key; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_address
    ADD CONSTRAINT member_address_member_id_key UNIQUE (member_id);


--
-- Name: member_address member_address_pkey; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_address
    ADD CONSTRAINT member_address_pkey PRIMARY KEY (id);


--
-- Name: member_address_versioned_historical member_address_versioned_historical_member_id_key; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_address_versioned_historical
    ADD CONSTRAINT member_address_versioned_historical_member_id_key UNIQUE (member_id);


--
-- Name: member_address_versioned_historical member_address_versioned_historical_pkey; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_address_versioned_historical
    ADD CONSTRAINT member_address_versioned_historical_pkey PRIMARY KEY (id);


--
-- Name: member_address_versioned member_address_versioned_member_id_key; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_address_versioned
    ADD CONSTRAINT member_address_versioned_member_id_key UNIQUE (member_id);


--
-- Name: member_address_versioned member_address_versioned_pkey; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_address_versioned
    ADD CONSTRAINT member_address_versioned_pkey PRIMARY KEY (id);


--
-- Name: member_custom_attributes member_attribute_uidx; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_custom_attributes
    ADD CONSTRAINT member_attribute_uidx UNIQUE (member_id, attribute_name);


--
-- Name: member_custom_attributes member_custom_attributes_pkey; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_custom_attributes
    ADD CONSTRAINT member_custom_attributes_pkey PRIMARY KEY (id);


--
-- Name: member member_pkey; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member
    ADD CONSTRAINT member_pkey PRIMARY KEY (id);


--
-- Name: member_sub_population member_sub_population_member_id_key; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_sub_population
    ADD CONSTRAINT member_sub_population_member_id_key UNIQUE (member_id);


--
-- Name: member_sub_population member_sub_population_pk; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_sub_population
    ADD CONSTRAINT member_sub_population_pk PRIMARY KEY (member_id, sub_population_id);


--
-- Name: member_verification member_verification_id_pkey; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_verification
    ADD CONSTRAINT member_verification_id_pkey PRIMARY KEY (id);


--
-- Name: member_versioned_historical member_versioned_historical_hash_value_hash_version_key; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_versioned_historical
    ADD CONSTRAINT member_versioned_historical_hash_value_hash_version_key UNIQUE (hash_value, hash_version);


--
-- Name: member_versioned_historical member_versioned_historical_pkey; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_versioned_historical
    ADD CONSTRAINT member_versioned_historical_pkey PRIMARY KEY (id);


--
-- Name: member_versioned member_versioned_pkey; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_versioned
    ADD CONSTRAINT member_versioned_pkey PRIMARY KEY (id);


--
-- Name: member_versioned mv_unique_hash_value_and_version; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_versioned
    ADD CONSTRAINT mv_unique_hash_value_and_version UNIQUE (hash_value, hash_version);


--
-- Name: header_alias org_header; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.header_alias
    ADD CONSTRAINT org_header UNIQUE (organization_id, header);


--
-- Name: organization_external_id organization_external_id_pkey; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.organization_external_id
    ADD CONSTRAINT organization_external_id_pkey PRIMARY KEY (id);


--
-- Name: sub_population population_feature_set; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.sub_population
    ADD CONSTRAINT population_feature_set UNIQUE (population_id, feature_set_name);


--
-- Name: population population_pkey; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.population
    ADD CONSTRAINT population_pkey PRIMARY KEY (id);


--
-- Name: sub_population sub_population_pkey; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.sub_population
    ADD CONSTRAINT sub_population_pkey PRIMARY KEY (id);


--
-- Name: tmp_file_parse_results tmp_file_parse_results_pkey; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.tmp_file_parse_results
    ADD CONSTRAINT tmp_file_parse_results_pkey PRIMARY KEY (id);


--
-- Name: sub_population unique_feature_set_name_within_population_key; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.sub_population
    ADD CONSTRAINT unique_feature_set_name_within_population_key UNIQUE (population_id, feature_set_name);


--
-- Name: verification_2 verification_2_pkey; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.verification_2
    ADD CONSTRAINT verification_2_pkey PRIMARY KEY (id);


--
-- Name: verification verification_pkey; Type: CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.verification
    ADD CONSTRAINT verification_pkey PRIMARY KEY (id);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: idx_address_country; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_address_country ON eligibility.member_address USING btree (country_code);


--
-- Name: idx_address_member_id; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_address_member_id ON eligibility.member_address USING btree (member_id);


--
-- Name: idx_address_updated_at; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_address_updated_at ON eligibility.member_address USING btree (updated_at);


--
-- Name: idx_configuration_organization_id; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_configuration_organization_id ON eligibility.configuration USING btree (organization_id);


--
-- Name: idx_file_created_at; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_file_created_at ON eligibility.file USING btree (created_at);


--
-- Name: idx_file_name; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_file_name ON eligibility.file USING btree (name);


--
-- Name: idx_file_org_completed_at; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_file_org_completed_at ON eligibility.file USING btree (organization_id, completed_at);


--
-- Name: idx_file_org_started_at; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_file_org_started_at ON eligibility.file USING btree (organization_id, started_at);


--
-- Name: idx_file_organization_id; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_file_organization_id ON eligibility.file USING btree (organization_id);


--
-- Name: idx_file_parse_errors_errors; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_file_parse_errors_errors ON eligibility.file_parse_errors USING gin (errors);


--
-- Name: idx_file_parse_errors_file_id; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_file_parse_errors_file_id ON eligibility.file_parse_errors USING btree (file_id);


--
-- Name: idx_file_parse_errors_org_id; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_file_parse_errors_org_id ON eligibility.file_parse_errors USING btree (organization_id);


--
-- Name: idx_file_parse_errors_record; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_file_parse_errors_record ON eligibility.file_parse_errors USING gin (record);


--
-- Name: idx_file_parse_errors_warnings; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_file_parse_errors_warnings ON eligibility.file_parse_errors USING gin (warnings);


--
-- Name: idx_file_parse_results_errors; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_file_parse_results_errors ON eligibility.file_parse_results USING gin (errors);


--
-- Name: idx_file_parse_results_file_id; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_file_parse_results_file_id ON eligibility.file_parse_results USING btree (file_id);


--
-- Name: idx_file_parse_results_org_id; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_file_parse_results_org_id ON eligibility.file_parse_results USING btree (organization_id);


--
-- Name: idx_file_parse_results_record; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_file_parse_results_record ON eligibility.file_parse_results USING gin (record);


--
-- Name: idx_file_parse_results_warnings; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_file_parse_results_warnings ON eligibility.file_parse_results USING gin (warnings);


--
-- Name: idx_file_updated_at; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_file_updated_at ON eligibility.file USING btree (updated_at);


--
-- Name: idx_gin_feature_set_details_json; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_gin_feature_set_details_json ON eligibility.sub_population USING gin (feature_set_details_json);


--
-- Name: idx_gin_sub_pop_lookup_map; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_gin_sub_pop_lookup_map ON eligibility.population USING gin (sub_pop_lookup_map_json);


--
-- Name: idx_header_organization_id; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_header_organization_id ON eligibility.header_alias USING btree (organization_id);


--
-- Name: idx_member_2_client_specific_verification; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_member_2_client_specific_verification ON eligibility.member_2 USING btree (date_of_birth, organization_id, ltrim(lower((unique_corp_id)::text)) text_pattern_ops);


--
-- Name: idx_member_2_effective_range; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_member_2_effective_range ON eligibility.member_2 USING gist (effective_range);


--
-- Name: idx_member_2_email; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_member_2_email ON eligibility.member_2 USING btree (btrim(lower((email)::text)) text_pattern_ops);


--
-- Name: idx_member_2_employer_assigned_id; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_member_2_employer_assigned_id ON eligibility.member_2 USING btree (btrim(lower((employer_assigned_id)::text)) text_pattern_ops);


--
-- Name: idx_member_2_identity; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_member_2_identity ON eligibility.member_2 USING btree (organization_id, ltrim(lower((dependent_id)::text)), ltrim(lower((unique_corp_id)::text)) text_pattern_ops);


--
-- Name: idx_member_2_name; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_member_2_name ON eligibility.member_2 USING gin (first_name, last_name);


--
-- Name: idx_member_2_name_dob; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_member_2_name_dob ON eligibility.member_2 USING gin (first_name, last_name, date_of_birth);


--
-- Name: idx_member_2_primary_verification; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_member_2_primary_verification ON eligibility.member_2 USING btree (date_of_birth, btrim(lower((email)::text)) text_pattern_ops);


--
-- Name: idx_member_2_secondary_verification; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_member_2_secondary_verification ON eligibility.member_2 USING btree (date_of_birth, btrim(lower((first_name)::text)), btrim(lower((last_name)::text)), btrim(lower((work_state)::text)) text_pattern_ops);


--
-- Name: idx_member_2_tertiary_verification; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_member_2_tertiary_verification ON eligibility.member_2 USING btree (date_of_birth, ltrim(lower((unique_corp_id)::text)) text_pattern_ops);


--
-- Name: idx_member_2_unique_corp_id; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_member_2_unique_corp_id ON eligibility.member_2 USING btree (ltrim(lower((unique_corp_id)::text), '0'::text) text_pattern_ops);


--
-- Name: idx_member_verification_member_id; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_member_verification_member_id ON eligibility.member_verification USING btree (member_id);


--
-- Name: idx_member_verification_verification_id; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_member_verification_verification_id ON eligibility.member_verification USING btree (verification_id);


--
-- Name: idx_member_versioned_address_country; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_member_versioned_address_country ON eligibility.member_address_versioned USING btree (country_code);


--
-- Name: idx_member_versioned_address_member_id; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_member_versioned_address_member_id ON eligibility.member_address_versioned USING btree (member_id);


--
-- Name: idx_member_versioned_address_updated_at; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_member_versioned_address_updated_at ON eligibility.member_address_versioned USING btree (updated_at);


--
-- Name: idx_member_versioned_client_specific_verification; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_member_versioned_client_specific_verification ON eligibility.member_versioned USING btree (date_of_birth, organization_id, ltrim(lower((unique_corp_id)::text)) text_pattern_ops);


--
-- Name: idx_member_versioned_effective_range; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_member_versioned_effective_range ON eligibility.member_versioned USING gist (effective_range);


--
-- Name: idx_member_versioned_email; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_member_versioned_email ON eligibility.member_versioned USING btree (btrim(lower((email)::text)) text_pattern_ops);


--
-- Name: idx_member_versioned_employer_assigned_id; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_member_versioned_employer_assigned_id ON eligibility.member_versioned USING btree (btrim(lower((employer_assigned_id)::text)) text_pattern_ops);


--
-- Name: idx_member_versioned_file_id; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_member_versioned_file_id ON eligibility.member_versioned USING btree (file_id);


--
-- Name: idx_member_versioned_identity; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_member_versioned_identity ON eligibility.member_versioned USING btree (organization_id, ltrim(lower((dependent_id)::text)), ltrim(lower((unique_corp_id)::text)) text_pattern_ops);


--
-- Name: idx_member_versioned_name; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_member_versioned_name ON eligibility.member_versioned USING gin (first_name, last_name);


--
-- Name: idx_member_versioned_name_dob; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_member_versioned_name_dob ON eligibility.member_versioned USING gin (first_name, last_name, date_of_birth);


--
-- Name: idx_member_versioned_pre_verified; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_member_versioned_pre_verified ON eligibility.member_versioned USING btree (pre_verified) WHERE (pre_verified = false);


--
-- Name: idx_member_versioned_primary_verification; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_member_versioned_primary_verification ON eligibility.member_versioned USING btree (date_of_birth, btrim(lower((email)::text)) text_pattern_ops);


--
-- Name: idx_member_versioned_secondary_verification; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_member_versioned_secondary_verification ON eligibility.member_versioned USING btree (date_of_birth, btrim(lower((first_name)::text)), btrim(lower((last_name)::text)), btrim(lower((work_state)::text)) text_pattern_ops);


--
-- Name: idx_member_versioned_tertiary_verification; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_member_versioned_tertiary_verification ON eligibility.member_versioned USING btree (date_of_birth, ltrim(lower((unique_corp_id)::text)) text_pattern_ops);


--
-- Name: idx_member_versioned_unique_corp_id; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_member_versioned_unique_corp_id ON eligibility.member_versioned USING btree (ltrim(lower((unique_corp_id)::text), '0'::text) text_pattern_ops);


--
-- Name: idx_parse_results_member_identity; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_parse_results_member_identity ON eligibility.file_parse_results USING btree (organization_id, ltrim(lower((unique_corp_id)::text), '0'::text), lower((dependent_id)::text) text_pattern_ops);


--
-- Name: idx_population_organization; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_population_organization ON eligibility.population USING btree (organization_id);


--
-- Name: idx_sub_population_population; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_sub_population_population ON eligibility.sub_population USING btree (population_id);


--
-- Name: idx_verification_2_deactivated_at; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_verification_2_deactivated_at ON eligibility.verification_2 USING btree (deactivated_at);


--
-- Name: idx_verification_2_member_id; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_verification_2_member_id ON eligibility.verification_2 USING btree (member_id);


--
-- Name: idx_verification_2_organization_id; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_verification_2_organization_id ON eligibility.verification_2 USING btree (organization_id);


--
-- Name: idx_verification_2_user_id; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_verification_2_user_id ON eligibility.verification_2 USING btree (user_id);


--
-- Name: idx_verification_deactivated_at; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_verification_deactivated_at ON eligibility.verification USING btree (deactivated_at);


--
-- Name: idx_verification_organization_id; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_verification_organization_id ON eligibility.verification USING btree (organization_id);


--
-- Name: idx_verification_user_id; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_verification_user_id ON eligibility.verification USING btree (user_id);


--
-- Name: idx_verification_verification_2_id; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX idx_verification_verification_2_id ON eligibility.verification USING btree (verification_2_id);


--
-- Name: tmp_file_parse_results_errors_idx; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX tmp_file_parse_results_errors_idx ON eligibility.tmp_file_parse_results USING gin (errors);


--
-- Name: tmp_file_parse_results_file_id_idx; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX tmp_file_parse_results_file_id_idx ON eligibility.tmp_file_parse_results USING btree (file_id);


--
-- Name: tmp_file_parse_results_organization_id_idx; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX tmp_file_parse_results_organization_id_idx ON eligibility.tmp_file_parse_results USING btree (organization_id);


--
-- Name: tmp_file_parse_results_organization_id_ltrim_lower_idx; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX tmp_file_parse_results_organization_id_ltrim_lower_idx ON eligibility.tmp_file_parse_results USING btree (organization_id, ltrim(lower((unique_corp_id)::text), '0'::text), lower((dependent_id)::text) text_pattern_ops);


--
-- Name: tmp_file_parse_results_record_idx; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX tmp_file_parse_results_record_idx ON eligibility.tmp_file_parse_results USING gin (record);


--
-- Name: tmp_file_parse_results_warnings_idx; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX tmp_file_parse_results_warnings_idx ON eligibility.tmp_file_parse_results USING gin (warnings);


--
-- Name: trgm_idx_member_2_dependent_id; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX trgm_idx_member_2_dependent_id ON eligibility.member_2 USING gin (dependent_id public.gin_trgm_ops);


--
-- Name: trgm_idx_member_2_email; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX trgm_idx_member_2_email ON eligibility.member_2 USING gin (email public.gin_trgm_ops);


--
-- Name: trgm_idx_member_2_first_name; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX trgm_idx_member_2_first_name ON eligibility.member_2 USING gin (first_name public.gin_trgm_ops);


--
-- Name: trgm_idx_member_2_last_name; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX trgm_idx_member_2_last_name ON eligibility.member_2 USING gin (last_name public.gin_trgm_ops);


--
-- Name: trgm_idx_member_2_unique_corp_id; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX trgm_idx_member_2_unique_corp_id ON eligibility.member_2 USING gin (unique_corp_id public.gin_trgm_ops);


--
-- Name: trgm_idx_member_versioned_dependent_id; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX trgm_idx_member_versioned_dependent_id ON eligibility.member_versioned USING gin (dependent_id public.gin_trgm_ops);


--
-- Name: trgm_idx_member_versioned_email; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX trgm_idx_member_versioned_email ON eligibility.member_versioned USING gin (email public.gin_trgm_ops);


--
-- Name: trgm_idx_member_versioned_first_name; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX trgm_idx_member_versioned_first_name ON eligibility.member_versioned USING gin (first_name public.gin_trgm_ops);


--
-- Name: trgm_idx_member_versioned_last_name; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX trgm_idx_member_versioned_last_name ON eligibility.member_versioned USING gin (last_name public.gin_trgm_ops);


--
-- Name: trgm_idx_member_versioned_unique_corp_id; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE INDEX trgm_idx_member_versioned_unique_corp_id ON eligibility.member_versioned USING gin (unique_corp_id public.gin_trgm_ops);


--
-- Name: uidx_data_provider_id_external_id; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE UNIQUE INDEX uidx_data_provider_id_external_id ON eligibility.organization_external_id USING btree (data_provider_organization_id, external_id);


--
-- Name: uidx_member_address_state_zip; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE UNIQUE INDEX uidx_member_address_state_zip ON eligibility.member_address USING btree (member_id, address_1, city, state, postal_code, country_code);


--
-- Name: uidx_member_identity; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE UNIQUE INDEX uidx_member_identity ON eligibility.member USING btree (organization_id, ltrim(lower((unique_corp_id)::text), '0'::text), lower((dependent_id)::text) text_pattern_ops);


--
-- Name: uidx_member_versioned_member_address_state_zip; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE UNIQUE INDEX uidx_member_versioned_member_address_state_zip ON eligibility.member_address_versioned USING btree (member_id, address_1, city, state, postal_code, country_code);


--
-- Name: uidx_organization_external_id; Type: INDEX; Schema: eligibility; Owner: -
--

CREATE UNIQUE INDEX uidx_organization_external_id ON eligibility.organization_external_id USING btree (source, external_id);


--
-- Name: incomplete_files_by_org _RETURN; Type: RULE; Schema: eligibility; Owner: -
--

CREATE OR REPLACE VIEW eligibility.incomplete_files_by_org AS
 WITH incomplete_file_info AS (
         SELECT incomplete_file_info_superset.file_id,
            incomplete_file_info_superset.organization_id
           FROM ( SELECT fpr.file_id,
                    fpr.organization_id
                   FROM eligibility.file_parse_results fpr
                UNION
                 SELECT fpe.file_id,
                    fpe.organization_id
                   FROM eligibility.file_parse_errors fpe) incomplete_file_info_superset
        ), org_id_map AS (
         SELECT DISTINCT ifi.organization_id AS child_org_id,
            COALESCE(oei.data_provider_organization_id, ifi.organization_id) AS parent_org_id
           FROM (incomplete_file_info ifi
             LEFT JOIN eligibility.organization_external_id oei ON ((ifi.organization_id = oei.organization_id)))
        ), latest_files AS (
         SELECT file_3.organization_id,
            file_3.success_count
           FROM (eligibility.file file_3
             JOIN ( SELECT file_2.organization_id,
                    max(file_2.completed_at) AS completed_at
                   FROM eligibility.file file_2
                  WHERE (file_2.organization_id IN ( SELECT DISTINCT org_id_map.parent_org_id
                           FROM org_id_map))
                  GROUP BY file_2.organization_id) latest_completion_timestamps ON (((file_3.organization_id = latest_completion_timestamps.organization_id) AND (file_3.completed_at = latest_completion_timestamps.completed_at))))
        ), incomplete_files AS (
         SELECT file_1.*::eligibility.file AS file,
            file_1.failure_count AS total_errors,
            file_1.success_count AS total_parsed,
                CASE
                    WHEN (COALESCE(latest_files.success_count, 0) > file_1.success_count) THEN (latest_files.success_count - file_1.success_count)
                    ELSE 0
                END AS total_missing,
            file_1.created_at
           FROM (eligibility.file file_1
             LEFT JOIN latest_files ON ((file_1.organization_id = latest_files.organization_id)))
          WHERE (file_1.id IN ( SELECT incomplete_file_info.file_id
                   FROM incomplete_file_info))
          ORDER BY file_1.created_at DESC
        )
 SELECT c.organization_id AS id,
    (COALESCE(l_files.success_count, 0))::bigint AS total_members,
    to_jsonb(c.*) AS config,
    to_jsonb(array_agg(i_files.*)) AS incomplete
   FROM ((eligibility.configuration c
     JOIN incomplete_files i_files ON ((c.organization_id = (i_files.file).organization_id)))
     LEFT JOIN latest_files l_files ON ((c.organization_id = l_files.organization_id)))
  GROUP BY c.organization_id, l_files.success_count;


--
-- Name: member_address_versioned_historical set_address_historical_timestamp; Type: TRIGGER; Schema: eligibility; Owner: -
--

CREATE TRIGGER set_address_historical_timestamp BEFORE UPDATE ON eligibility.member_address_versioned_historical FOR EACH ROW EXECUTE FUNCTION eligibility.trigger_set_timestamp();


--
-- Name: member_address set_address_timestamp; Type: TRIGGER; Schema: eligibility; Owner: -
--

CREATE TRIGGER set_address_timestamp BEFORE UPDATE ON eligibility.member_address FOR EACH ROW EXECUTE FUNCTION eligibility.trigger_set_timestamp();


--
-- Name: member_address_versioned set_address_timestamp; Type: TRIGGER; Schema: eligibility; Owner: -
--

CREATE TRIGGER set_address_timestamp BEFORE UPDATE ON eligibility.member_address_versioned FOR EACH ROW EXECUTE FUNCTION eligibility.trigger_set_timestamp();


--
-- Name: configuration set_configuration_timestamp; Type: TRIGGER; Schema: eligibility; Owner: -
--

CREATE TRIGGER set_configuration_timestamp BEFORE UPDATE ON eligibility.configuration FOR EACH ROW EXECUTE FUNCTION eligibility.trigger_set_timestamp();


--
-- Name: file set_file_timestamp; Type: TRIGGER; Schema: eligibility; Owner: -
--

CREATE TRIGGER set_file_timestamp BEFORE UPDATE ON eligibility.file FOR EACH ROW EXECUTE FUNCTION eligibility.trigger_set_timestamp();


--
-- Name: header_alias set_header_alias_timestamp; Type: TRIGGER; Schema: eligibility; Owner: -
--

CREATE TRIGGER set_header_alias_timestamp BEFORE UPDATE ON eligibility.header_alias FOR EACH ROW EXECUTE FUNCTION eligibility.trigger_set_timestamp();


--
-- Name: member_custom_attributes set_member_custom_attributes_update_timestamp; Type: TRIGGER; Schema: eligibility; Owner: -
--

CREATE TRIGGER set_member_custom_attributes_update_timestamp BEFORE UPDATE ON eligibility.member_custom_attributes FOR EACH ROW EXECUTE FUNCTION eligibility.trigger_set_timestamp();


--
-- Name: member_sub_population set_member_sub_population_update_timestamp; Type: TRIGGER; Schema: eligibility; Owner: -
--

CREATE TRIGGER set_member_sub_population_update_timestamp BEFORE UPDATE ON eligibility.member_sub_population FOR EACH ROW EXECUTE FUNCTION eligibility.trigger_set_timestamp();


--
-- Name: member set_member_timestamp; Type: TRIGGER; Schema: eligibility; Owner: -
--

CREATE TRIGGER set_member_timestamp BEFORE UPDATE ON eligibility.member FOR EACH ROW EXECUTE FUNCTION eligibility.trigger_set_timestamp();


--
-- Name: member_verification set_member_verification_timestamp; Type: TRIGGER; Schema: eligibility; Owner: -
--

CREATE TRIGGER set_member_verification_timestamp BEFORE UPDATE ON eligibility.member_verification FOR EACH ROW EXECUTE FUNCTION eligibility.trigger_set_timestamp();


--
-- Name: member_versioned_historical set_member_versioned_historical_timestamp; Type: TRIGGER; Schema: eligibility; Owner: -
--

CREATE TRIGGER set_member_versioned_historical_timestamp BEFORE UPDATE ON eligibility.member_versioned_historical FOR EACH ROW EXECUTE FUNCTION eligibility.trigger_set_timestamp();


--
-- Name: member_versioned set_member_versioned_timestamp; Type: TRIGGER; Schema: eligibility; Owner: -
--

CREATE TRIGGER set_member_versioned_timestamp BEFORE UPDATE ON eligibility.member_versioned FOR EACH ROW EXECUTE FUNCTION eligibility.trigger_set_timestamp();


--
-- Name: population set_population_update_timestamp; Type: TRIGGER; Schema: eligibility; Owner: -
--

CREATE TRIGGER set_population_update_timestamp BEFORE UPDATE ON eligibility.population FOR EACH ROW EXECUTE FUNCTION eligibility.trigger_set_timestamp();


--
-- Name: sub_population set_sub_population_update_timestamp; Type: TRIGGER; Schema: eligibility; Owner: -
--

CREATE TRIGGER set_sub_population_update_timestamp BEFORE UPDATE ON eligibility.sub_population FOR EACH ROW EXECUTE FUNCTION eligibility.trigger_set_timestamp();


--
-- Name: verification_2 set_verification_2_timestamp; Type: TRIGGER; Schema: eligibility; Owner: -
--

CREATE TRIGGER set_verification_2_timestamp BEFORE UPDATE ON eligibility.verification_2 FOR EACH ROW EXECUTE FUNCTION eligibility.trigger_set_timestamp();


--
-- Name: verification_attempt set_verification_attempt_timestamp; Type: TRIGGER; Schema: eligibility; Owner: -
--

CREATE TRIGGER set_verification_attempt_timestamp BEFORE UPDATE ON eligibility.verification_attempt FOR EACH ROW EXECUTE FUNCTION eligibility.trigger_set_timestamp();


--
-- Name: verification set_verification_timestamp; Type: TRIGGER; Schema: eligibility; Owner: -
--

CREATE TRIGGER set_verification_timestamp BEFORE UPDATE ON eligibility.verification FOR EACH ROW EXECUTE FUNCTION eligibility.trigger_set_timestamp();


--
-- Name: member_custom_attributes custom_attribute_member_id_fkey; Type: FK CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_custom_attributes
    ADD CONSTRAINT custom_attribute_member_id_fkey FOREIGN KEY (member_id) REFERENCES eligibility.member_versioned(id) ON DELETE CASCADE;


--
-- Name: file file_organization_id_fkey; Type: FK CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.file
    ADD CONSTRAINT file_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES eligibility.configuration(organization_id) ON DELETE CASCADE;


--
-- Name: file_parse_errors file_parse_errors_file_id_fkey; Type: FK CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.file_parse_errors
    ADD CONSTRAINT file_parse_errors_file_id_fkey FOREIGN KEY (file_id) REFERENCES eligibility.file(id);


--
-- Name: file_parse_errors file_parse_errors_organization_id_fkey; Type: FK CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.file_parse_errors
    ADD CONSTRAINT file_parse_errors_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES eligibility.configuration(organization_id) ON DELETE CASCADE;


--
-- Name: file_parse_results file_parse_results_file_id_fkey; Type: FK CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.file_parse_results
    ADD CONSTRAINT file_parse_results_file_id_fkey FOREIGN KEY (file_id) REFERENCES eligibility.file(id) ON DELETE CASCADE;


--
-- Name: file_parse_results file_parse_results_organization_id_fkey; Type: FK CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.file_parse_results
    ADD CONSTRAINT file_parse_results_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES eligibility.configuration(organization_id) ON DELETE CASCADE;


--
-- Name: member_address fk_member_address_member; Type: FK CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_address
    ADD CONSTRAINT fk_member_address_member FOREIGN KEY (member_id) REFERENCES eligibility.member(id) ON DELETE CASCADE;


--
-- Name: member_address_versioned_historical fk_member_address_versioned_historical_member; Type: FK CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_address_versioned_historical
    ADD CONSTRAINT fk_member_address_versioned_historical_member FOREIGN KEY (member_id) REFERENCES eligibility.member_versioned_historical(id) ON DELETE CASCADE;


--
-- Name: member_address_versioned fk_member_address_versioned_member; Type: FK CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_address_versioned
    ADD CONSTRAINT fk_member_address_versioned_member FOREIGN KEY (member_id) REFERENCES eligibility.member_versioned(id) ON DELETE CASCADE;


--
-- Name: header_alias header_alias_organization_id_fkey; Type: FK CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.header_alias
    ADD CONSTRAINT header_alias_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES eligibility.configuration(organization_id) ON DELETE CASCADE;


--
-- Name: member_2 member_2_organization_id_fkey; Type: FK CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_2
    ADD CONSTRAINT member_2_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES eligibility.configuration(organization_id) ON DELETE CASCADE;


--
-- Name: member member_file_id_fkey; Type: FK CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member
    ADD CONSTRAINT member_file_id_fkey FOREIGN KEY (file_id) REFERENCES eligibility.file(id);


--
-- Name: member_versioned member_file_id_fkey; Type: FK CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_versioned
    ADD CONSTRAINT member_file_id_fkey FOREIGN KEY (file_id) REFERENCES eligibility.file(id);


--
-- Name: member member_organization_id_fkey; Type: FK CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member
    ADD CONSTRAINT member_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES eligibility.configuration(organization_id) ON DELETE CASCADE;


--
-- Name: member_versioned member_organization_id_fkey; Type: FK CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_versioned
    ADD CONSTRAINT member_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES eligibility.configuration(organization_id) ON DELETE CASCADE;


--
-- Name: member_sub_population member_sub_population_member_id_fkey; Type: FK CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_sub_population
    ADD CONSTRAINT member_sub_population_member_id_fkey FOREIGN KEY (member_id) REFERENCES eligibility.member_versioned(id) ON DELETE CASCADE;


--
-- Name: member_verification member_verification_member_id_fkey; Type: FK CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_verification
    ADD CONSTRAINT member_verification_member_id_fkey FOREIGN KEY (member_id) REFERENCES eligibility.member_versioned(id) ON DELETE CASCADE;


--
-- Name: organization_external_id organization_external_id_organization_id_fkey; Type: FK CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.organization_external_id
    ADD CONSTRAINT organization_external_id_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES eligibility.configuration(organization_id);


--
-- Name: sub_population sub_population_population_fkey; Type: FK CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.sub_population
    ADD CONSTRAINT sub_population_population_fkey FOREIGN KEY (population_id) REFERENCES eligibility.population(id) ON DELETE CASCADE;


--
-- Name: verification_2 verification_2_member_id_fkey; Type: FK CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.verification_2
    ADD CONSTRAINT verification_2_member_id_fkey FOREIGN KEY (member_id) REFERENCES eligibility.member_2(id);


--
-- Name: verification_2 verification_2_organization_id_fkey; Type: FK CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.verification_2
    ADD CONSTRAINT verification_2_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES eligibility.configuration(organization_id) ON DELETE CASCADE;


--
-- Name: member_verification verification_attempt_id_fkey; Type: FK CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_verification
    ADD CONSTRAINT verification_attempt_id_fkey FOREIGN KEY (verification_attempt_id) REFERENCES eligibility.verification_attempt(id) ON DELETE CASCADE;


--
-- Name: verification_attempt verification_attempt_organization_id_fkey; Type: FK CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.verification_attempt
    ADD CONSTRAINT verification_attempt_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES eligibility.configuration(organization_id) ON DELETE CASCADE;


--
-- Name: member_verification verification_id_fkey; Type: FK CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.member_verification
    ADD CONSTRAINT verification_id_fkey FOREIGN KEY (verification_id) REFERENCES eligibility.verification(id) ON DELETE CASCADE;


--
-- Name: verification verification_organization_id_fkey; Type: FK CONSTRAINT; Schema: eligibility; Owner: -
--

ALTER TABLE ONLY eligibility.verification
    ADD CONSTRAINT verification_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES eligibility.configuration(organization_id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--


--
-- Dbmate schema migrations
--

INSERT INTO public.schema_migrations (version) VALUES
    ('20201022023923'),
    ('20201221173427'),
    ('20201222232212'),
    ('20210108165707'),
    ('20210112203738'),
    ('20210120222919'),
    ('20210127210431'),
    ('20210127211517'),
    ('20210128180844'),
    ('20210622183413'),
    ('20210726172103'),
    ('20210811210847'),
    ('20210819185537'),
    ('20210917174950'),
    ('20211014014241'),
    ('20211117181122'),
    ('20211202190806'),
    ('20211214160806'),
    ('20220111025325'),
    ('20220218221306'),
    ('20220218222724'),
    ('20220218222756'),
    ('20220418150803'),
    ('20220425175059'),
    ('20220506174518'),
    ('20220516142007'),
    ('20220526225115'),
    ('20220623224541'),
    ('20220624154345'),
    ('20220721170136'),
    ('20220726142741'),
    ('20220808151736'),
    ('20220808213645'),
    ('20220808221757'),
    ('20220824162540'),
    ('20220915185539'),
    ('20220921161516'),
    ('20221006191036'),
    ('20221014185717'),
    ('20221020195232'),
    ('20221108170725'),
    ('20221109001915'),
    ('20221109210357'),
    ('20221201233534'),
    ('20221219172048'),
    ('20230120211812'),
    ('20230322181008'),
    ('20230323192226'),
    ('20230404205052'),
    ('20230426042157'),
    ('20230510145259'),
    ('20230530195632'),
    ('20230620160953'),
    ('20230705191611'),
    ('20230712192957'),
    ('20230719183616'),
    ('20230802184355'),
    ('20230803205412'),
    ('20230808194651'),
    ('20230809180104'),
    ('20230821144016'),
    ('20230822170108'),
    ('20230822190732'),
    ('20230825170546'),
    ('20230830145834'),
    ('20230830160216'),
    ('20230906203146'),
    ('20230906215155'),
    ('20230908094048'),
    ('20230915175449'),
    ('20230915184321'),
    ('20230915184429'),
    ('20230915184523'),
    ('20230915184600'),
    ('20230918205636'),
    ('20230918205647'),
    ('20230918205716'),
    ('20230918205723'),
    ('20230918205757'),
    ('20230918205803'),
    ('20230918210002'),
    ('20230918210016'),
    ('20230918210031'),
    ('20230918210056'),
    ('20230918210152'),
    ('20230920154954'),
    ('20230921131821'),
    ('20230921194604'),
    ('20230922194321'),
    ('20231012164234'),
    ('20231012192702'),
    ('20231012193053'),
    ('20231012210544'),
    ('20231012210913'),
    ('20231013141141'),
    ('20231030205244'),
    ('20231031133442'),
    ('20231031134750'),
    ('20231031134759'),
    ('20231031134803'),
    ('20231031134819'),
    ('20231031135118'),
    ('20231031135127'),
    ('20231031135148'),
    ('20231031135154'),
    ('20231031135206'),
    ('20231031135212'),
    ('20231031135219'),
    ('20231031135224'),
    ('20231031135227'),
    ('20231031135252'),
    ('20231031152753'),
    ('20231102141534'),
    ('20231116181941'),
    ('20231120185806'),
    ('20231121155739'),
    ('20231203153508'),
    ('20231203153937'),
    ('20231204145827'),
    ('20231205171242'),
    ('20231205202053'),
    ('20231206195136'),
    ('20231213224539'),
    ('20231215114152'),
    ('20231218200705'),
    ('20240119150151'),
    ('20240130014958'),
    ('20240130015006'),
    ('20240205152612'),
    ('20240205160347'),
    ('20240205160420'),
    ('20240220195232'),
    ('20240220195328'),
    ('20240222160849'),
    ('20240229194237'),
    ('20240229194813'),
    ('20240329171139'),
    ('20240329171309'),
    ('20240329171427'),
    ('20240329171526'),
    ('20240329171747'),
    ('20240329171845'),
    ('20240329171937'),
    ('20240425173713'),
    ('20240528095327'),
    ('20240529181107'),
    ('20240604180853'),
    ('20240624172732'),
    ('20240625122156'),
    ('20240709145155'),
    ('20240722154927'),
    ('20240905200901'),
    ('20240905200905'),
    ('20241113144104'),
    ('20250130194943');
