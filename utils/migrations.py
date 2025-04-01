import logging
from typing import List, Tuple

import aiosql
import asyncpg

from db.clients.member_client import Members
from db.clients.postgres_connector import PostgresConnector

logger = logging.getLogger(__name__)

NAME_EMAIL_DOB_MATCH_CRITERIA = [
    (
        "source.first_name",
        "target.first_name",
    ),
    (
        "source.last_name",
        "target.last_name",
    ),
    ("source.email", "target.email"),
    ("source.date_of_birth", "target.date_of_birth"),
]

NAME_MATCH_CRITERIA = [
    (
        "source.first_name",
        "target.first_name",
    ),
    (
        "source.last_name",
        "target.last_name",
    ),
    ("source.email", "target.email"),
    ("source.date_of_birth", "target.date_of_birth"),
]

NAME_DOB_CRITERIA = [
    (
        "source.first_name",
        "target.first_name",
    ),
    (
        "source.last_name",
        "target.last_name",
    ),
    ("source.date_of_birth", "target.date_of_birth"),
]

ALTID_TO_UNQCORPID_MATCH_CRITERIA = [
    (
        "ltrim(lower(source.record->>'altId'), '0')",
        "ltrim(lower(target.unique_corp_id), '0')",
    ),
    (
        "ltrim(lower(source.dependent_id), '0')",
        "ltrim(lower(target.dependent_id), '0')",
    ),
    ("source.email", "target.email"),
]

SUREST_MATCH_CRITERIA = ALTID_TO_UNQCORPID_MATCH_CRITERIA


async def analyze(
    organization_id: int, connector: PostgresConnector = None
) -> Tuple[int, int]:
    """
    Identify the number of records that are kafka-based vs file-based for an organization and print it out

    Args:
        organization_id: organization_id
        connector: PostgresConnector
    Returns:

    """
    member_client: Members = Members(connector=connector)

    n_kafka_members: int = await member_client.get_kafka_record_count_for_org(
        organization_id=organization_id
    )
    n_file_members: int = await member_client.get_file_record_count_for_org(
        organization_id=organization_id
    )

    print(
        f"""
        ### PARAMS ###
        organization_id: {organization_id}

        ### RESULTS ###
        Number of File Members: {n_file_members}
        Number of Kafka Members: {n_kafka_members}
        """
    )

    return n_file_members, n_kafka_members


async def update_file_to_kafka_matched_records_for_org(
    organization_id: int,
    match_fields: List[Tuple[str, str]],
    no_op: bool = True,
    connector: PostgresConnector = None,
    debug: bool = False,
) -> int:
    """
    Identify the number of 1:1 records that are kafka-based vs file-based for an organization
    The match_fields tuple should be formatted in the order of (source.field_name, target.field_name) where
    source is the kafka record, and target is the file record that we will be copying to.

    Args:
        organization_id:
        match_fields: List[Tuple[str, str]]
        no_op: bool = True
        connector: PostgresConnector = None
    Returns:
        int - number of rows updated

    """
    member_client: Members = Members(connector=connector)
    sql_str: str = (
        _get_matched_file_to_kafka_records_for_org_sql(match_fields=match_fields)
        + "\n\n"
        + _update_file_record_with_kafka_record_sql()
    )

    if debug:
        print(sql_str)

    queries = aiosql.from_str(sql_str, "asyncpg")
    member_client.client.queries.add_child_queries(
        child_name="migration", child_queries=queries
    )

    async with member_client.client.connector.transaction() as c:
        matches: List[
            asyncpg.Record
        ] = await member_client.client.queries.migration.get_matched_records_for_org(
            c, organization_id=organization_id
        )

    file_to_kafka_id_pairs: List[Tuple[int, int]] = [
        (match["kafka_record_id"], match["file_record_id"]) for match in matches
    ]

    print(
        f"""
        ### PARAMS ###
        organization_id: {organization_id}
        match_fields: {match_fields}
        no_op: {no_op}

        ### RESULTS ###
        Number of Exact Matches: {len(file_to_kafka_id_pairs)}
        """
    )

    if no_op:
        return 0

    # Merge the records here
    async with member_client.client.connector.transaction() as c:
        updated = await member_client.client.queries.migration.update_file_record_with_kafka_record(
            c, mapping=file_to_kafka_id_pairs
        )

    print(f"{len(updated)} records updated")

    return len(updated)


async def update_matched_records_for_org(
    organization_id: int,
    match_fields: List[Tuple[str, str]],
    source_criteria_sql: str,
    destination_criteria_sql: str,
    no_op: bool = True,
    connector: PostgresConnector = None,
    debug: bool = False,
) -> int:
    """
    Identify the number of 1:1 records that are kafka-based vs file-based for an organization
    The match_fields tuple should be formatted in the order of (source.field_name, target.field_name) where
    source is the kafka record, and target is the file record that we will be copying to.

    Args:
        organization_id:
        match_fields: List[Tuple[str, str]]
        source_criteria_sql: str
        destination_criteria_sql: str
        no_op: bool = True
        connector: PostgresConnector = None
        debug: bool = False
    Returns:
        int - number of rows updated

    """
    member_client: Members = Members(connector=connector)
    sql_str: str = (
        _get_matched_records_for_org_sql(
            match_fields=match_fields,
            source_criteria_sql=source_criteria_sql,
            destination_criteria_sql=destination_criteria_sql,
        )
        + "\n\n"
        + _update_file_record_with_kafka_record_sql()
    )

    if debug:
        print(sql_str)

    queries = aiosql.from_str(sql_str, "asyncpg")
    member_client.client.queries.add_child_queries(
        child_name="migration", child_queries=queries
    )

    async with member_client.client.connector.transaction() as c:
        matches: List[
            asyncpg.Record
        ] = await member_client.client.queries.migration.get_matched_records_for_org(
            c, organization_id=organization_id
        )

    file_to_kafka_id_pairs: List[Tuple[int, int]] = [
        (match["source_record_id"], match["dest_record_id"]) for match in matches
    ]

    print(
        f"""
        ### PARAMS ###
        organization_id: {organization_id}
        match_fields: {match_fields}
        no_op: {no_op}

        ### RESULTS ###
        Number of Exact Matches: {len(file_to_kafka_id_pairs)}
        """
    )

    if no_op:
        return 0

    # Merge the records here
    async with member_client.client.connector.transaction() as c:
        updated = await member_client.client.queries.migration.update_file_record_with_kafka_record(
            c, mapping=file_to_kafka_id_pairs
        )

    print(f"{len(updated)} records updated")

    return len(updated)


async def update_matched_records_for_pairs(
    record_pairs: List[Tuple[int, int]], connector: PostgresConnector = None
) -> int:
    """
    Based on a list of record pairs, reconcile those record pairs

    Args:
        record_pairs: List[Tuple[int, int]]
        connector: PostgresConnector = None
    Returns:
        int - number of rows updated

    """
    member_client: Members = Members(connector=connector)
    sql_str: str = _update_file_record_with_kafka_record_sql()

    queries = aiosql.from_str(sql_str, "asyncpg")
    member_client.client.queries.add_child_queries(
        child_name="migration", child_queries=queries
    )

    # Merge the records here
    async with member_client.client.connector.transaction() as c:
        updated = await member_client.client.queries.migration.update_file_record_with_kafka_record(
            c, mapping=record_pairs
        )

    print(f"{len(updated)} records updated")

    return len(updated)


def _get_matched_file_to_kafka_records_for_org_sql(
    match_fields: List[Tuple[str, str]], one_to_one: bool = True
) -> str:
    """
    Format an aiosql query that will match up file records and kafka records based on the input match_fields
    and return a list of member.id pairs that are matches

    Args:
        match_fields: List[Tuple[str, str]]
        one_to_one: Only return exact 1 to 1 matches between file records and kafka records if True,
                    otherwise, will return a row for every match

    Returns:
        str: An aiosql query definition
    """

    def _conditions(field_pairs: List[Tuple[str, str]]) -> List[str]:
        return [f"{field[0]}={field[1]}\n" for field in field_pairs]

    def _on_clause(fields: List[str], op: str):
        return f"{op} ".join(fields)

    on_clause_formatted: str = _on_clause(fields=_conditions(match_fields), op="AND")

    one_to_one_formatted: str = ""
    if one_to_one:
        source_one_to_one_clause: str = _on_clause(
            fields=_conditions(
                [(f[0].replace("source", "source2"), f[1]) for f in match_fields]
            ),
            op="AND",
        )
        target_one_to_one_clause: str = _on_clause(
            fields=_conditions(
                [(f[0], f[1].replace("target", "target2")) for f in match_fields]
            ),
            op="AND",
        )
        one_to_one_formatted: str = f"""
            WHERE NOT EXISTS (
                SELECT 1
                FROM kafka_records source2
                WHERE {source_one_to_one_clause}
                AND source2.id != source.id
            ) AND NOT EXISTS (
                SELECT 1
                FROM file_records target2
                WHERE {target_one_to_one_clause}
                AND target2.id != target.id
            )
        """

    formatted_str: str = f"""
        -- name: get_matched_records_for_org
        WITH file_records AS (
            SELECT *
            FROM eligibility.member
            WHERE organization_id = :organization_id
            AND file_id IS NOT NULL
        ), kafka_records AS (
            SELECT *
            FROM eligibility.member
            WHERE organization_id = :organization_id
            AND file_id IS NULL
        )
        SELECT
            target.id as file_record_id,
            source.id as kafka_record_id
        FROM file_records target
        INNER JOIN kafka_records source 
        ON {on_clause_formatted}
        {one_to_one_formatted};
    """

    return formatted_str


def _get_matched_records_for_org_sql(
    match_fields: List[Tuple[str, str]],
    source_criteria_sql: str,
    destination_criteria_sql: str,
    one_to_one: bool = True,
) -> str:
    """
    Format an aiosql query that will match up a source batch with a destination batch based on the input match_fields
    and a criteria to select each respective source and destination batch and return a list of member.id pairs
    that are matches

    Args:
        match_fields: List[Tuple[str, str]]
        source_criteria_sql: str
        destination_criteria_sql: str
        one_to_one: Only return exact 1 to 1 matches between source records and destination records if True,
                    otherwise, will return a row for every match

    Returns:
        str: An aiosql query definition
    """

    def _conditions(field_pairs: List[Tuple[str, str]]) -> List[str]:
        return [f"{field[0]}={field[1]}\n" for field in field_pairs]

    def _on_clause(fields: List[str], op: str):
        return f"{op} ".join(fields)

    on_clause_formatted: str = _on_clause(fields=_conditions(match_fields), op="AND")

    one_to_one_formatted: str = ""
    if one_to_one:
        source_one_to_one_clause: str = _on_clause(
            fields=_conditions(
                [(f[0].replace("source", "source2"), f[1]) for f in match_fields]
            ),
            op="AND",
        )
        target_one_to_one_clause: str = _on_clause(
            fields=_conditions(
                [(f[0], f[1].replace("target", "target2")) for f in match_fields]
            ),
            op="AND",
        )
        one_to_one_formatted: str = f"""
            WHERE NOT EXISTS (
                SELECT 1
                FROM source_records source2
                WHERE {source_one_to_one_clause}
                AND source2.id != source.id
            ) AND NOT EXISTS (
                SELECT 1
                FROM destination_records target2
                WHERE {target_one_to_one_clause}
                AND target2.id != target.id
            )
        """

    formatted_str: str = f"""
        -- name: get_matched_records_for_org
        WITH destination_records AS (
            SELECT *
            FROM eligibility.member
            WHERE organization_id = :organization_id
            AND {destination_criteria_sql}
        ), source_records AS (
            SELECT *
            FROM eligibility.member
            WHERE organization_id = :organization_id
            AND {source_criteria_sql}
        )
        SELECT
            target.id as dest_record_id,
            source.id as source_record_id
        FROM destination_records target
        INNER JOIN source_records source 
        ON {on_clause_formatted}
        {one_to_one_formatted};
    """

    return formatted_str


def _update_file_record_with_kafka_record_sql() -> str:
    """
    Format an aiosql query that will take a list of ID pairs as input and swap the data on the member record,
    update the member_id of the associated address, and delete the kafka record.

    Returns:
        str: An aiosql query definition
    """
    formatted_str: str = """
        -- name: update_file_record_with_kafka_record
        WITH mappings as (
            SELECT (unnest(:mapping::eligibility.id_to_id[])::eligibility.id_to_id).*
        ), updated_addresses as (
            -- Update the member addresses with the new ID's
            UPDATE eligibility.member_address ma
            SET member_id = mappings.target_id
            FROM mappings
            WHERE mappings.source_id = ma.member_id
        ), kafka as (
            DELETE FROM eligibility.member mem
            USING mappings m
            WHERE m.source_id = mem.id
            RETURNING m.*, mem.*
        ), updated as (
            UPDATE eligibility.member mem
            SET file_id = NULL,
                unique_corp_id = kafka.unique_corp_id,
                dependent_id = kafka.dependent_id,
                record = kafka.record,
                effective_range = kafka.effective_range,
                first_name = kafka.first_name,
                last_name = kafka.last_name,
                email = kafka.email,
                date_of_birth = kafka.date_of_birth,
                work_state = kafka.work_state,
                gender_code = kafka.gender_code,
                do_not_contact = kafka.do_not_contact,
                employer_assigned_id = kafka.employer_assigned_id
            FROM kafka
            WHERE kafka.target_id = mem.id
            RETURNING mem.*
        )
        SELECT * FROM updated;
    """
    return formatted_str
