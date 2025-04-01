from typing import List, Optional, Tuple

import aiosql
import asyncpg
import pytest
from aiosql.queries import Queries
from tests.factories import data_models

from db.clients.member_client import Members
from db.clients.postgres_connector import PostgresConnector
from db.model import Address, Configuration, File, Member
from utils import migrations

pytestmark = pytest.mark.asyncio


class TestCompositeKeyMigration:
    @staticmethod
    async def test_analyze(
        e9y_connector: PostgresConnector,
        test_config: Configuration,
        test_file: File,
        member_test_client: Members,
    ):
        # Given
        # 1. Add a bunch of file records
        n_file_members = 100
        file_members: List[Member] = data_models.MemberFactory.create_batch(
            size=n_file_members,
            organization_id=test_config.organization_id,
            file_id=test_file.id,
        )
        # 2. Add a bunch of kafka records
        n_kafka_members = 12
        kafka_members: List[Member] = data_models.MemberFactory.create_batch(
            size=n_kafka_members,
            organization_id=test_config.organization_id,
            file_id=None,
        )

        await member_test_client.bulk_persist(models=file_members + kafka_members)

        # When
        retrieved_file_members, retrieved_kafka_members = await migrations.analyze(
            organization_id=test_config.organization_id, connector=e9y_connector
        )

        assert (retrieved_file_members, retrieved_kafka_members) == (
            n_file_members,
            n_kafka_members,
        )

    @staticmethod
    async def test_get_matched_records_for_org_sql_single_match_one_to_one_on(
        e9y_connector: PostgresConnector,
        test_config: Configuration,
        test_file: File,
        member_test_client: Members,
    ):
        """1 file record and 1 kafka record, test that the query returns a tuple of ID's that match with one_to_one=True"""
        # Given
        # A kafka member that needs reconciliation
        unmatched_kafka_member_1: Member = await member_test_client.persist(
            model=data_models.MemberFactory.create(
                file_id=None,
                organization_id=test_config.organization_id,
                unique_corp_id="CCC",
                dependent_id="EEE",
                email="senna@mclaren.com",
                record={
                    "subscriberId": "CCC",
                    "altId": "DDD",
                    "primaryMemberId": "EEE",
                },
            )
        )
        # A file member that needs reconciliation with 1.
        unmatched_file_member_1: Member = await member_test_client.persist(
            model=data_models.MemberFactory.create(
                file_id=test_file.id,
                organization_id=test_config.organization_id,
                unique_corp_id="DDD",
                dependent_id="EEE",
                email="senna@mclaren.com",
                record={
                    # Mapped from altId
                    "unique_corp_id": "DDD",
                    # Mapped from primaryMemberId
                    "dependent_id": "EEE",
                },
            )
        )
        # Reconciled kafka members
        matched_kafka_members: List[Member] = data_models.MemberFactory.create_batch(
            size=10,
            file_id=None,
            organization_id=test_config.organization_id,
        )
        # Unreconciled file members
        unmatched_file_members: List[Member] = data_models.MemberFactory.create_batch(
            size=10,
            file_id=None,
            organization_id=test_config.organization_id,
        )

        await member_test_client.bulk_persist(
            models=matched_kafka_members + unmatched_file_members
        )
        # Load our sql
        sql_str = migrations._get_matched_file_to_kafka_records_for_org_sql(
            match_fields=migrations.ALTID_TO_UNQCORPID_MATCH_CRITERIA, one_to_one=True
        )
        queries: Queries = aiosql.from_str(sql_str, "asyncpg")
        member_test_client.load_child_queries(name="test", queries=queries)

        # When
        async with member_test_client.client.connector.connection() as c:
            fetched: List[
                asyncpg.Record
            ] = await member_test_client.client.queries.test.get_matched_records_for_org(
                c, organization_id=test_config.organization_id
            )

        assert len(fetched) == 1
        assert (
            fetched[0].get("kafka_record_id"),
            fetched[0].get("file_record_id"),
        ) == (unmatched_kafka_member_1.id, unmatched_file_member_1.id)

    @staticmethod
    async def test_get_matched_records_for_org_sql_single_match_one_to_one_off(
        e9y_connector: PostgresConnector,
        test_config: Configuration,
        test_file: File,
        member_test_client: Members,
    ):
        """1 file record and 1 kafka record, test that the query returns a tuple of ID's that match with one_to_one=False"""
        # Given
        # A kafka member that needs reconciliation
        unmatched_kafka_member_1: Member = await member_test_client.persist(
            model=data_models.MemberFactory.create(
                file_id=None,
                organization_id=test_config.organization_id,
                unique_corp_id="CCC",
                dependent_id="EEE",
                email="senna@mclaren.com",
                record={
                    "subscriberId": "CCC",
                    "altId": "DDD",
                    "primaryMemberId": "EEE",
                },
            )
        )
        # A file member that needs reconciliation with 1.
        unmatched_file_member_1: Member = await member_test_client.persist(
            model=data_models.MemberFactory.create(
                file_id=test_file.id,
                organization_id=test_config.organization_id,
                unique_corp_id="DDD",
                dependent_id="EEE",
                email="senna@mclaren.com",
                record={
                    # Mapped from altId
                    "unique_corp_id": "DDD",
                    # Mapped from primaryMemberId
                    "dependent_id": "EEE",
                },
            )
        )
        # Reconciled kafka members
        matched_kafka_members: List[Member] = data_models.MemberFactory.create_batch(
            size=10,
            file_id=None,
            organization_id=test_config.organization_id,
        )
        # Unreconciled file members
        unmatched_file_members: List[Member] = data_models.MemberFactory.create_batch(
            size=10,
            file_id=None,
            organization_id=test_config.organization_id,
        )

        await member_test_client.bulk_persist(
            models=matched_kafka_members + unmatched_file_members
        )
        # Load our sql
        sql_str = migrations._get_matched_file_to_kafka_records_for_org_sql(
            match_fields=migrations.ALTID_TO_UNQCORPID_MATCH_CRITERIA, one_to_one=False
        )
        queries: Queries = aiosql.from_str(sql_str, "asyncpg")
        member_test_client.load_child_queries(name="test", queries=queries)

        # When
        async with member_test_client.client.connector.connection() as c:
            fetched: List[
                asyncpg.Record
            ] = await member_test_client.client.queries.test.get_matched_records_for_org(
                c, organization_id=test_config.organization_id
            )

        assert len(fetched) == 1
        assert (
            fetched[0].get("kafka_record_id"),
            fetched[0].get("file_record_id"),
        ) == (unmatched_kafka_member_1.id, unmatched_file_member_1.id)

    @staticmethod
    async def test_get_matched_records_for_org_sql_duplicate_kafka_member_one_to_one_off(
        e9y_connector: PostgresConnector,
        test_config: Configuration,
        test_file: File,
        member_test_client: Members,
    ):
        """1 file record and 2 kafka record, test that the query returns 2 tuples of ID's that match with one_to_one=False"""
        # Given
        # A kafka member that needs reconciliation
        unmatched_kafka_member_1: Member = await member_test_client.persist(
            model=data_models.MemberFactory.create(
                file_id=None,
                organization_id=test_config.organization_id,
                unique_corp_id="CCC",
                dependent_id="EEE",
                email="dricciardo@redbull.com",
                record={
                    "subscriberId": "CCC",
                    "altId": "DDD",
                    "primaryMemberId": "EEE",
                },
            )
        )
        # A duplicate kafka member that needs reconciliation
        unmatched_kafka_member_2: Member = await member_test_client.persist(
            model=data_models.MemberFactory.create(
                file_id=None,
                organization_id=test_config.organization_id,
                unique_corp_id="CCX",
                dependent_id="EEE",
                email="dricciardo@redbull.com",
                record={
                    "subscriberId": "CCC",
                    "altId": "DDD",
                    "primaryMemberId": "EEE",
                },
            )
        )
        # A file member that needs reconciliation with 1.
        unmatched_file_member_1: Member = await member_test_client.persist(
            model=data_models.MemberFactory.create(
                file_id=test_file.id,
                organization_id=test_config.organization_id,
                unique_corp_id="DDD",
                dependent_id="EEE",
                email="dricciardo@redbull.com",
                record={
                    # Mapped from altId
                    "unique_corp_id": "DDD",
                    # Mapped from primaryMemberId
                    "dependent_id": "EEE",
                },
            )
        )
        # Reconciled kafka members
        matched_kafka_members: List[Member] = data_models.MemberFactory.create_batch(
            size=10,
            file_id=None,
            organization_id=test_config.organization_id,
        )
        # Unreconciled file members
        unmatched_file_members: List[Member] = data_models.MemberFactory.create_batch(
            size=10,
            file_id=None,
            organization_id=test_config.organization_id,
        )

        await member_test_client.bulk_persist(
            models=matched_kafka_members + unmatched_file_members
        )
        # Load our sql
        sql_str: str = migrations._get_matched_file_to_kafka_records_for_org_sql(
            match_fields=migrations.ALTID_TO_UNQCORPID_MATCH_CRITERIA, one_to_one=False
        )
        queries: Queries = aiosql.from_str(sql_str, "asyncpg")
        member_test_client.load_child_queries(name="test", queries=queries)

        # When
        async with member_test_client.client.connector.connection() as c:
            fetched: List[
                asyncpg.Record
            ] = await member_test_client.client.queries.test.get_matched_records_for_org(
                c, organization_id=test_config.organization_id
            )

        # Expect 2 since one_to_one was set to False when calling _get_matched_records_for_org_sql()
        assert len(fetched) == 2
        # Make sure we get back the 2 pairs we expected
        assert set(
            [
                (pair.get("kafka_record_id"), pair.get("file_record_id"))
                for pair in fetched
            ]
        ) == set(
            [
                (unmatched_kafka_member_1.id, unmatched_file_member_1.id),
                (unmatched_kafka_member_2.id, unmatched_file_member_1.id),
            ]
        )

    @staticmethod
    async def test_get_matched_records_for_org_sql_duplicate_kafka_member_one_to_one_on(
        e9y_connector: PostgresConnector,
        test_config: Configuration,
        test_file: File,
        member_test_client: Members,
    ):
        """1 file record and 2 kafka record, test that the query returns NO ID's that match with one_to_one=True"""
        # Given
        # A kafka member that needs reconciliation
        unmatched_kafka_member_1: Member = data_models.MemberFactory.create(
            file_id=None,
            organization_id=test_config.organization_id,
            unique_corp_id="CCC",
            dependent_id="EEE",
            email="maxverstappen@redbull.com",
            record={"subscriberId": "CCC", "altId": "DDD", "primaryMemberId": "EEE"},
        )
        # A duplicate kafka member that needs reconciliation
        unmatched_kafka_member_2: Member = data_models.MemberFactory.create(
            file_id=None,
            organization_id=test_config.organization_id,
            unique_corp_id="CCX",
            dependent_id="EEE",
            email="maxverstappen@redbull.com",
            record={"subscriberId": "CCC", "altId": "DDD", "primaryMemberId": "EEE"},
        )
        # A file member that needs reconciliation with 1.
        unmatched_file_member_1: Member = data_models.MemberFactory.create(
            file_id=test_file.id,
            organization_id=test_config.organization_id,
            unique_corp_id="DDD",
            dependent_id="EEE",
            email="maxverstappen@redbull.com",
            record={
                # Mapped from altId
                "unique_corp_id": "DDD",
                # Mapped from primaryMemberId
                "dependent_id": "EEE",
            },
        )
        # Reconciled kafka members
        matched_kafka_members: List[Member] = data_models.MemberFactory.create_batch(
            size=10,
            file_id=None,
            organization_id=test_config.organization_id,
        )
        # Unreconciled file members
        unmatched_file_members: List[Member] = data_models.MemberFactory.create_batch(
            size=10,
            file_id=None,
            organization_id=test_config.organization_id,
        )

        await member_test_client.bulk_persist(
            models=[
                unmatched_kafka_member_1,
                unmatched_kafka_member_2,
                unmatched_file_member_1,
            ]
            + matched_kafka_members
            + unmatched_file_members
        )
        # Load our sql
        sql_str: str = migrations._get_matched_file_to_kafka_records_for_org_sql(
            match_fields=migrations.ALTID_TO_UNQCORPID_MATCH_CRITERIA, one_to_one=True
        )
        queries: Queries = aiosql.from_str(sql_str, "asyncpg")
        member_test_client.load_child_queries(name="test", queries=queries)

        # When
        async with member_test_client.client.connector.connection() as c:
            fetched: List[
                asyncpg.Record
            ] = await member_test_client.client.queries.test.get_matched_records_for_org(
                c, organization_id=test_config.organization_id
            )

        # Expect 0 since one_to_one was set to True when calling _get_matched_records_for_org_sql()
        assert len(fetched) == 0

    @staticmethod
    async def test_get_matched_records_for_org_sql_duplicate_file_member_one_to_one_off(
        e9y_connector: PostgresConnector,
        test_config: Configuration,
        test_file: File,
        member_test_client: Members,
    ):
        """2 file record and 1 kafka record, test that the query returns 2 tuples of ID's that match with one_to_one=False"""
        # Given
        # Matching criteria
        criteria: List[Tuple[str, str]] = [
            (
                "ltrim(lower(source.record->>'altId'), '0')",
                "ltrim(lower(target.record->>'unique_corp_id'), '0')",
            ),
            ("lower(source.dependent_id)", "lower(target.dependent_id)"),
            ("source.email", "target.email"),
        ]
        # A kafka member that needs reconciliation
        unmatched_kafka_member_1: Member = await member_test_client.persist(
            model=data_models.MemberFactory.create(
                file_id=None,
                organization_id=test_config.organization_id,
                unique_corp_id="CCC",
                dependent_id="EEE",
                email="aprost@ferrari.com",
                record={
                    "subscriberId": "CCC",
                    "altId": "DDD",
                    "primaryMemberId": "EEE",
                },
            )
        )
        # A file member that needs reconciliation with 1.
        unmatched_file_member_1: Member = await member_test_client.persist(
            model=data_models.MemberFactory.create(
                file_id=test_file.id,
                organization_id=test_config.organization_id,
                unique_corp_id="DDD",
                dependent_id="EEE",
                email="aprost@ferrari.com",
                record={
                    # Mapped from altId
                    "unique_corp_id": "DDD",
                    # Mapped from primaryMemberId
                    "dependent_id": "EEE",
                },
            )
        )
        # A duplicate file member that matches with the kafka member
        unmatched_file_member_2: Member = await member_test_client.persist(
            model=data_models.MemberFactory.create(
                file_id=test_file.id,
                organization_id=test_config.organization_id,
                unique_corp_id="DDC",
                dependent_id="EEE",
                email="aprost@ferrari.com",
                record={
                    # Mapped from altId
                    "unique_corp_id": "DDD",
                    # Mapped from primaryMemberId
                    "dependent_id": "EEE",
                },
            )
        )
        # Reconciled kafka members
        matched_kafka_members: List[Member] = data_models.MemberFactory.create_batch(
            size=10,
            file_id=None,
            organization_id=test_config.organization_id,
        )
        # Unreconciled file members
        unmatched_file_members: List[Member] = data_models.MemberFactory.create_batch(
            size=10,
            file_id=None,
            organization_id=test_config.organization_id,
        )

        await member_test_client.bulk_persist(
            models=matched_kafka_members + unmatched_file_members
        )
        # Load our sql
        sql_str: str = migrations._get_matched_file_to_kafka_records_for_org_sql(
            match_fields=criteria, one_to_one=False
        )
        queries: Queries = aiosql.from_str(sql_str, "asyncpg")
        member_test_client.load_child_queries(name="test", queries=queries)

        # When
        async with member_test_client.client.connector.connection() as c:
            fetched: List[
                asyncpg.Record
            ] = await member_test_client.client.queries.test.get_matched_records_for_org(
                c, organization_id=test_config.organization_id
            )

        # Expect 2 since one_to_one was set to False when calling _get_matched_records_for_org_sql()
        assert len(fetched) == 2
        # Make sure we get back the 2 pairs we expected
        assert set(
            [
                (pair.get("kafka_record_id"), pair.get("file_record_id"))
                for pair in fetched
            ]
        ) == set(
            [
                (unmatched_kafka_member_1.id, unmatched_file_member_1.id),
                (unmatched_kafka_member_1.id, unmatched_file_member_2.id),
            ]
        )

    @staticmethod
    async def test_get_matched_records_for_org_sql_duplicate_file_member_one_to_one_on(
        e9y_connector: PostgresConnector,
        test_config: Configuration,
        test_file: File,
        member_test_client: Members,
    ):
        """2 file record and 1 kafka record, test that the query returns NO ID's that match with one_to_one=True"""
        # Given
        # Matching criteria
        criteria: List[Tuple[str, str]] = [
            (
                "ltrim(lower(source.record->>'altId'), '0')",
                "ltrim(lower(target.record->>'unique_corp_id'), '0')",
            ),
            ("lower(source.dependent_id)", "lower(target.dependent_id)"),
            ("source.email", "target.email"),
        ]
        # A kafka member that needs reconciliation
        unmatched_kafka_member_1: Member = data_models.MemberFactory.create(
            file_id=None,
            organization_id=test_config.organization_id,
            unique_corp_id="CCC",
            dependent_id="EEE",
            record={"subscriberId": "CCC", "altId": "DDD", "primaryMemberId": "EEE"},
        )
        # A file member that needs reconciliation with 1.
        unmatched_file_member_1: Member = data_models.MemberFactory.create(
            file_id=test_file.id,
            organization_id=test_config.organization_id,
            unique_corp_id="DDD",
            dependent_id="EEE",
            record={
                # Mapped from altId
                "unique_corp_id": "DDD",
                # Mapped from primaryMemberId
                "dependent_id": "EEE",
            },
        )
        # A duplicate file member that matches with the kafka member
        unmatched_file_member_2: Member = data_models.MemberFactory.create(
            file_id=test_file.id,
            organization_id=test_config.organization_id,
            unique_corp_id="DDC",
            dependent_id="EEE",
            record={
                # Mapped from altId
                "unique_corp_id": "DDD",
                # Mapped from primaryMemberId
                "dependent_id": "EEE",
            },
        )
        # Reconciled kafka members
        matched_kafka_members: List[Member] = data_models.MemberFactory.create_batch(
            size=10,
            file_id=None,
            organization_id=test_config.organization_id,
        )
        # Unreconciled file members
        unmatched_file_members: List[Member] = data_models.MemberFactory.create_batch(
            size=10,
            file_id=None,
            organization_id=test_config.organization_id,
        )

        await member_test_client.bulk_persist(
            models=[
                unmatched_kafka_member_1,
                unmatched_file_member_1,
                unmatched_file_member_2,
            ]
            + matched_kafka_members
            + unmatched_file_members
        )
        # Load our sql
        sql_str: str = migrations._get_matched_file_to_kafka_records_for_org_sql(
            match_fields=criteria, one_to_one=True
        )
        queries: Queries = aiosql.from_str(sql_str, "asyncpg")
        member_test_client.load_child_queries(name="test", queries=queries)

        # When
        async with member_test_client.client.connector.connection() as c:
            fetched = await member_test_client.client.queries.test.get_matched_records_for_org(
                c, organization_id=test_config.organization_id
            )

        # Expect 0 since one_to_one was set to True when calling _get_matched_records_for_org_sql()
        assert len(fetched) == 0

    @staticmethod
    async def test_get_matched_records_for_org_sql_no_match(
        e9y_connector: PostgresConnector,
        test_config: Configuration,
        test_file: File,
        member_test_client: Members,
    ):
        """1 file record and 1 kafka record that doesn't match, test that the query returns NO ID's"""
        # Given
        # A kafka member that needs reconciliation
        unmatched_kafka_member_1: Member = data_models.MemberFactory.create(
            file_id=None,
            organization_id=test_config.organization_id,
            unique_corp_id="CCC",
            dependent_id="EEE",
            email="senna@mclaren.com",
            record={"subscriberId": "CCC", "altId": "DDD", "primaryMemberId": "EEE"},
        )
        # A file member that needs reconciliation with 1.
        unmatched_file_member_1: Member = data_models.MemberFactory.create(
            file_id=test_file.id,
            organization_id=test_config.organization_id,
            unique_corp_id="DDD",
            dependent_id="EEE",
            # email does not match
            email="sennaXXXX@mclaren.com",
            record={
                # Mapped from altId
                "unique_corp_id": "DDD",
                # Mapped from primaryMemberId
                "dependent_id": "EEE",
            },
        )
        # Reconciled kafka members
        matched_kafka_members: List[Member] = data_models.MemberFactory.create_batch(
            size=10,
            file_id=None,
            organization_id=test_config.organization_id,
        )
        # Unreconciled file members
        unmatched_file_members: List[Member] = data_models.MemberFactory.create_batch(
            size=10,
            file_id=None,
            organization_id=test_config.organization_id,
        )

        await member_test_client.bulk_persist(
            models=[unmatched_kafka_member_1, unmatched_file_member_1]
            + matched_kafka_members
            + unmatched_file_members
        )
        # Load our sql
        sql_str = migrations._get_matched_file_to_kafka_records_for_org_sql(
            match_fields=migrations.ALTID_TO_UNQCORPID_MATCH_CRITERIA
        )
        queries: Queries = aiosql.from_str(sql_str, "asyncpg")
        member_test_client.load_child_queries(name="test", queries=queries)

        # When
        async with member_test_client.client.connector.connection() as c:
            fetched = await member_test_client.client.queries.test.get_matched_records_for_org(
                c, organization_id=test_config.organization_id
            )

        assert len(fetched) == 0

    @staticmethod
    async def test_update_file_record_with_kafka_record_sql(
        e9y_connector: PostgresConnector,
        test_config: Configuration,
        test_file: File,
        member_test_client: Members,
    ):
        """1 file record and 1 kafka record with address that matches, test that the query updates the file record"""
        # Given
        # A kafka member that needs reconciliation
        unmatched_kafka_member_1: Member = await member_test_client.persist(
            model=data_models.MemberFactory.create(
                file_id=None,
                organization_id=test_config.organization_id,
                unique_corp_id="CCC",
                dependent_id="EEE",
                email="senna@mclaren.com",
                record={
                    "subscriberId": "CCC",
                    "altId": "DDD",
                    "primaryMemberId": "EEE",
                },
            )
        )
        # An address record attached to the kafka record
        unmatched_kafka_member_1_address: Address = data_models.AddressFactory.create(
            member_id=unmatched_kafka_member_1.id,
            country_code="None",
            address_type="263",
        )
        await member_test_client.set_address_for_member(
            address=unmatched_kafka_member_1_address
        )
        # A file member that needs reconciliation with 1.
        unmatched_file_member_1: Member = await member_test_client.persist(
            model=data_models.MemberFactory.create(
                file_id=test_file.id,
                organization_id=test_config.organization_id,
                unique_corp_id="DDD",
                dependent_id="EEE",
                email="senna@mclaren.com",
                record={
                    # Mapped from altId
                    "unique_corp_id": "DDD",
                    # Mapped from primaryMemberId
                    "dependent_id": "EEE",
                },
            )
        )
        # Reconciled kafka members
        matched_kafka_members: List[Member] = data_models.MemberFactory.create_batch(
            size=10,
            file_id=None,
            organization_id=test_config.organization_id,
        )
        # Unreconciled file members
        unmatched_file_members: List[Member] = data_models.MemberFactory.create_batch(
            size=10,
            file_id=None,
            organization_id=test_config.organization_id,
        )

        await member_test_client.bulk_persist(
            models=matched_kafka_members + unmatched_file_members
        )
        # Load our sql
        sql_str = migrations._update_file_record_with_kafka_record_sql()
        queries: Queries = aiosql.from_str(sql_str, "asyncpg")
        member_test_client.load_child_queries(name="test", queries=queries)

        mapping: List[Tuple[int, int]] = [
            (unmatched_kafka_member_1.id, unmatched_file_member_1.id)
        ]

        # When
        async with member_test_client.client.connector.connection() as c:
            await member_test_client.client.queries.test.update_file_record_with_kafka_record(
                c, mapping=mapping
            )

        # Check that the kafka based member is gone
        fetched_kafka_member: Optional[Member] = await member_test_client.get(
            unmatched_kafka_member_1.id
        )
        # Check that the file, now kafka, based member is updated
        fetched_file_member: Optional[Member] = await member_test_client.get(
            unmatched_file_member_1.id
        )
        # Check that the address is no longer attached to kafka member ID
        fetched_kafka_member_address = (
            await member_test_client.get_address_by_member_id(
                member_id=unmatched_kafka_member_1.id
            )
        )
        # Check that the address is now attached to file member ID
        fetched_file_member_address = await member_test_client.get_address_by_member_id(
            member_id=unmatched_file_member_1.id
        )

        assert not fetched_kafka_member and not fetched_kafka_member_address
        assert fetched_file_member_address.member_id == unmatched_file_member_1.id
        assert (
            fetched_file_member.id,
            fetched_file_member.organization_id,
            fetched_file_member.unique_corp_id,
            fetched_file_member.dependent_id,
            fetched_file_member.email,
        ) == (
            unmatched_file_member_1.id,
            unmatched_kafka_member_1.organization_id,
            unmatched_kafka_member_1.unique_corp_id,
            unmatched_kafka_member_1.dependent_id,
            unmatched_kafka_member_1.email,
        )

    @staticmethod
    async def test_update_matched_records_for_org_no_op(
        e9y_connector: PostgresConnector,
        test_config: Configuration,
        test_file: File,
        member_test_client: Members,
    ):
        """1 file record and 1 kafka record with address that matches, test that the function doesn't update the file record"""
        # Given
        # A kafka member that needs reconciliation
        unmatched_kafka_member_1: Member = data_models.MemberFactory.create(
            file_id=None,
            organization_id=test_config.organization_id,
            unique_corp_id="CCC",
            dependent_id="EEE",
            email="senna@mclaren.com",
            record={"subscriberId": "CCC", "altId": "DDD", "primaryMemberId": "EEE"},
        )
        # A file member that needs reconciliation with 1.
        unmatched_file_member_1: Member = data_models.MemberFactory.create(
            file_id=test_file.id,
            organization_id=test_config.organization_id,
            unique_corp_id="DDD",
            dependent_id="EEE",
            email="senna@mclaren.com",
            record={
                # Mapped from altId
                "unique_corp_id": "DDD",
                # Mapped from primaryMemberId
                "dependent_id": "EEE",
            },
        )
        # Reconciled kafka members
        matched_kafka_members: List[Member] = data_models.MemberFactory.create_batch(
            size=10,
            file_id=None,
            organization_id=test_config.organization_id,
        )
        # Unreconciled file members
        unmatched_file_members: List[Member] = data_models.MemberFactory.create_batch(
            size=10,
            file_id=None,
            organization_id=test_config.organization_id,
        )

        await member_test_client.bulk_persist(
            models=[unmatched_kafka_member_1, unmatched_file_member_1]
            + matched_kafka_members
            + unmatched_file_members
        )

        # When
        rows_updated: int = (
            await migrations.update_file_to_kafka_matched_records_for_org(
                organization_id=test_config.organization_id,
                match_fields=migrations.ALTID_TO_UNQCORPID_MATCH_CRITERIA,
                no_op=True,
                connector=e9y_connector,
            )
        )

        fetched_members = await member_test_client.all()

        # Then
        assert len(fetched_members) == 22 and rows_updated == 0

    @staticmethod
    async def test_update_matched_records_for_org(
        e9y_connector: PostgresConnector,
        test_config: Configuration,
        test_file: File,
        member_test_client: Members,
    ):
        """1 file record and 1 kafka record with address that matches, test that the function updates the file record"""
        # Given
        # A kafka member that needs reconciliation
        unmatched_kafka_member_1: Member = await member_test_client.persist(
            model=data_models.MemberFactory.create(
                file_id=None,
                organization_id=test_config.organization_id,
                unique_corp_id="CCC",
                dependent_id="EEE",
                email="senna@mclaren.com",
                record={
                    "subscriberId": "CCC",
                    "altId": "DDD",
                    "primaryMemberId": "EEE",
                },
            )
        )
        # An address record attached to the kafka record
        unmatched_kafka_member_1_address: Address = data_models.AddressFactory.create(
            member_id=unmatched_kafka_member_1.id,
            country_code="None",
            address_type="263",
        )
        await member_test_client.set_address_for_member(
            address=unmatched_kafka_member_1_address
        )
        # A file member that needs reconciliation with 1.
        unmatched_file_member_1: Member = await member_test_client.persist(
            model=data_models.MemberFactory.create(
                file_id=test_file.id,
                organization_id=test_config.organization_id,
                unique_corp_id="DDD",
                dependent_id="EEE",
                email="senna@mclaren.com",
                record={
                    # Mapped from altId
                    "unique_corp_id": "DDD",
                    # Mapped from primaryMemberId
                    "dependent_id": "EEE",
                },
            )
        )
        # Reconciled kafka members
        matched_kafka_members: List[Member] = data_models.MemberFactory.create_batch(
            size=10,
            file_id=None,
            organization_id=test_config.organization_id,
        )
        # Unreconciled file members
        unmatched_file_members: List[Member] = data_models.MemberFactory.create_batch(
            size=10,
            file_id=None,
            organization_id=test_config.organization_id,
        )

        await member_test_client.bulk_persist(
            models=[unmatched_kafka_member_1, unmatched_file_member_1]
            + matched_kafka_members
            + unmatched_file_members
        )

        # When
        rows_updated: int = (
            await migrations.update_file_to_kafka_matched_records_for_org(
                organization_id=test_config.organization_id,
                match_fields=migrations.ALTID_TO_UNQCORPID_MATCH_CRITERIA,
                no_op=False,
                connector=e9y_connector,
            )
        )

        fetched_members = await member_test_client.all()
        # Check that the kafka based member is gone
        fetched_kafka_member: Optional[Member] = await member_test_client.get(
            unmatched_kafka_member_1.id
        )
        # Check that the file, now kafka, based member is updated
        fetched_file_member: Optional[Member] = await member_test_client.get(
            unmatched_file_member_1.id
        )
        # Check that the address is no longer attached to kafka member ID
        fetched_kafka_member_address = (
            await member_test_client.get_address_by_member_id(
                member_id=unmatched_kafka_member_1.id
            )
        )
        # Check that the address is now attached to file member ID
        fetched_file_member_address = await member_test_client.get_address_by_member_id(
            member_id=unmatched_file_member_1.id
        )

        # Then
        assert len(fetched_members) == 21 and rows_updated == 1
        assert not fetched_kafka_member and not fetched_kafka_member_address
        assert fetched_file_member_address.member_id == unmatched_file_member_1.id
        assert (
            fetched_file_member.id,
            fetched_file_member.organization_id,
            fetched_file_member.unique_corp_id,
            fetched_file_member.dependent_id,
            fetched_file_member.email,
        ) == (
            unmatched_file_member_1.id,
            unmatched_kafka_member_1.organization_id,
            unmatched_kafka_member_1.unique_corp_id,
            unmatched_kafka_member_1.dependent_id,
            unmatched_kafka_member_1.email,
        )

    @staticmethod
    async def test_update_matched_records_for_pairs(
        e9y_connector: PostgresConnector,
        test_config: Configuration,
        test_file: File,
        member_test_client: Members,
    ):
        """1 file record and 1 kafka record with address that matches, test that the query updates the file record"""
        # Given
        # A kafka member that needs reconciliation
        unmatched_kafka_member_1: Member = await member_test_client.persist(
            model=data_models.MemberFactory.create(
                file_id=None,
                organization_id=test_config.organization_id,
                unique_corp_id="CCC",
                dependent_id="EEE",
                email="senna@mclaren.com",
                record={
                    "subscriberId": "CCC",
                    "altId": "DDD",
                    "primaryMemberId": "EEE",
                },
            )
        )
        # An address record attached to the kafka record
        unmatched_kafka_member_1_address: Address = data_models.AddressFactory.create(
            member_id=unmatched_kafka_member_1.id,
            country_code="None",
            address_type="263",
        )
        await member_test_client.set_address_for_member(
            address=unmatched_kafka_member_1_address
        )
        # A file member that needs reconciliation with 1.
        unmatched_file_member_1: Member = await member_test_client.persist(
            model=data_models.MemberFactory.create(
                file_id=test_file.id,
                organization_id=test_config.organization_id,
                unique_corp_id="DDD",
                dependent_id="EEE",
                email="senna@mclaren.com",
                record={
                    # Mapped from altId
                    "unique_corp_id": "DDD",
                    # Mapped from primaryMemberId
                    "dependent_id": "EEE",
                },
            )
        )
        # Reconciled kafka members
        matched_kafka_members: List[Member] = data_models.MemberFactory.create_batch(
            size=10,
            file_id=None,
            organization_id=test_config.organization_id,
        )
        # Unreconciled file members
        unmatched_file_members: List[Member] = data_models.MemberFactory.create_batch(
            size=10,
            file_id=None,
            organization_id=test_config.organization_id,
        )

        await member_test_client.bulk_persist(
            models=matched_kafka_members + unmatched_file_members
        )
        # Load our sql
        sql_str = migrations._update_file_record_with_kafka_record_sql()
        queries: Queries = aiosql.from_str(sql_str, "asyncpg")
        member_test_client.load_child_queries(name="test", queries=queries)

        mapping: List[Tuple[int, int]] = [
            (unmatched_kafka_member_1.id, unmatched_file_member_1.id)
        ]

        # When
        await migrations.update_matched_records_for_pairs(
            record_pairs=mapping, connector=e9y_connector
        )

        # Check that the kafka based member is gone
        fetched_kafka_member: Optional[Member] = await member_test_client.get(
            unmatched_kafka_member_1.id
        )
        # Check that the file, now kafka, based member is updated
        fetched_file_member: Optional[Member] = await member_test_client.get(
            unmatched_file_member_1.id
        )
        # Check that the address is no longer attached to kafka member ID
        fetched_kafka_member_address = (
            await member_test_client.get_address_by_member_id(
                member_id=unmatched_kafka_member_1.id
            )
        )
        # Check that the address is now attached to file member ID
        fetched_file_member_address = await member_test_client.get_address_by_member_id(
            member_id=unmatched_file_member_1.id
        )

        assert not fetched_kafka_member and not fetched_kafka_member_address
        assert fetched_file_member_address.member_id == unmatched_file_member_1.id
        assert (
            fetched_file_member.id,
            fetched_file_member.organization_id,
            fetched_file_member.unique_corp_id,
            fetched_file_member.dependent_id,
            fetched_file_member.email,
        ) == (
            unmatched_file_member_1.id,
            unmatched_kafka_member_1.organization_id,
            unmatched_kafka_member_1.unique_corp_id,
            unmatched_kafka_member_1.dependent_id,
            unmatched_kafka_member_1.email,
        )


class TestGenericMemberMigration:
    @staticmethod
    async def test_get_matched_records_for_org_sql_single_match_one_to_one_on(
        e9y_connector: PostgresConnector,
        test_config: Configuration,
        test_file: File,
        member_test_client: Members,
    ):
        """1 source record and 1 destination record, test that the query returns a tuple of ID's that match with one_to_one=True"""
        # Given
        # A bad member record that needs to be overwritten with new data
        unmatched_destination_member_1: Member = await member_test_client.persist(
            model=data_models.MemberFactory.create(
                file_id=None,
                first_name="Ayrton",
                last_name="Senna",
                organization_id=test_config.organization_id,
                unique_corp_id="2361925",
                dependent_id="2361925",
                email="senna@mclaren.com",
                record={
                    "subscriberId": "2361925",
                    "altId": "2361925",
                    "primaryMemberId": "2361925",
                },
                employer_assigned_id="2361925",
            )
        )
        # An updated member that needs reconciliation with 1.
        unmatched_source_member_1: Member = await member_test_client.persist(
            model=data_models.MemberFactory.create(
                file_id=None,
                first_name="Ayrton",
                last_name="Senna",
                date_of_birth=unmatched_destination_member_1.date_of_birth,
                organization_id=test_config.organization_id,
                unique_corp_id="13175562",
                dependent_id="2361925",
                email="senna@mclaren.com",
                record={
                    "subscriberId": "13175562",
                    "altId": "2361925",
                    "primaryMemberId": "2361925",
                },
            )
        )
        # Load our sql
        sql_str = migrations._get_matched_records_for_org_sql(
            match_fields=migrations.NAME_EMAIL_DOB_MATCH_CRITERIA,
            source_criteria_sql="unique_corp_id ~ '^0*\\d{8}$'",
            destination_criteria_sql="unique_corp_id=employer_assigned_id",
            one_to_one=True,
        )
        queries: Queries = aiosql.from_str(sql_str, "asyncpg")
        member_test_client.load_child_queries(name="test", queries=queries)

        # When
        async with member_test_client.client.connector.connection() as c:
            fetched: List[
                asyncpg.Record
            ] = await member_test_client.client.queries.test.get_matched_records_for_org(
                c, organization_id=test_config.organization_id
            )

        assert len(fetched) == 1
        assert (
            fetched[0].get("source_record_id"),
            fetched[0].get("dest_record_id"),
        ) == (unmatched_source_member_1.id, unmatched_destination_member_1.id)

    @staticmethod
    async def test_update_matched_records_for_org_no_op(
        e9y_connector: PostgresConnector,
        test_config: Configuration,
        test_file: File,
        member_test_client: Members,
    ):
        """1 source record and 1 destination record , test that the function doesn't update the dest record"""
        # Given
        # A member record that needs to be updated
        unmatched_destination_member_1: Member = await member_test_client.persist(
            model=data_models.MemberFactory.create(
                file_id=None,
                first_name="Ayrton",
                last_name="Senna",
                organization_id=test_config.organization_id,
                unique_corp_id="2361925",
                dependent_id="2361925",
                email="senna@mclaren.com",
                record={
                    "subscriberId": "2361925",
                    "altId": "2361925",
                    "primaryMemberId": "2361925",
                },
                employer_assigned_id="2361925",
            )
        )
        # An updated member that needs reconciliation with 1.
        _ = await member_test_client.persist(
            model=data_models.MemberFactory.create(
                file_id=None,
                first_name="Ayrton",
                last_name="Senna",
                date_of_birth=unmatched_destination_member_1.date_of_birth,
                organization_id=test_config.organization_id,
                unique_corp_id="13175562",
                dependent_id="2361925",
                email="senna@mclaren.com",
                record={
                    "subscriberId": "13175562",
                    "altId": "2361925",
                    "primaryMemberId": "2361925",
                },
            )
        )
        # some other members
        other_members: List[Member] = data_models.MemberFactory.create_batch(
            size=10,
            file_id=None,
            organization_id=test_config.organization_id,
        )

        await member_test_client.bulk_persist(models=other_members)

        # When
        rows_updated: int = await migrations.update_matched_records_for_org(
            organization_id=test_config.organization_id,
            match_fields=migrations.NAME_EMAIL_DOB_MATCH_CRITERIA,
            source_criteria_sql="unique_corp_id ~ '^0*\\d{8}$'",
            destination_criteria_sql="unique_corp_id=employer_assigned_id",
            no_op=True,
            connector=e9y_connector,
        )

        fetched_members = await member_test_client.all()

        # Then
        assert len(fetched_members) == 12 and rows_updated == 0

    @staticmethod
    async def test_update_matched_records_for_org(
        e9y_connector: PostgresConnector,
        test_config: Configuration,
        test_file: File,
        member_test_client: Members,
    ):
        """1 file record and 1 kafka record with address that matches, test that the function updates the file record"""
        # Given
        # A kafka member that needs reconciliation
        unmatched_destination_member_1: Member = await member_test_client.persist(
            model=data_models.MemberFactory.create(
                file_id=None,
                first_name="Ayrton",
                last_name="Senna",
                organization_id=test_config.organization_id,
                unique_corp_id="2361925",
                dependent_id="2361925",
                email="senna@mclaren.com",
                record={
                    "subscriberId": "2361925",
                    "altId": "2361925",
                    "primaryMemberId": "2361925",
                },
                employer_assigned_id="2361925",
            )
        )
        # A file member that needs reconciliation with 1.
        unmatched_source_member_1: Member = await member_test_client.persist(
            model=data_models.MemberFactory.create(
                file_id=None,
                first_name="Ayrton",
                last_name="Senna",
                date_of_birth=unmatched_destination_member_1.date_of_birth,
                organization_id=test_config.organization_id,
                unique_corp_id="13175562",
                dependent_id="2361925",
                email="senna@mclaren.com",
                record={
                    "subscriberId": "13175562",
                    "altId": "2361925",
                    "primaryMemberId": "2361925",
                },
            )
        )
        # An address record attached to the kafka record
        unmatched_source_member_1_address: Address = data_models.AddressFactory.create(
            member_id=unmatched_source_member_1.id,
            country_code="None",
            address_type="263",
        )
        await member_test_client.set_address_for_member(
            address=unmatched_source_member_1_address
        )
        # Other members
        other_members: List[Member] = data_models.MemberFactory.create_batch(
            size=10,
            file_id=None,
            organization_id=test_config.organization_id,
        )

        await member_test_client.bulk_persist(models=other_members)

        # When
        rows_updated: int = await migrations.update_matched_records_for_org(
            organization_id=test_config.organization_id,
            match_fields=migrations.NAME_EMAIL_DOB_MATCH_CRITERIA,
            source_criteria_sql="unique_corp_id ~ '^0*\\d{8}$'",
            destination_criteria_sql="unique_corp_id=employer_assigned_id",
            no_op=False,
            connector=e9y_connector,
        )

        fetched_members = await member_test_client.all()
        # Check that the kafka based member is gone
        fetched_source_member: Optional[Member] = await member_test_client.get(
            unmatched_source_member_1.id
        )
        # Check that the file, now kafka, based member is updated
        fetched_dest_member: Optional[Member] = await member_test_client.get(
            unmatched_destination_member_1.id
        )
        # Check that the address is no longer attached to kafka member ID
        fetched_source_member_address = (
            await member_test_client.get_address_by_member_id(
                member_id=unmatched_source_member_1.id
            )
        )
        # Check that the address is now attached to file member ID
        fetched_dest_member_address = await member_test_client.get_address_by_member_id(
            member_id=unmatched_destination_member_1.id
        )

        # Then
        assert len(fetched_members) == 11 and rows_updated == 1
        assert not fetched_source_member and not fetched_source_member_address
        assert (
            fetched_dest_member_address.member_id == unmatched_destination_member_1.id
        )
        assert (
            fetched_dest_member.id,
            fetched_dest_member.organization_id,
            fetched_dest_member.unique_corp_id,
            fetched_dest_member.dependent_id,
            fetched_dest_member.email,
        ) == (
            unmatched_destination_member_1.id,
            unmatched_source_member_1.organization_id,
            unmatched_source_member_1.unique_corp_id,
            unmatched_source_member_1.dependent_id,
            unmatched_source_member_1.email,
        )
