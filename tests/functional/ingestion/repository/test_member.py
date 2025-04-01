from typing import List

import pytest
from ingestion import repository
from tests.factories import data_models

from db import model as db_model
from db.clients import configuration_client, member_client

pytestmark = pytest.mark.asyncio


class TestPersistOptumMembers:
    @staticmethod
    async def test_persist_optum_members_members_are_persisted(
        member_repo: repository.MemberRepository,
        member_test_client: member_client.Members,
        configuration_test_client: configuration_client.Configurations,
    ):
        # Given
        org: db_model.Configuration = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create()
        )
        record: db_model.ExternalRecord = data_models.ExternalRecordFactory.create(
            organization_id=org.organization_id
        )
        external_record_and_address: db_model.ExternalRecordAndAddress = (
            data_models.ExternalRecordAndAddressFactory(external_record=record)
        )

        # When
        await member_repo.persist_optum_members(records=[external_record_and_address])

        # Then
        members: List[db_model.Member] = await member_test_client.get_for_org(
            organization_id=org.organization_id
        )
        member: db_model.Member = members[0]
        assert (
            member.organization_id,
            member.unique_corp_id,
            member.dependent_id,
            member.date_of_birth,
            member.email,
            member.first_name,
            member.last_name,
        ) == (
            record["organization_id"],
            record["unique_corp_id"],
            record["dependent_id"],
            record["date_of_birth"],
            record["email"],
            record["first_name"],
            record["last_name"],
        )

    @staticmethod
    async def test_persist_optum_members_address_is_persisted(
        member_repo: repository.MemberRepository,
        member_test_client: member_client.Members,
        configuration_test_client: configuration_client.Configurations,
    ):
        # Given
        org: db_model.Configuration = await configuration_test_client.persist(
            model=data_models.ConfigurationFactory.create()
        )
        record: db_model.ExternalRecord = data_models.ExternalRecordFactory.create(
            organization_id=org.organization_id
        )
        external_record_and_address: db_model.ExternalRecordAndAddress = (
            data_models.ExternalRecordAndAddressFactory(external_record=record)
        )

        # When
        member, _ = await member_repo.persist_optum_members(
            records=[external_record_and_address]
        )

        # Then
        address = await member_test_client.get_address_by_member_id(
            member_id=member[0]["id"]
        )

        assert address
