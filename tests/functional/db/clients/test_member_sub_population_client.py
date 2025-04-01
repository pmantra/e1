from __future__ import annotations

import pytest
from tests.factories import data_models

from app.eligibility.populations import model as pop_model
from db import model
from db.clients import member_sub_population_client, member_versioned_client

pytestmark = pytest.mark.asyncio


async def _create_eligible_and_termed_members(
    num_members_eligible,
    num_members_termed,
    test_config,
    test_sub_population,
    member_versioned_test_client,
    member_sub_population_test_client,
):
    if num_members_eligible > 0:
        await member_versioned_test_client.bulk_persist(
            models=[
                data_models.MemberVersionedFactory.create(
                    organization_id=test_config.organization_id,
                    effective_range=data_models.DateRangeFactory.create(),
                )
                for _ in range(num_members_eligible)
            ]
        )
    # Termed members
    if num_members_termed > 0:
        await member_versioned_test_client.bulk_persist(
            models=[
                data_models.MemberVersionedFactory.create(
                    organization_id=test_config.organization_id,
                    effective_range=data_models.ExpiredDateRangeFactory.create(),
                )
                for _ in range(num_members_termed)
            ]
        )

    all_members = await member_versioned_test_client.all()
    await member_sub_population_test_client.bulk_persist(
        models=[
            data_models.MemberSubPopulationFactory.create(
                member_id=test_member.id,
                sub_population_id=test_sub_population.id,
            )
            for test_member in all_members
        ]
    )


class TestMemberSubPopulationClient:
    @staticmethod
    async def test_get_sub_population_id_for_member_id(
        test_member_versioned: model.MemberVersioned,
        member_sub_population_test_client: member_sub_population_client.MemberSubPopulations,
    ):
        # Given
        test_member_sub_population_info = data_models.MemberSubPopulationFactory.create(
            member_id=test_member_versioned.id,
        )
        await member_sub_population_test_client.persist(
            model=test_member_sub_population_info
        )

        # When
        returned_sub_population_id = (
            await member_sub_population_test_client.get_sub_population_id_for_member_id(
                test_member_sub_population_info.member_id
            )
        )

        # Then
        assert (
            returned_sub_population_id
            == test_member_sub_population_info.sub_population_id
        )

    @staticmethod
    @pytest.mark.parametrize(
        argnames="num_members_eligible,num_members_termed",
        argvalues=[
            (0, 10),
            (5, 5),
            (10, 0),
        ],
        ids=[
            "all-termed",
            "half-half",
            "all-eligible",
        ],
    )
    async def test_get_all_member_ids_for_sub_population_id(
        num_members_eligible: int,
        num_members_termed: int,
        test_config: model.Configuration,
        test_sub_population: pop_model.SubPopulation,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        member_sub_population_test_client: member_sub_population_client.MemberSubPopulations,
    ):
        # Given
        await _create_eligible_and_termed_members(
            num_members_eligible=num_members_eligible,
            num_members_termed=num_members_termed,
            test_config=test_config,
            test_sub_population=test_sub_population,
            member_versioned_test_client=member_versioned_test_client,
            member_sub_population_test_client=member_sub_population_test_client,
        )

        # When
        returned_member_ids = await member_sub_population_test_client.get_all_member_ids_for_sub_population_id(
            test_sub_population.id
        )

        # Then
        # All members are returned
        assert len(returned_member_ids) == (num_members_eligible + num_members_termed)

    @staticmethod
    @pytest.mark.parametrize(
        argnames="num_members_eligible,num_members_termed",
        argvalues=[
            (0, 10),
            (5, 5),
            (10, 0),
        ],
        ids=[
            "all-termed",
            "half-half",
            "all-eligible",
        ],
    )
    async def test_get_all_active_member_ids_for_sub_population_id(
        num_members_eligible: int,
        num_members_termed: int,
        test_config: model.Configuration,
        test_sub_population: pop_model.SubPopulation,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        member_sub_population_test_client: member_sub_population_client.MemberSubPopulations,
    ):
        # Given
        await _create_eligible_and_termed_members(
            num_members_eligible=num_members_eligible,
            num_members_termed=num_members_termed,
            test_config=test_config,
            test_sub_population=test_sub_population,
            member_versioned_test_client=member_versioned_test_client,
            member_sub_population_test_client=member_sub_population_test_client,
        )

        # When
        returned_member_ids = await member_sub_population_test_client.get_all_active_member_ids_for_sub_population_id(
            test_sub_population.id
        )

        # Then
        # Only eligible members are returned
        assert len(returned_member_ids) == num_members_eligible
