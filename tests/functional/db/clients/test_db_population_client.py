from __future__ import annotations

import datetime
from typing import List

import pytest
from tests.factories import data_models
from tests.functional.conftest import NUMBER_TEST_OBJECTS

from app.eligibility.populations import model as pop_model
from db import model
from db.clients import configuration_client, member_versioned_client, population_client

pytestmark = pytest.mark.asyncio


class TestPopulationClient:
    @staticmethod
    async def test_all(
        multiple_test_populations: List[pop_model.Population],
        population_test_client: population_client.Populations,
    ):
        # Given
        expected_total = len(multiple_test_populations)

        # When
        all_populations = await population_test_client.all()

        # Then
        # Ensure we have grabbed all populations
        assert len(all_populations) == expected_total

    @staticmethod
    async def test_get(
        test_population: pop_model.Population,
        population_test_client: population_client.Populations,
    ):
        # Given
        # When
        returned_population = await population_test_client.get(test_population.id)

        # Then
        assert returned_population == test_population

    @staticmethod
    @pytest.mark.parametrize(
        argnames="num_active",
        argvalues=[
            NUMBER_TEST_OBJECTS,
            NUMBER_TEST_OBJECTS // 2,
            0,
        ],
        ids=["all", "half", "none"],
    )
    async def test_get_all_active_populations(
        num_active: int,
        multiple_test_config: List[model.Configuration],
        population_test_client: population_client.Populations,
    ):
        # Given
        assert NUMBER_TEST_OBJECTS == len(multiple_test_config)
        yesterday = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            days=1
        )
        await population_test_client.bulk_persist(
            models=[
                data_models.PopulationFactory.create(
                    organization_id=c.organization_id,
                    activated_at=yesterday,
                    # Leave the first num_active as active and set the
                    # deactivation date for the rest to yesterday
                    deactivated_at=None if i < num_active else yesterday,
                )
                for i, c in enumerate(multiple_test_config)
            ]
        )

        # When
        all_active_populations = (
            await population_test_client.get_all_active_populations()
        )

        # Then
        assert len(all_active_populations) == num_active

    @staticmethod
    async def test_get_all_for_organization_id(
        multiple_test_config: List[configuration_client.Configuration],
        population_test_client: population_client.Populations,
    ):
        # Given
        populations = []
        for test_config in multiple_test_config:
            for _ in range(3):
                populations.append(
                    data_models.PopulationFactory.create(
                        organization_id=test_config.organization_id
                    )
                )
        await population_test_client.bulk_persist(models=populations)

        # When
        returned_populations = await population_test_client.get_all_for_organization_id(
            multiple_test_config[0].organization_id
        )

        # Then
        # Confirm that 3 were returned, as it was set up as 3 per org
        assert len(returned_populations) == 3
        # Confirm the returned populations aren't the only ones there, i.e., that there was
        # more than one configured organization
        assert len(returned_populations) != len(await population_test_client.all())

    @staticmethod
    async def test_get_all_for_organization_id_no_populations(
        multiple_test_config: List[configuration_client.Configuration],
        population_test_client: population_client.Populations,
    ):
        # Given
        # When
        returned_populations = await population_test_client.get_all_for_organization_id(
            multiple_test_config[0].organization_id
        )

        # Then
        # Confirm that an empty list was returned
        assert returned_populations == []

    @staticmethod
    async def test_get_active_population_for_organization_id(
        test_config: configuration_client.Configuration,
        population_test_client: population_client.Populations,
    ):
        # Given
        temp_population = data_models.PopulationFactory.create(
            organization_id=test_config.organization_id,
            activated_at=datetime.datetime(2021, 1, 1),
            deactivated_at=datetime.datetime(2021, 12, 31),
        )
        await population_test_client.persist(model=temp_population)
        temp_population = data_models.PopulationFactory.create(
            organization_id=test_config.organization_id,
            activated_at=datetime.datetime(2022, 1, 1),
            deactivated_at=datetime.datetime(2022, 12, 31),
        )
        await population_test_client.persist(model=temp_population)
        temp_population = data_models.PopulationFactory.create(
            organization_id=test_config.organization_id,
            activated_at=datetime.datetime(2023, 1, 1),
        )
        expected_population = await population_test_client.persist(
            model=temp_population
        )

        # When
        active_population = (
            await population_test_client.get_active_population_for_organization_id(
                test_config.organization_id
            )
        )

        # Then
        assert active_population == expected_population

    @staticmethod
    async def test_get_active_population_for_organization_id_none_active(
        test_config: configuration_client.Configuration,
        population_test_client: population_client.Populations,
    ):
        # Given
        temp_population = data_models.PopulationFactory.create(
            organization_id=test_config.organization_id,
            activated_at=datetime.datetime(2021, 1, 1),
            deactivated_at=datetime.datetime(2021, 12, 31),
        )
        await population_test_client.persist(model=temp_population)
        temp_population = data_models.PopulationFactory.create(
            organization_id=test_config.organization_id,
            activated_at=datetime.datetime(2022, 1, 1),
            deactivated_at=datetime.datetime(2022, 12, 31),
        )
        await population_test_client.persist(model=temp_population)

        # When
        active_population = (
            await population_test_client.get_active_population_for_organization_id(
                test_config.organization_id
            )
        )

        # Then
        assert active_population is None

    @staticmethod
    async def test_get_the_population_information_for_user_id(
        active_test_population: pop_model.Population,
        test_verification: model.Verification,
        population_test_client: population_client.Populations,
    ):
        # Given
        # When
        pop_info = (
            await population_test_client.get_the_population_information_for_user_id(
                user_id=test_verification.user_id
            )
        )

        # Then
        assert pop_info.population_id == active_test_population.id
        assert pop_info.sub_pop_lookup_keys_csv is not None
        assert pop_info.organization_id == active_test_population.organization_id

    @staticmethod
    async def test_get_the_population_information_for_user_id_no_population(
        test_verification: model.Verification,
        population_test_client: population_client.Populations,
    ):
        # Given
        # When
        pop_info = (
            await population_test_client.get_the_population_information_for_user_id(
                user_id=test_verification.user_id
            )
        )

        # Then
        assert pop_info is None

    @staticmethod
    async def test_get_the_population_information_for_user_and_org(
        multiple_active_test_populations: List[pop_model.Population],
        same_user_multiple_test_verifications: List[model.Verification],
        population_test_client: population_client.Populations,
    ):
        # Given
        # When
        pop_info = await population_test_client.get_the_population_information_for_user_and_org(
            user_id=same_user_multiple_test_verifications[0].user_id,
            organization_id=same_user_multiple_test_verifications[0].organization_id,
        )

        # Then
        assert pop_info.population_id in [
            p.id for p in multiple_active_test_populations
        ]
        assert pop_info.sub_pop_lookup_keys_csv is not None

    @staticmethod
    async def test_get_the_population_information_for_user_and_org_no_active_population(
        multiple_test_populations: List[pop_model.Population],
        same_user_multiple_test_verifications: List[model.Verification],
        population_test_client: population_client.Populations,
    ):
        # Given
        # When
        pop_info = await population_test_client.get_the_population_information_for_user_and_org(
            user_id=same_user_multiple_test_verifications[0].user_id,
            organization_id=same_user_multiple_test_verifications[0].organization_id,
        )

        # Then
        assert pop_info is None

    @staticmethod
    async def test_get_the_population_information_for_user_and_org_no_population(
        test_verification: model.Verification,
        population_test_client: population_client.Populations,
    ):
        # Given
        # When
        pop_info = await population_test_client.get_the_population_information_for_user_and_org(
            user_id=test_verification.user_id,
            organization_id=test_verification.organization_id,
        )

        # Then
        assert pop_info is None

    @staticmethod
    @pytest.mark.usefixtures("test_member_verification")
    @pytest.mark.parametrize(
        argnames="work_state,custom_attributes,expected_sub_pop_id",
        argvalues=[
            ("NY", '{"employment_status": "Part", "group_number": "3"}', 203),
            ("ZZ", '{"employment_status": "Full", "group_number": "2"}', 202),
        ],
        ids=["NY-Part-3", "ZZ-Full-2"],
    )
    async def test_get_the_sub_pop_id_using_lookup_keys_for_member(
        work_state: str,
        custom_attributes: str,
        expected_sub_pop_id: int,
        test_member_versioned: model.MemberVersioned,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        mapped_population: pop_model.Population,
        population_test_client: population_client.Populations,
    ):
        # Given
        test_member_versioned.work_state = work_state
        test_member_versioned.custom_attributes = custom_attributes
        await member_versioned_test_client.persist(model=test_member_versioned)

        # When
        returned_sub_pop_id = await population_test_client.get_the_sub_pop_id_using_lookup_keys_for_member(
            lookup_keys_csv=mapped_population.sub_pop_lookup_keys_csv,
            member=test_member_versioned,
            population_id=mapped_population.id,
        )

        # Then
        assert returned_sub_pop_id == expected_sub_pop_id

    @staticmethod
    @pytest.mark.usefixtures("test_member_verification")
    @pytest.mark.parametrize(
        argnames="record,expected_sub_pop_id",
        argvalues=[
            ({"wallet_enabled": True}, 101),
            ({"wallet_enabled": False}, 102),
            ({}, None),
        ],
        ids=["wallet enabled", "wallet not enabled", "wallet not set"],
    )
    async def test_get_the_sub_pop_id_using_lookup_keys_for_member_using_bool(
        record: dict,
        expected_sub_pop_id: int,
        test_member_versioned: model.MemberVersioned,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        mapped_populations_with_bool: pop_model.Population,
        population_test_client: population_client.Populations,
    ):
        # Given
        test_member_versioned.record = record
        await member_versioned_test_client.persist(model=test_member_versioned)

        # When
        returned_sub_pop_id = await population_test_client.get_the_sub_pop_id_using_lookup_keys_for_member(
            lookup_keys_csv=mapped_populations_with_bool.sub_pop_lookup_keys_csv,
            member=test_member_versioned,
            population_id=mapped_populations_with_bool.id,
        )

        # Then
        assert returned_sub_pop_id == expected_sub_pop_id

    @staticmethod
    @pytest.mark.usefixtures("test_member_verification")
    @pytest.mark.parametrize(
        argnames="work_state,custom_attributes",
        argvalues=[
            ("NY", '{"employment_status": "Party", "group_number": "3"}'),
            ("ZZ", '{"employment_status": "Full", "group_number": "21"}'),
            ("NJ", '{"unemployment_status": "Empty", "group_number": "2"}'),
            ("NY", '{"employment_status": "Full", "group_dumber": "1"}'),
        ],
        ids=["NY-Party-3", "ZZ-Full-21", "NJ-None-2", "NY-Full-None"],
    )
    async def test_get_the_sub_pop_id_using_lookup_keys_for_member_fail(
        work_state: str,
        custom_attributes: str,
        test_member_versioned: model.MemberVersioned,
        member_versioned_test_client: member_versioned_client.MembersVersioned,
        mapped_population: pop_model.Population,
        population_test_client: population_client.Populations,
    ):
        # Given
        test_member_versioned.work_state = work_state
        test_member_versioned.custom_attributes = custom_attributes
        await member_versioned_test_client.persist(model=test_member_versioned)

        # When
        returned_sub_pop_id = await population_test_client.get_the_sub_pop_id_using_lookup_keys_for_member(
            lookup_keys_csv=mapped_population.sub_pop_lookup_keys_csv,
            member=test_member_versioned,
            population_id=mapped_population.id,
        )

        # Then
        assert returned_sub_pop_id is None

    @staticmethod
    async def test_set_sub_pop_lookup_info(
        active_test_population: pop_model.Population,
        population_test_client: population_client.Populations,
    ):
        # Given
        test_lookup_keys = "thistest"
        test_lookup_map = {"test": "pass"}
        # When
        await population_test_client.set_sub_pop_lookup_info(
            population_id=active_test_population.id,
            sub_pop_lookup_keys_csv=test_lookup_keys,
            sub_pop_lookup_map=test_lookup_map,
        )
        active_population = (
            await population_test_client.get_active_population_for_organization_id(
                organization_id=active_test_population.organization_id
            )
        )

        # Then
        assert active_population.sub_pop_lookup_keys_csv == test_lookup_keys
        assert active_population.sub_pop_lookup_map_json == test_lookup_map

    @staticmethod
    async def test_activate_population_with_date(
        test_config: configuration_client.Configuration,
        test_population: pop_model.Population,
        population_test_client: population_client.Populations,
    ):
        # Given
        active_population = (
            await population_test_client.get_active_population_for_organization_id(
                test_config.organization_id
            )
        )
        assert active_population is None

        # When
        await population_test_client.activate_population(
            population_id=test_population.id,
            activated_at=datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(days=1),
        )

        # Then
        active_population = (
            await population_test_client.get_active_population_for_organization_id(
                test_config.organization_id
            )
        )
        assert active_population.id == test_population.id

    @staticmethod
    async def test_activate_population_without_date(
        test_config: configuration_client.Configuration,
        test_population: pop_model.Population,
        population_test_client: population_client.Populations,
    ):
        # Given
        active_population = (
            await population_test_client.get_active_population_for_organization_id(
                test_config.organization_id
            )
        )
        assert active_population is None

        # When
        await population_test_client.activate_population(
            population_id=test_population.id
        )

        # Then
        active_population = (
            await population_test_client.get_active_population_for_organization_id(
                test_config.organization_id
            )
        )
        assert active_population.id == test_population.id

    @staticmethod
    async def test_activate_population_with_future_date(
        test_config: configuration_client.Configuration,
        test_population: pop_model.Population,
        population_test_client: population_client.Populations,
    ):
        # Given
        active_population = (
            await population_test_client.get_active_population_for_organization_id(
                test_config.organization_id
            )
        )
        assert active_population is None

        # When
        await population_test_client.activate_population(
            population_id=test_population.id,
            activated_at=datetime.datetime.now(datetime.timezone.utc)
            + datetime.timedelta(days=1),
        )

        # Then
        active_population = (
            await population_test_client.get_active_population_for_organization_id(
                test_config.organization_id
            )
        )
        assert active_population is None

    @staticmethod
    async def test_deactivate_population_with_date(
        test_config: configuration_client.Configuration,
        active_test_population: pop_model.Population,
        population_test_client: population_client.Populations,
    ):
        # Given
        active_population = (
            await population_test_client.get_active_population_for_organization_id(
                test_config.organization_id
            )
        )
        assert active_population == active_test_population

        # When
        await population_test_client.deactivate_population(
            population_id=active_test_population.id,
            deactivated_at=datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(days=1),
        )

        # Then
        active_population = (
            await population_test_client.get_active_population_for_organization_id(
                test_config.organization_id
            )
        )
        assert active_population is None

    @staticmethod
    async def test_deactivate_population_without_date(
        test_config: configuration_client.Configuration,
        active_test_population: pop_model.Population,
        population_test_client: population_client.Populations,
    ):
        # Given
        active_population = (
            await population_test_client.get_active_population_for_organization_id(
                test_config.organization_id
            )
        )
        assert active_population == active_test_population

        # When
        await population_test_client.deactivate_population(
            population_id=active_test_population.id
        )

        # Then
        active_population = (
            await population_test_client.get_active_population_for_organization_id(
                test_config.organization_id
            )
        )
        assert active_population is None

    @staticmethod
    async def test_deactivate_population_with_future_date(
        test_config: configuration_client.Configuration,
        active_test_population: pop_model.Population,
        population_test_client: population_client.Populations,
    ):
        # Given
        active_population = (
            await population_test_client.get_active_population_for_organization_id(
                test_config.organization_id
            )
        )
        assert active_population == active_test_population

        # When
        await population_test_client.deactivate_population(
            population_id=active_test_population.id,
            deactivated_at=datetime.datetime.now(datetime.timezone.utc)
            + datetime.timedelta(days=1),
        )

        # Then
        active_population = (
            await population_test_client.get_active_population_for_organization_id(
                test_config.organization_id
            )
        )
        assert active_population is not None

    @staticmethod
    async def test_deactivate_population_for_organization_id_with_date(
        test_config: configuration_client.Configuration,
        active_test_population: pop_model.Population,
        population_test_client: population_client.Populations,
    ):
        # Given
        active_population = (
            await population_test_client.get_active_population_for_organization_id(
                test_config.organization_id
            )
        )
        assert active_population == active_test_population

        # When
        await population_test_client.deactivate_populations_for_organization_id(
            organization_id=test_config.organization_id,
            deactivated_at=datetime.datetime.now(datetime.timezone.utc)
            - datetime.timedelta(days=1),
        )

        # Then
        active_population = (
            await population_test_client.get_active_population_for_organization_id(
                test_config.organization_id
            )
        )
        assert active_population is None

    @staticmethod
    async def test_deactivate_population_for_organization_id_without_date(
        test_config: configuration_client.Configuration,
        active_test_population: pop_model.Population,
        population_test_client: population_client.Populations,
    ):
        # Given
        active_population = (
            await population_test_client.get_active_population_for_organization_id(
                test_config.organization_id
            )
        )
        assert active_population == active_test_population

        # When
        await population_test_client.deactivate_populations_for_organization_id(
            organization_id=test_config.organization_id
        )

        # Then
        active_population = (
            await population_test_client.get_active_population_for_organization_id(
                test_config.organization_id
            )
        )
        assert active_population is None

    @staticmethod
    async def test_deactivate_population_for_organization_id_with_future_date(
        test_config: configuration_client.Configuration,
        active_test_population: pop_model.Population,
        population_test_client: population_client.Populations,
    ):
        # Given
        active_population = (
            await population_test_client.get_active_population_for_organization_id(
                test_config.organization_id
            )
        )
        assert active_population == active_test_population

        # When
        await population_test_client.deactivate_populations_for_organization_id(
            organization_id=test_config.organization_id,
            deactivated_at=datetime.datetime.now(datetime.timezone.utc)
            + datetime.timedelta(days=1),
        )

        # Then
        active_population = (
            await population_test_client.get_active_population_for_organization_id(
                test_config.organization_id
            )
        )
        assert active_population is not None
