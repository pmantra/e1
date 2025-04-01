import datetime
from typing import List

import pytest
from tests.factories import data_models

from app.eligibility.populations import model as pop_model
from db.clients import configuration_client, population_client, sub_population_client

pytestmark = pytest.mark.asyncio


class TestSubPopulationClient:
    @staticmethod
    async def test_all(
        multiple_test_sub_populations: List[pop_model.SubPopulation],
        sub_population_test_client: sub_population_client.SubPopulations,
    ):
        # Given
        expected_total = len(multiple_test_sub_populations)

        # When
        all_sub_populations = await sub_population_test_client.all()

        # Then
        # Ensure we have grabbed all populations
        assert len(all_sub_populations) == expected_total

    @staticmethod
    async def test_get(
        test_sub_population: pop_model.SubPopulation,
        sub_population_test_client: sub_population_client.SubPopulations,
    ):
        # Given
        # When
        returned_sub_population = await sub_population_test_client.get(
            test_sub_population.id
        )

        # Then
        assert returned_sub_population == test_sub_population

    @staticmethod
    async def test_get_for_population_id(
        multiple_test_populations: List[pop_model.Population],
        sub_population_test_client: sub_population_client.SubPopulations,
    ):
        # Given
        sub_populations = []
        for test_pop in multiple_test_populations:
            for _ in range(3):
                sub_populations.append(
                    data_models.SubPopulationFactory.create(population_id=test_pop.id)
                )
        await sub_population_test_client.bulk_persist(models=sub_populations)

        # When
        returned_sub_populations = (
            await sub_population_test_client.get_for_population_id(
                multiple_test_populations[0].id
            )
        )

        # Then
        # Confirm that 3 were returned, as it was set up as 3 per population
        assert len(returned_sub_populations) == 3
        # Confirm the returned sub_populations aren't the only ones there, i.e., that there was
        # more than one population
        assert len(returned_sub_populations) != len(
            await sub_population_test_client.all()
        )

    @staticmethod
    async def test_get_for_population_id_no_sub_pops(
        multiple_test_populations: List[pop_model.Population],
        sub_population_test_client: sub_population_client.SubPopulations,
    ):
        # Given
        # When
        returned_sub_populations = (
            await sub_population_test_client.get_for_population_id(
                multiple_test_populations[0].id
            )
        )

        # Then
        # Confirm that an empty list is returned
        assert returned_sub_populations == []

    @staticmethod
    async def test_get_for_population_id_bad_pop_id(
        multiple_test_populations: List[pop_model.Population],
        sub_population_test_client: sub_population_client.SubPopulations,
    ):
        # Given
        # When
        returned_sub_populations = (
            await sub_population_test_client.get_for_population_id(population_id=7357)
        )

        # Then
        # Confirm that an empty list is returned
        assert returned_sub_populations == []

    @staticmethod
    async def test_get_for_active_population_for_organization_id(
        test_config: configuration_client.Configuration,
        population_test_client: population_client.Populations,
        sub_population_test_client: sub_population_client.SubPopulations,
    ):
        # Given
        temp_population = data_models.PopulationFactory.create(
            organization_id=test_config.organization_id,
            activated_at=datetime.datetime(2021, 1, 1),
            deactivated_at=datetime.datetime(2021, 12, 31),
        )
        temp_population = await population_test_client.persist(model=temp_population)
        for _ in range(2):
            await sub_population_test_client.persist(
                model=data_models.SubPopulationFactory.create(
                    population_id=temp_population.id
                )
            )
        temp_population = data_models.PopulationFactory.create(
            organization_id=test_config.organization_id,
            activated_at=datetime.datetime(2022, 1, 1),
            deactivated_at=datetime.datetime(2022, 12, 31),
        )
        temp_population = await population_test_client.persist(model=temp_population)
        for _ in range(4):
            await sub_population_test_client.persist(
                model=data_models.SubPopulationFactory.create(
                    population_id=temp_population.id
                )
            )
        temp_population = data_models.PopulationFactory.create(
            organization_id=test_config.organization_id,
            activated_at=datetime.datetime(2023, 1, 1),
        )
        expected_population = await population_test_client.persist(
            model=temp_population
        )
        for _ in range(8):
            await sub_population_test_client.persist(
                model=data_models.SubPopulationFactory.create(
                    population_id=expected_population.id
                )
            )

        # When
        returned_sub_populations = await sub_population_test_client.get_for_active_population_for_organization_id(
            organization_id=test_config.organization_id
        )

        # Then
        assert len(returned_sub_populations) == 8

    @staticmethod
    async def test_get_for_active_population_for_organization_id_none_active(
        test_config: configuration_client.Configuration,
        population_test_client: population_client.Populations,
        sub_population_test_client: sub_population_client.SubPopulations,
    ):
        # Given
        temp_population = data_models.PopulationFactory.create(
            organization_id=test_config.organization_id,
            activated_at=datetime.datetime(2021, 1, 1),
            deactivated_at=datetime.datetime(2021, 12, 31),
        )
        temp_population = await population_test_client.persist(model=temp_population)
        for _ in range(2):
            await sub_population_test_client.persist(
                model=data_models.SubPopulationFactory.create(
                    population_id=temp_population.id
                )
            )
        temp_population = data_models.PopulationFactory.create(
            organization_id=test_config.organization_id,
            activated_at=datetime.datetime(2022, 1, 1),
            deactivated_at=datetime.datetime(2022, 12, 31),
        )
        temp_population = await population_test_client.persist(model=temp_population)
        for _ in range(4):
            await sub_population_test_client.persist(
                model=data_models.SubPopulationFactory.create(
                    population_id=temp_population.id
                )
            )

        # When
        returned_sub_populations = await sub_population_test_client.get_for_active_population_for_organization_id(
            organization_id=test_config.organization_id
        )

        # Then
        assert returned_sub_populations == []

    @staticmethod
    @pytest.mark.parametrize(
        argnames="feature_type",
        argvalues=[
            pop_model.FeatureTypes.TRACK_FEATURE,
            pop_model.FeatureTypes.WALLET_FEATURE,
        ],
        ids=["Tracks", "Wallets"],
    )
    async def test_get_feature_list_of_type_for_id(
        feature_type: int,
        test_sub_population: pop_model.SubPopulation,
        sub_population_test_client: sub_population_client.SubPopulations,
    ):
        # Given
        # When
        feature_ids = await sub_population_test_client.get_feature_list_of_type_for_id(
            id=test_sub_population.id,
            feature_type=f"{feature_type}",
        )
        # Then
        assert feature_ids is not None

    @staticmethod
    @pytest.mark.parametrize(
        argnames="feature_type",
        argvalues=[
            pop_model.FeatureTypes.TRACK_FEATURE,
            pop_model.FeatureTypes.WALLET_FEATURE,
        ],
        ids=["Tracks", "Wallets"],
    )
    async def test_get_feature_list_of_type_for_id_non_existent(
        feature_type: int,
        sub_population_test_client: sub_population_client.SubPopulations,
    ):
        # Given
        # When
        feature_ids = await sub_population_test_client.get_feature_list_of_type_for_id(
            id=7357,
            feature_type=f"{feature_type}",
        )
        # Then
        assert feature_ids is None
