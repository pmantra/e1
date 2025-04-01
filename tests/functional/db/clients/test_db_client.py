from __future__ import annotations

from dataclasses import asdict
from typing import List

import asyncpg
import pytest
import typic
from pymysql.err import OperationalError
from tests.conftest import _mock_namespace
from tests.factories import data_models as factory
from tests.functional.conftest import _create_mono_connector

from db import model
from db.clients import client, configuration_client

pytestmark = pytest.mark.asyncio


class TestGenericMutations:
    @staticmethod
    async def test_persist(configuration_test_client):
        # Given
        inp = factory.ConfigurationFactory.create()
        # When
        out: model.Configuration = await configuration_test_client.persist(model=inp)
        # Then
        # Always a new instance
        assert inp is not out
        # Set by the database
        assert out.created_at and out.updated_at
        # These should match.
        assert (
            out.organization_id == inp.organization_id
            and out.directory_name == inp.directory_name
        )
        # When
        inp.directory_name = "bar"
        # Graceful UPSERTs
        nout = await configuration_test_client.persist(model=inp)
        # Then
        assert nout.directory_name == inp.directory_name

    @staticmethod
    async def test_persist_data(configuration_test_client):
        # Given
        inp = factory.ConfigurationFactory.create()
        # When
        out: model.Configuration = await configuration_test_client.persist(
            organization_id=inp.organization_id,
            directory_name=inp.directory_name,
            email_domains=inp.email_domains,
            implementation=inp.implementation,
            data_provider=inp.data_provider,
            activated_at=inp.activated_at,
            terminated_at=inp.terminated_at,
            employee_only=inp.employee_only,
            medical_plan_only=inp.medical_plan_only,
            eligibility_type="HEALTHPLAN",
        )
        # Then
        # Set by the database
        assert out.created_at and out.updated_at
        # These should match.
        assert (
            out.organization_id == inp.organization_id
            and out.directory_name == inp.directory_name
        )

    @staticmethod
    async def test_persist_raw(configuration_test_client):
        # Given
        inp = factory.ConfigurationFactory.create()
        # When
        out: asyncpg.Record = await configuration_test_client.persist(
            organization_id=inp.organization_id,
            directory_name=inp.directory_name,
            email_domains=inp.email_domains,
            implementation=inp.implementation,
            data_provider=inp.data_provider,
            coerce=False,
            activated_at=inp.activated_at,
            terminated_at=inp.terminated_at,
            employee_only=inp.employee_only,
            medical_plan_only=inp.medical_plan_only,
            eligibility_type="HEALTHPLAN",
        )
        # Then
        assert isinstance(out, asyncpg.Record)

    @staticmethod
    async def test_bulk_persist(configuration_test_client):
        # Given
        inputs = [factory.ConfigurationFactory.create() for _ in range(10)]
        # When
        await configuration_test_client.bulk_persist(models=inputs)
        outputs = await configuration_test_client.all()
        # Then
        assert {(i.organization_id, i.directory_name) for i in inputs} == {
            (o.organization_id, o.directory_name) for o in outputs
        }

    @staticmethod
    async def test_bulk_persist_data(configuration_test_client):
        # Given
        inputs: List[client.Configuration] = [
            factory.ConfigurationFactory.create() for _ in range(10)
        ]
        data = [
            {
                k: v
                for k, v in asdict(i).items()
                if k not in configuration_test_client.__exclude_fields__
            }
            for i in inputs
        ]
        # When
        await configuration_test_client.bulk_persist(data=data)
        outputs = await configuration_test_client.all()
        # Then
        assert {(i.organization_id, i.directory_name) for i in inputs} == {
            (o.organization_id, o.directory_name) for o in outputs
        }

    @staticmethod
    async def test_delete(
        test_config: configuration_client.Configuration, configuration_test_client
    ):
        # When
        await configuration_test_client.delete(test_config.organization_id)
        # Then
        assert await configuration_test_client.get(test_config.organization_id) is None

    @staticmethod
    async def test_bulk_delete(
        test_config: configuration_client.Configuration, configuration_test_client
    ):
        # When
        await configuration_test_client.bulk_delete(test_config.organization_id)
        # Then
        assert await configuration_test_client.get(test_config.organization_id) is None


class TestMetaQueries:
    @staticmethod
    async def test_select(
        test_config: configuration_client.Configuration, configuration_test_client
    ):
        # When
        _, queried = await configuration_test_client.select(
            organization_id=test_config.organization_id, count=False
        )
        # Then
        assert queried == [test_config]

    @staticmethod
    async def test_count(
        configuration_test_client: configuration_client.Configurations,
    ):
        # Given
        inp = factory.ConfigurationFactory.create()
        assert (
            await configuration_test_client.count(
                configuration_test_client.get, organization_id=inp.organization_id
            )
            == 0
        )
        # When
        await configuration_test_client.persist(model=inp)
        # Then
        assert (
            await configuration_test_client.count(
                configuration_test_client.get, organization_id=inp.organization_id
            )
            == 1
        )


def test_range_serialization():
    # Given
    given_range = asyncpg.Range(1, 3, lower_inc=True)
    expected_dict = {
        "lower": 1,
        "upper": 3,
        "lower_inc": True,
        "upper_inc": False,
        "empty": False,
    }
    # When
    primitive = typic.primitive(given_range)
    # Then
    # Get the expected output
    assert primitive == expected_dict
    # Output is idempotent.
    assert asyncpg.Range(**primitive) == given_range


class TestMavenMonoClientNamespacing:
    async def test_without_namespace(self, maven_connection):
        """
        The default un-namespaced database name for dev / prod based off the yml config files
        """
        # Given
        # When
        # Then
        assert maven_connection.db == "maven"

    async def test_with_namespace(self, patch_namespace):
        """
        The namespaced database name for multi-tenant QA based off the APP_ENVIRONMENT_NAMESPACE env variable and yml config files
        """
        with pytest.raises(
            OperationalError
        ):  # Will throw "cannot connect" error, but that's expected since namespaced DB does not exist in test environment.
            # Given
            # When
            connector = await _create_mono_connector()
            # Then
            assert connector.db == f"{_mock_namespace}__maven"
