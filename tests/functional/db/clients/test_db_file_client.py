from __future__ import annotations

import pytest
from tests.functional.conftest import NUMBER_TEST_OBJECTS

from db import model
from db.clients import configuration_client, file_client

pytestmark = pytest.mark.asyncio


class TestFileClient:

    # region fetch tests

    @staticmethod
    async def test_return_all(multiple_test_file, file_test_client):
        all_files = await file_test_client.all()
        # We have created 10 files for 10 organizations- ensure we have grabbed all of them
        assert len(all_files) == NUMBER_TEST_OBJECTS * NUMBER_TEST_OBJECTS

    @staticmethod
    async def test_get(test_file: file_client.File, file_test_client):
        # When
        returned_file = await file_test_client.get(test_file.id)
        # Then
        assert returned_file == test_file

    @staticmethod
    async def test_get_for_ids(multiple_test_file, file_test_client):
        input_ids = [f.id for f in multiple_test_file]

        # return files using their ids
        returned_files = await file_test_client.for_ids(*input_ids)

        assert len(returned_files) == len(input_ids)

        # We can't confirm the full object is the same, but ensure we have the right fileID
        for f in returned_files:
            assert f.id in input_ids

    @staticmethod
    async def test_get_names(multiple_test_file, file_test_client):

        input_ids = [f.id for f in multiple_test_file]

        # return file names using their ids
        returned_file_name_records = await file_test_client.get_names(*input_ids)
        returned_file_names = [record["name"] for record in returned_file_name_records]

        assert len(returned_file_names) == len(input_ids)

        # ensure we have returned the correct file names
        for f in multiple_test_file:
            assert f.name in returned_file_names

    @staticmethod
    async def test_get_by_name(
        multiple_test_config: configuration_client.Configuration, file_test_client
    ):
        expected_file_name = "foo"
        expected_return_files = []

        for conf in multiple_test_config:
            org_id = conf.organization_id

            # create our expected return file
            file = model.File(org_id, expected_file_name)
            expected_return_files.append(await file_test_client.persist(model=file))

            # create complementary random file we want filtered out using our  get_by_name query
            file2 = model.File(org_id, "bar")
            await file_test_client.persist(model=file2)

        returned_files = await file_test_client.get_by_name(expected_file_name)

        assert len(returned_files) == len(multiple_test_config)

        for f in returned_files:
            assert f.name == expected_file_name

    @staticmethod
    async def test_get_by_name_for_org(
        multiple_test_config: configuration_client.Configuration, file_test_client
    ):
        expected_file_name = "foo"
        expected_org_id = multiple_test_config[0].organization_id

        for conf in multiple_test_config:
            org_id = conf.organization_id

            # create our expected return file
            file = model.File(org_id, expected_file_name)
            await file_test_client.persist(model=file)

        returned_file = await file_test_client.get_by_name_for_org(
            expected_file_name, expected_org_id
        )

        assert len(returned_file) == 1
        assert returned_file[0].organization_id == expected_org_id
        assert returned_file[0].name == expected_file_name

    @staticmethod
    async def test_get_all_for_org(
        test_config: configuration_client.Configuration,
        multiple_test_file,
        file_test_client,
    ):
        all_files = await file_test_client.get_all_for_org(test_config.organization_id)

        # We have created 10 files for each organization
        assert len(all_files) == NUMBER_TEST_OBJECTS

    @staticmethod
    async def test_get_completed_for_org(
        test_config: configuration_client.Configuration, file_test_client
    ):

        completed_file_ids = []

        for i in range(NUMBER_TEST_OBJECTS):
            file = await file_test_client.persist(
                model=model.File(test_config.organization_id, f"bar_{i}")
            )
            await file_test_client.set_completed_at(id=file.id)
            completed_file_ids.append(file.id)

        returned_completed_files = await file_test_client.get_completed_for_org(
            test_config.organization_id
        )
        assert len(returned_completed_files) == NUMBER_TEST_OBJECTS
        for file in returned_completed_files:
            assert file.id in completed_file_ids

    @staticmethod
    async def test_get_completed(
        multiple_test_config: configuration_client.Configuration, file_test_client
    ):
        completed_file_ids = []

        config_ids = []
        for config in multiple_test_config:
            file = await file_test_client.persist(
                model=model.File(config.organization_id, "foo")
            )
            await file_test_client.set_completed_at(file.id)
            completed_file_ids.append(file.id)
            config_ids.append(config.organization_id)

        returned_completed_files = await file_test_client.get_completed()

        assert len(returned_completed_files) == len(multiple_test_config)
        for file in returned_completed_files:
            assert file.id in completed_file_ids
            assert file.organization_id in config_ids

    @staticmethod
    async def test_get_incomplete_for_org(
        test_config: configuration_client.Configuration, file_test_client
    ):

        incompleted_file_ids = []

        for i in range(NUMBER_TEST_OBJECTS):
            file = await file_test_client.persist(
                model=model.File(test_config.organization_id, f"foo_{i}")
            )
            incompleted_file_ids.append(file.id)

        # Ensure we have set half of the files to completed, and can identify files that are complete/incomplete
        returned_incomplete_files = await file_test_client.get_incomplete_for_org(
            test_config.organization_id
        )
        assert len(returned_incomplete_files) == NUMBER_TEST_OBJECTS
        for file in returned_incomplete_files:
            assert file.id in incompleted_file_ids

    @staticmethod
    async def test_get_incomplete(
        multiple_test_config: configuration_client.Configuration, file_test_client
    ):
        incomplete_file_ids = []

        config_ids = []
        for config in multiple_test_config:
            file = await file_test_client.persist(
                model=model.File(config.organization_id, "foo")
            )
            incomplete_file_ids.append(file.id)
            config_ids.append(config.organization_id)

        returned_incomplete_files = await file_test_client.get_incomplete()

        assert len(returned_incomplete_files) == len(multiple_test_config)
        for file in returned_incomplete_files:
            assert file.id in incomplete_file_ids
            assert file.organization_id in config_ids

    @staticmethod
    async def test_get_incomplete_org_ids_file_ids(
        multiple_test_config: configuration_client.Configuration, file_test_client
    ):
        """
        Scenario:
            We need to grab all incomplete file records as well as the organizations they belong to.
        """

        config_file_map = {}

        for config in multiple_test_config:
            file = await file_test_client.persist(
                model=model.File(config.organization_id, "foo")
            )
            config_file_map[config.organization_id] = file.id

        returned_incomplete_files = (
            await file_test_client.get_incomplete_org_ids_file_ids()
        )

        assert len(returned_incomplete_files) == len(multiple_test_config)
        for file in returned_incomplete_files:
            assert file["organization_id"] in config_file_map.keys()
            assert config_file_map[file["organization_id"]] == file["id"]

    @staticmethod
    async def test_get_pending_for_org(
        test_config: configuration_client.Configuration, file_test_client
    ):
        pending_file_ids = []

        for i in range(NUMBER_TEST_OBJECTS):
            file = await file_test_client.persist(
                model=model.File(test_config.organization_id, f"bar_{i}")
            )
            pending_file_ids.append(file.id)

        returned_pending_files = await file_test_client.get_pending_for_org(
            test_config.organization_id
        )
        assert len(returned_pending_files) == NUMBER_TEST_OBJECTS
        for file in returned_pending_files:
            assert file.id in pending_file_ids

    @staticmethod
    async def test_get_pending(
        multiple_test_config: configuration_client.Configuration, file_test_client
    ):
        pending_file_ids = []

        config_ids = []
        for config in multiple_test_config:
            file = await file_test_client.persist(
                model=model.File(config.organization_id, "foo")
            )
            pending_file_ids.append(file.id)
            config_ids.append(config.organization_id)

        returned_pending_files = await file_test_client.get_pending()

        assert len(returned_pending_files) == len(multiple_test_config)
        for file in returned_pending_files:
            assert file.id in pending_file_ids
            assert file.organization_id in config_ids

    # endregion

    # region mutate tests

    @staticmethod
    async def test_persist(test_config, file_test_client):
        file = model.File(test_config.organization_id, "foo")
        created_file = await file_test_client.persist(model=file)
        assert created_file.organization_id == test_config.organization_id
        assert created_file.name == "foo"

    @staticmethod
    async def test_bulk_persist(test_config, file_test_client):
        files = []
        file_names = []
        for i in range(NUMBER_TEST_OBJECTS):
            file_name = f"foo_{i}"
            files.append(model.File(test_config.organization_id, file_name))
            file_names.append(file_name)

        await file_test_client.bulk_persist(models=files)

        created_files = await file_test_client.get_all_for_org(
            test_config.organization_id
        )

        assert len(files) == len(created_files)

        for f in created_files:
            assert f.organization_id == test_config.organization_id
            assert f.name in file_names

    @staticmethod
    async def test_delete(multiple_test_file: file_client.File, file_test_client):
        id_to_delete = multiple_test_file[0].id
        await file_test_client.delete(id_to_delete)
        remaining_files = await file_test_client.all()
        assert len(remaining_files) == len(multiple_test_file) - 1
        for f in remaining_files:
            assert f.id != id_to_delete

    @staticmethod
    async def test_bulk_delete(multiple_test_file: file_client.File, file_test_client):
        ids_to_delete = [f.id for f in multiple_test_file[0:2]]

        await file_test_client.bulk_delete(*ids_to_delete)

        remaining_files = await file_test_client.all()
        assert len(remaining_files) == len(multiple_test_file) - len(ids_to_delete)
        for f in remaining_files:
            assert f.id not in ids_to_delete

    @staticmethod
    async def test_set_started_at(test_file: file_client.File, file_test_client):
        await file_test_client.set_started_at(test_file.id)

        file = await file_test_client.get(test_file.id)

        assert file.started_at is not None  # noqa

    @staticmethod
    async def test_set_file_count(test_file: file_client.File, file_test_client):
        # Given
        initial_file_id = test_file.id

        # When
        await file_test_client.set_file_count(
            id=initial_file_id, raw_count=100, success_count=99, failure_count=1
        )
        file = await file_test_client.get(initial_file_id)

        # Then
        assert file.raw_count == 100
        assert file.success_count == 99
        assert file.failure_count == 1

    @staticmethod
    async def test_set_completed_at(test_file: file_client.File, file_test_client):
        await file_test_client.set_completed_at(test_file.id)

        file = await file_test_client.get(test_file.id)

        assert file.completed_at is not None  # noqa

    @staticmethod
    async def test_set_encoding(test_file: file_client.File, file_test_client):
        encoding = "foobar"
        await file_test_client.set_encoding(test_file.id, encoding)

        file = await file_test_client.get(test_file.id)

        assert file.encoding == encoding

    @staticmethod
    async def test_set_error(test_file: file_client.File, file_test_client):
        error = model.FileError.DELIMITER
        await file_test_client.set_error(test_file.id, error)

        file = await file_test_client.get(test_file.id)

        assert file.error == error

    @staticmethod
    async def test_get_one_before_latest_for_org(test_config, file_test_client):
        number_of_test_files = 3
        expected_test_file_name = "test_file_1"

        for i in range(number_of_test_files):
            file = model.File(test_config.organization_id, f"test_file_{i}")
            await file_test_client.persist(model=file)

        one_before_latest_file = await file_test_client.get_one_before_latest_for_org(
            test_config.organization_id
        )
        assert one_before_latest_file.name == expected_test_file_name


# endregion
