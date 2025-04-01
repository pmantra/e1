from __future__ import annotations

import pytest
from tests.factories import data_models as factory

from db.clients import configuration_client, header_aliases_client
from db.model import File, HeaderAlias

pytestmark = pytest.mark.asyncio


class TestHeaderAliasClient:

    # region fetch tests

    @staticmethod
    async def test_all(
        multiple_test_header_alias: header_aliases_client.HeaderAliases,
        header_test_client,
    ):
        returned_headers = await header_test_client.all()

        # TODO Replace this with the constant we created
        assert len(returned_headers) == 10

    @staticmethod
    async def test_get(
        test_header_alias: header_aliases_client.HeaderAliases, header_test_client
    ):
        returned_header_alias = await header_test_client.get(test_header_alias.id)

        assert returned_header_alias == test_header_alias

    @staticmethod
    async def test_get_org_header_alias(
        test_header_alias: header_aliases_client.HeaderAliases,
        test_config: configuration_client.Configuration,
        header_test_client,
    ):
        returned_header_alias = await header_test_client.get_org_header_alias(
            test_config.organization_id, test_header_alias.header
        )

        assert returned_header_alias == test_header_alias

    @staticmethod
    async def test_get_for_org(
        test_header_alias: header_aliases_client.HeaderAliases,
        test_config: configuration_client.Configuration,
        header_test_client,
    ):
        returned_header_alias = await header_test_client.get_for_org(
            test_config.organization_id
        )

        assert len(returned_header_alias) == 1
        assert returned_header_alias[0] == test_header_alias

    @staticmethod
    async def test_get_for_file(
        test_config: configuration_client.Configuration,
        header_test_client,
        file_test_client,
    ):
        # generate our header and our file - we want a really unique value, so not using the conftest stuff
        test_header_alias = await header_test_client.persist(
            model=HeaderAlias(
                organization_id=test_config.organization_id,
                header="foobar",
                alias="fizzbuzz",
            )
        )
        test_file = await file_test_client.persist(
            model=File(
                organization_id=test_config.organization_id, name="primary/cleaner.csv"
            )
        )

        returned_header_alias = await header_test_client.get_for_file(test_file.id)
        assert len(returned_header_alias) == 1
        assert returned_header_alias[0] == test_header_alias

    @staticmethod
    async def test_get_for_files(
        test_config: configuration_client.Configuration,
        header_test_client,
        file_test_client,
    ):
        # generate our header and our file - we want a really unique value, so not using the conftest stuff
        test_header_aliases = []
        test_files = []

        for i in range(2):
            test_header_aliases.append(
                await header_test_client.persist(
                    model=HeaderAlias(
                        organization_id=test_config.organization_id,
                        header=f"foobar_{i}",
                        alias=f"fizzbuzz_{i}",
                    )
                )
            )
            test_files.append(
                await file_test_client.persist(
                    model=File(
                        organization_id=test_config.organization_id,
                        name=f"primary/clean_{i}.csv",
                    )
                )
            )

        returned_headers = await header_test_client.get_for_files(
            *[f.id for f in test_files]
        )
        assert len(returned_headers) == 2

        for r_h in returned_headers:
            assert r_h in test_header_aliases

    @staticmethod
    async def test_get_header_mapping(
        test_config: configuration_client.Configuration, header_test_client
    ):
        # Given
        aliases = factory.HeaderAliasFactory.create_batch(
            10, organization_id=test_config.organization_id
        )
        mapping = {a.header: a.alias for a in aliases}
        await header_test_client.bulk_persist(models=aliases)
        # When
        persisted = await header_test_client.get_header_mapping(
            test_config.organization_id
        )
        # Then
        assert persisted == mapping

    @staticmethod
    async def test_get_header_mapping_empty(
        test_config: configuration_client.Configuration, header_test_client
    ):
        # Whend tests/functional/db/clients/test_db_header_aliases_client.py
        mapping = await header_test_client.get_header_mapping(
            test_config.organization_id
        )
        # Then
        assert mapping == {}

    # endregion

    # region mutate tests

    @staticmethod
    async def test_bulk_refresh(
        test_config: configuration_client.Configuration,
        header_test_client: header_aliases_client.HeaderAliases,
        configuration_test_client: configuration_client.Configurations,
    ):
        # Given
        aliases: list[
            header_aliases_client.HeaderAlias
        ] = factory.HeaderAliasFactory.create_batch(
            10, organization_id=test_config.organization_id
        )
        other_config = factory.ConfigurationFactory.create()
        await configuration_test_client.persist(model=other_config)
        other_aliases = factory.HeaderAliasFactory.create_batch(
            10, organization_id=other_config.organization_id
        )
        await header_test_client.bulk_persist(models=other_aliases)
        # When
        # Remove an alias
        removed = aliases.pop()
        # Update the rest
        for ha in aliases:
            ha.alias += "foo"
        org_mapping = {
            test_config.organization_id: {ha.header: ha.alias for ha in aliases}
        }
        expected_result = {(ha.organization_id, ha.header, ha.alias) for ha in aliases}
        expected_untouched = {
            (ha.organization_id, ha.header, ha.alias) for ha in other_aliases
        }
        # Refresh the header mapping
        persisted = await header_test_client.bulk_refresh(org_mapping.items())
        result = {(ha.organization_id, ha.header, ha.alias) for ha in persisted}
        untouched = {
            (ha.organization_id, ha.header, ha.alias)
            for ha in (
                await header_test_client.get_for_org(other_config.organization_id)
            )
        }
        # Then
        # Assert that all header aliases for the org match the reported output,
        #   and that matches the expectation, with the "removed" alias now deleted.
        assert (removed.organization_id, removed.header, removed.alias) not in result
        assert result == expected_result
        # Assert that any aliases for organizations not seen in the refresh are untouched.
        assert untouched == expected_untouched

    @staticmethod
    async def test_bulk_persist_header_alias(
        test_config: configuration_client.Configuration,
        header_test_client: header_aliases_client.HeaderAliases,
        faker,
    ):
        # Given
        inputs = factory.HeaderAliasFactory.create_batch(
            10, organization_id=test_config.organization_id
        )
        await header_test_client.bulk_persist(models=inputs)
        # When
        # New aliases!
        for ha in inputs:
            ha.alias = faker.domain_word()
        await header_test_client.bulk_persist(models=inputs)
        outputs = await header_test_client.all()
        # Then
        # We didn't create new ones and the aliases are updated.
        assert {ha.header: ha.alias for ha in inputs} == {
            ha.header: ha.alias for ha in outputs
        }

    @staticmethod
    async def test_persist_header_mapping(
        test_config: configuration_client.Configuration,
        header_test_client: header_aliases_client.HeaderAliases,
    ):
        # Given
        aliases = factory.HeaderAliasFactory.create_batch(
            10, organization_id=test_config.organization_id
        )
        mapping = {a.header: a.alias for a in aliases}
        # When
        persisted = await header_test_client.persist_header_mapping(
            test_config.organization_id, mapping=mapping
        )
        # Then
        assert persisted == mapping

    # endregion
